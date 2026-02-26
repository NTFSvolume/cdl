from __future__ import annotations

import asyncio
import contextlib
import datetime
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import aiofiles

from cyberdrop_dl import config
from cyberdrop_dl.clients.jdownloader import JDownloader
from cyberdrop_dl.constants import REGEX_LINKS, BlockedDomains
from cyberdrop_dl.crawlers._chevereto import CheveretoCrawler
from cyberdrop_dl.crawlers.crawler import Crawler, create_crawlers
from cyberdrop_dl.crawlers.discourse import DiscourseCrawler
from cyberdrop_dl.crawlers.http_direct import DirectHttpFile
from cyberdrop_dl.crawlers.realdebrid import RealDebridCrawler
from cyberdrop_dl.crawlers.wordpress import WordPressHTMLCrawler, WordPressMediaCrawler
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.exceptions import JDownloaderError, NoExtensionError
from cyberdrop_dl.logger import log, log_spacer
from cyberdrop_dl.utils.utilities import get_download_path, remove_trailing_slash

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Sequence

    import aiosqlite

    from cyberdrop_dl.config.settings import GenericCrawlerInstances
    from cyberdrop_dl.crawlers import Crawler
    from cyberdrop_dl.managers import Manager

existing_crawlers: dict[str, Crawler] = {}
_seen_urls: set[AbsoluteHttpURL] = set()
_crawlers_disabled_at_runtime: set[str] = set()


def is_outside_date_range(scrape_item: ScrapeItem, before: datetime.date | None, after: datetime.date | None) -> bool:
    skip = False
    item_date = scrape_item.completed_at or scrape_item.created_at
    if not item_date:
        return False
    date = datetime.datetime.fromtimestamp(item_date).date()
    if (after and date < after) or (before and date > before):
        skip = True

    return skip


def is_in_domain_list(scrape_item: ScrapeItem, domain_list: Sequence[str]) -> bool:
    return any(domain in scrape_item.url.host for domain in domain_list)


class ScrapeMapper:
    """This class maps links to their respective handlers, or JDownloader if they are unsupported."""

    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        self.existing_crawlers: dict[str, Crawler] = {}
        self.direct_crawler = DirectHttpFile(self.manager)
        self.jdownloader = JDownloader.new(config.get())
        self.jdownloader_whitelist = config.get().runtime.jdownloader_whitelist
        self.using_input_file = False
        self.groups = set()
        self.count = 0
        self.real_debrid: RealDebridCrawler

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    @property
    def group_count(self) -> int:
        return len(self.groups)

    def start_scrapers(self) -> None:
        """Starts all scrapers."""
        from cyberdrop_dl import plugins

        self.existing_crawlers = get_crawlers_mapping(self.manager)
        generic_crawlers = create_generic_crawlers_by_config(config.get().generic_crawlers_instances)
        for crawler in generic_crawlers:
            register_crawler(self.existing_crawlers, crawler(self.manager), from_user=True)
        disable_crawlers_by_config(self.existing_crawlers, config.get().general.disable_crawlers)
        plugins.load(self.manager)

    async def start_real_debrid(self) -> None:
        """Starts RealDebrid."""
        self.existing_crawlers["real-debrid"] = self.real_debrid = real = RealDebridCrawler(self.manager)
        await real.startup()

    @classmethod
    @contextlib.asynccontextmanager
    async def managed(cls, manager: Manager) -> AsyncGenerator[Self]:
        """Creates a new scrape mapper that auto closses http session on exit"""

        self = cls(manager)
        await self.manager.http_client.load_cookie_files()

        async with self.manager.http_client, asyncio.TaskGroup() as tg:
            self.manager.scrape_mapper = self
            self.manager.task_group = tg
            yield self

    async def run(self) -> None:
        """Starts the orchestra."""
        self.start_scrapers()
        await self.manager.db_manager.history_table.update_previously_unsupported(self.existing_crawlers)
        await self.jdownloader.connect()
        await self.start_real_debrid()
        self.direct_crawler._init_downloader()
        async for item in self.get_input_items():
            self.manager.task_group.create_task(self.send_to_crawler(item))

    async def get_input_items(self, input_file) -> AsyncGenerator[ScrapeItem]:
        items_generator = self.load_links(input_file)
        children_limits = config.get().download.max_children

        async for item in items_generator:
            item.children_limits = children_limits
            if self.should_scrape(item):
                yield item
                self.count += 1

        if not self.count:
            log("No valid links found.", 30)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def parse_input_file_groups(self, input_file) -> AsyncGenerator[tuple[str, list[AbsoluteHttpURL]]]:
        """Split URLs from input file by their groups."""

        if not await asyncio.to_thread(input_file.is_file):
            yield ("", [])
            return

        block_quote = False
        current_group_name = ""
        async with aiofiles.open(input_file, encoding="utf8") as f:
            async for line in f:
                if line.startswith(("---", "===")):  # New group begins here
                    current_group_name = line.replace("---", "").replace("===", "").strip()

                if current_group_name:
                    self.groups.add(current_group_name)
                    yield (current_group_name, list(regex_links(line)))
                    continue

                block_quote = not block_quote if line == "#\n" else block_quote
                if not block_quote:
                    yield ("", list(regex_links(line)))

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~``

    async def load_links(self, source: list[AbsoluteHttpURL] | Path) -> AsyncGenerator[ScrapeItem]:
        """Loads links from args / input file."""

        if isinstance(source, Path):
            self.using_input_file = True
            async for group_name, urls in self.parse_input_file_groups():
                for url in urls:
                    if not url:
                        continue
                    item = ScrapeItem(url=url)
                    if group_name:
                        item.add_to_parent_title(group_name)
                        item.part_of_album = True
                    yield item

            return

        for url in source:
            yield ScrapeItem(url=url)

    async def load_failed_links(self) -> AsyncGenerator[ScrapeItem]:
        """Loads failed links from database."""
        async for rows in self.manager.db_manager.history_table.get_failed_items():
            for row in rows:
                yield _create_item_from_row(row)

    async def load_all_bunkr_failed_links_via_hash(self) -> AsyncGenerator[ScrapeItem]:
        """Loads all bunkr links with maintenance hash."""
        async for rows in self.manager.db_manager.history_table.get_all_bunkr_failed():
            for row in rows:
                yield _create_item_from_row(row)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def filter_and_send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Send scrape_item to a supported crawler."""
        if self.should_scrape(scrape_item):
            await self.send_to_crawler(scrape_item)

    async def send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Maps URLs to their respective handlers."""
        scrape_item.url = remove_trailing_slash(scrape_item.url)
        crawler_match = match_url_to_crawler(self.existing_crawlers, scrape_item.url)
        jdownloader_whitelisted = True
        if self.jdownloader_whitelist:
            jdownloader_whitelisted = any(domain in scrape_item.url.host for domain in self.jdownloader_whitelist)

        if crawler_match:
            if not crawler_match.ready:
                await crawler_match.startup()
            self.manager.task_group.create_task(crawler_match.run(scrape_item))
            return

        if not self.real_debrid.disabled and self.real_debrid.api.is_supported(scrape_item.url):
            log(f"Using RealDebrid for unsupported URL: {scrape_item.url}", 10)
            self.manager.task_group.create_task(self.real_debrid.run(scrape_item))
            return

        try:
            await self.direct_crawler.fetch(scrape_item)
            return

        except (NoExtensionError, ValueError):
            pass

        if self.jdownloader._enabled and jdownloader_whitelisted:
            log(f"Sending unsupported URL to JDownloader: {scrape_item.url}", 20)
            success = False
            try:
                download_folder = get_download_path(self.manager, scrape_item, "jdownloader")
                relative_download_dir = download_folder.relative_to(config.get().files.download_folder)
                self.jdownloader.send(
                    scrape_item.url,
                    scrape_item.parent_title,
                    relative_download_dir,
                )
                success = True
            except JDownloaderError as e:
                log(f"Failed to send {scrape_item.url} to JDownloader\n{e.message}", 40)
                self.manager.logs.write_unsupported(scrape_item.url, scrape_item)
            self.manager.progress.scrape_errors.add_unsupported(sent_to_jdownloader=success)
            return

        log(f"Unsupported URL: {scrape_item.url}", 30)
        self.manager.logs.write_unsupported(scrape_item.url, scrape_item)
        self.manager.progress.scrape_errors.add_unsupported()

    def should_scrape(self, scrape_item: ScrapeItem) -> bool:
        """Pre-filter scrape items base on URL."""

        if scrape_item.url in _seen_urls:
            return False

        _seen_urls.add(scrape_item.url)

        if (
            is_in_domain_list(scrape_item, BlockedDomains.partial_match)
            or scrape_item.url.host in BlockedDomains.exact_match
        ):
            log(f"Skipping {scrape_item.url} as it is a blocked domain", 10)
            return False

        skip_hosts = config.get().ignore.skip_hosts
        if skip_hosts and is_in_domain_list(scrape_item, skip_hosts):
            log(f"Skipping URL by skip_hosts config: {scrape_item.url}", 10)
            return False

        only_hosts = config.get().ignore.only_hosts
        if only_hosts and not is_in_domain_list(scrape_item, only_hosts):
            log(f"Skipping URL by only_hosts config: {scrape_item.url}", 10)
            return False

        return True

    def disable_crawler(self, domain: str) -> Crawler | None:
        """Disables a crawler at runtime, after the scrape mapper is already running.

        It does not remove the crawler from the crawlers map, it just sets it as `disabled"`

        This has the effect to silently ignore any URL that maps to that crawler, without any "unsupported" or "errors" log messages

        `domain` must match _exactly_, AKA: it must be the value of `crawler.DOMAIN`

        Returns the crawler instance that was disabled (if Any)

        """

        if domain in _crawlers_disabled_at_runtime:
            return

        crawler = next((crawler for crawler in self.existing_crawlers.values() if crawler.DOMAIN == domain), None)
        if crawler and not crawler.disabled:
            crawler.disabled = True
            _crawlers_disabled_at_runtime.add(domain)
            return crawler


def regex_links(line: str) -> Generator[AbsoluteHttpURL]:
    """Regex grab the links from the URLs.txt file.

    This allows code blocks or full paragraphs to be copy and pasted into the URLs.txt.
    """

    line = line.strip()
    if line.startswith("#"):
        return

    http_urls = (x.group().replace(".md.", ".") for x in re.finditer(REGEX_LINKS, line))
    for link in http_urls:
        try:
            encoded = "%" in link
            yield AbsoluteHttpURL(link, encoded=encoded)
        except Exception as e:
            log(f"Unable to parse URL from input file: {link} {e:!r}", 40)


def _create_item_from_row(row: aiosqlite.Row) -> ScrapeItem:
    referer: str = row["referer"]
    url = AbsoluteHttpURL(referer, encoded="%" in referer)
    item = ScrapeItem(url=url, retry_path=Path(row["download_path"]), part_of_album=True)
    if completed_at := row["completed_at"]:
        item.completed_at = int(datetime.datetime.fromisoformat(completed_at).timestamp())
    if created_at := row["created_at"]:
        item.created_at = int(datetime.datetime.fromisoformat(created_at).timestamp())
    return item


def get_crawlers_mapping(manager: Manager | None = None, include_generics: bool = False) -> dict[str, Crawler]:
    """Returns a mapping with an instance of all crawlers.

    Crawlers are only created on the first calls. Future calls always return a reference to the same crawlers

    If manager is `None`, the `MOCK_MANAGER` will be used, which means the crawlers won't be able to actually run"""

    from cyberdrop_dl.crawlers import CRAWLERS
    from cyberdrop_dl.managers.mock_manager import MOCK_MANAGER

    manager_ = manager or MOCK_MANAGER
    global existing_crawlers
    if not existing_crawlers:
        for crawler in CRAWLERS:
            crawler_instance = crawler(manager_)
            register_crawler(existing_crawlers, crawler_instance, include_generics)
    return existing_crawlers


def register_crawler(
    existing_crawlers: dict[str, Crawler],
    crawler: Crawler,
    include_generics: bool = False,
    from_user: bool | Literal["raise"] = False,
) -> None:
    if crawler.IS_GENERIC and include_generics:
        keys = (crawler.GENERIC_NAME,)
    else:
        keys = crawler.SCRAPE_MAPPER_KEYS

    for domain in keys:
        other = existing_crawlers.get(domain)
        if from_user:
            if not other and (match := match_url_to_crawler(existing_crawlers, crawler.PRIMARY_URL)):
                other = match
            if other:
                msg = (
                    f"Unable to assign {crawler.PRIMARY_URL} to generic crawler {crawler.GENERIC_NAME}. "
                    f"URL conflicts with URL format of builtin crawler {other.NAME}. "
                    "URL will be ignored"
                )
                if from_user == "raise":
                    raise ValueError(msg)
                log(msg, 40)
                continue
            else:
                log(f"Successfully mapped {crawler.PRIMARY_URL} to generic crawler {crawler.GENERIC_NAME}")

        elif other:
            msg = f"{domain} from {crawler.NAME} already registered by {other}"
            assert domain not in existing_crawlers, msg
        existing_crawlers[domain] = crawler


def get_unique_crawlers() -> list[Crawler]:
    return sorted(set(get_crawlers_mapping(include_generics=True).values()), key=lambda x: x.INFO.site)


def create_generic_crawlers_by_config(generic_crawlers: GenericCrawlerInstances) -> set[type[Crawler]]:
    new_crawlers: set[type[Crawler]] = set()
    if generic_crawlers.wordpress_html:
        new_crawlers.update(create_crawlers(generic_crawlers.wordpress_html, WordPressHTMLCrawler))
    if generic_crawlers.wordpress_media:
        new_crawlers.update(create_crawlers(generic_crawlers.wordpress_media, WordPressMediaCrawler))
    if generic_crawlers.discourse:
        new_crawlers.update(create_crawlers(generic_crawlers.discourse, DiscourseCrawler))
    if generic_crawlers.chevereto:
        new_crawlers.update(create_crawlers(generic_crawlers.chevereto, CheveretoCrawler))
    return new_crawlers


def disable_crawlers_by_config(existing_crawlers: dict[str, Crawler], crawlers_to_disable: list[str]) -> None:
    if not crawlers_to_disable:
        return

    crawlers_to_disable = sorted({name.casefold() for name in crawlers_to_disable})

    new_crawlers_mapping = {
        key: crawler
        for key, crawler in existing_crawlers.items()
        if crawler.INFO.site.casefold() not in crawlers_to_disable
    }
    disabled_crawlers = set(existing_crawlers.values()) - set(new_crawlers_mapping.values())
    if len(disabled_crawlers) != len(crawlers_to_disable):
        msg = (
            f"{len(crawlers_to_disable)} Crawler names where provided to disable"
            f", but only {len(disabled_crawlers)} {'is' if len(disabled_crawlers) == 1 else 'are'} a valid crawler's name."
        )
        log(msg, 30)

    if disabled_crawlers:
        existing_crawlers.clear()
        existing_crawlers.update(new_crawlers_mapping)
        crawlers_info = "\n".join(
            str({info.site: info.supported_domains}) for info in sorted(crawlers.INFO for crawlers in disabled_crawlers)
        )
        log(f"Crawlers disabled by config: \n{crawlers_info}")
    log_spacer(10)


def match_url_to_crawler(existing_crawlers: dict[str, Crawler], url: AbsoluteHttpURL) -> Crawler | None:
    # match exact domain
    if crawler := existing_crawlers.get(url.host):
        return crawler

    # get most restrictive domain if multiple domain matches
    try:
        domain = max((domain for domain in existing_crawlers if domain in url.host), key=len)
        existing_crawlers[url.host] = crawler = existing_crawlers[domain]
        return crawler
    except (ValueError, TypeError):
        return
