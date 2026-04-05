from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self, TypeVar

import aiofiles
from yarl import URL

from cyberdrop_dl.clients.jdownloader import JDownloader
from cyberdrop_dl.constants import REGEX_LINKS, BlockedDomains
from cyberdrop_dl.crawlers import create_crawlers
from cyberdrop_dl.crawlers._chevereto import CheveretoCrawler
from cyberdrop_dl.crawlers.crawler import Crawler
from cyberdrop_dl.crawlers.discourse import DiscourseCrawler
from cyberdrop_dl.crawlers.http_direct import DirectHttpFile
from cyberdrop_dl.crawlers.realdebrid import RealDebridCrawler
from cyberdrop_dl.crawlers.wordpress import WordPressHTMLCrawler, WordPressMediaCrawler
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.exceptions import JDownloaderError, NoExtensionError
from cyberdrop_dl.utils.logger import log_spacer
from cyberdrop_dl.utils.utilities import get_download_path, remove_trailing_slash

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Sequence

    import aiosqlite

    from cyberdrop_dl.config.global_model import GenericCrawlerInstances, GlobalSettings
    from cyberdrop_dl.managers.manager import Manager

_T = TypeVar("_T")
_CrawlerT = TypeVar("_CrawlerT", bound=Crawler)
logger = logging.getLogger(__name__)
existing_crawlers: dict[str, type[Crawler]] = {}
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
        self.jdownloader = JDownloader.from_manager(self.manager)
        self.using_input_file = False
        self.groups = set()
        self.count = 0
        self.real_debrid: RealDebridCrawler

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    @property
    def group_count(self) -> int:
        return len(self.groups)

    @property
    def global_settings(self) -> GlobalSettings:
        return self.manager.config_manager.global_settings_data

    def start_scrapers(self) -> None:
        """Starts all scrapers."""
        from cyberdrop_dl import plugins

        crawlers = get_crawlers_mapping()

        generic_crawlers = create_generic_crawlers_by_config(self.global_settings.generic_crawlers_instances)
        for crawler in generic_crawlers:
            register_crawler(crawlers, crawler, from_user=True)

        disable_crawlers_by_config(crawlers, self.global_settings.general.disable_crawlers)

        self.existing_crawlers = {domain: crawler(self.manager) for domain, crawler in crawlers.items()}

        plugins.load(self.manager)

    async def start_real_debrid(self) -> None:
        """Starts RealDebrid."""
        self.existing_crawlers["real-debrid"] = self.real_debrid = real = RealDebridCrawler(self.manager)
        await real.ready()

    @classmethod
    @contextlib.asynccontextmanager
    async def managed(cls, manager: Manager) -> AsyncGenerator[Self]:
        """Creates a new scrape mapper that auto closses http session on exit"""

        self = cls(manager)
        await self.manager.client_manager.load_cookie_files()

        async with self.manager.client_manager, self.manager.task_group:
            self.manager.scrape_mapper = self
            yield self

    async def run(self) -> None:
        """Starts the orchestra."""
        self.start_scrapers()
        await self.manager.database.history.update_previously_unsupported(self.existing_crawlers)
        try:
            await self.jdownloader.connect()
        except JDownloaderError:
            logger.exception("Failed to connect to jDownloader")

        await self.start_real_debrid()
        self.direct_crawler.__init_downloader__()
        async for item in self.get_input_items():
            self.manager.task_group.create_task(self.send_to_crawler(item))

    async def get_input_items(self) -> AsyncGenerator[ScrapeItem]:
        item_limit = 0
        if self.manager.parsed_args.cli_only_args.retry_any and self.manager.parsed_args.cli_only_args.max_items_retry:
            item_limit = self.manager.parsed_args.cli_only_args.max_items_retry

        if self.manager.parsed_args.cli_only_args.retry_failed:
            items_generator = self.load_failed_links()
        elif self.manager.parsed_args.cli_only_args.retry_all:
            items_generator = self.load_all_links()
        elif self.manager.parsed_args.cli_only_args.retry_maintenance:
            items_generator = self.load_all_bunkr_failed_links_via_hash()
        else:
            items_generator = self.load_links()

        async for item in items_generator:
            item.children_limits = self.manager.config_manager.settings_data.download_options.maximum_number_of_children
            if self.filter_items(item):
                if item_limit and self.count >= item_limit:
                    break
                yield item
                self.count += 1

        if not self.count:
            logger.warning("No valid links found")

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def parse_input_file_groups(self) -> AsyncGenerator[tuple[str, list[AbsoluteHttpURL]]]:
        """Split URLs from input file by their groups."""
        input_file = self.manager.config.files.input_file
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

    async def load_links(self) -> AsyncGenerator[ScrapeItem]:
        """Loads links from args / input file."""

        if not self.manager.parsed_args.cli_only_args.links:
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

        for url in self.manager.parsed_args.cli_only_args.links:
            yield ScrapeItem(url=url)

    async def load_failed_links(self) -> AsyncGenerator[ScrapeItem]:
        """Loads failed links from database."""
        async for rows in self.manager.database.history.get_failed_items():
            for row in rows:
                yield _create_item_from_row(row)

    async def load_all_links(self) -> AsyncGenerator[ScrapeItem]:
        """Loads all links from database."""
        after = self.manager.parsed_args.cli_only_args.completed_after or datetime.date.min
        before = self.manager.parsed_args.cli_only_args.completed_before or datetime.date.today()
        async for rows in self.manager.database.history.get_all_items(after, before):
            for row in rows:
                yield _create_item_from_row(row)

    async def load_all_bunkr_failed_links_via_hash(self) -> AsyncGenerator[ScrapeItem]:
        """Loads all bunkr links with maintenance hash."""
        async for rows in self.manager.database.history.get_all_bunkr_failed():
            for row in rows:
                yield _create_item_from_row(row)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def filter_and_send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Send scrape_item to a supported crawler."""
        if not isinstance(scrape_item.url, URL):
            scrape_item.url = AbsoluteHttpURL(scrape_item.url)
        if self.filter_items(scrape_item):
            await self.send_to_crawler(scrape_item)

    async def send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Maps URLs to their respective handlers."""
        scrape_item.url = remove_trailing_slash(scrape_item.url)
        crawler_match = match_url_to_crawler(self.existing_crawlers, scrape_item.url)

        if crawler_match:
            await crawler_match.ready()
            self.manager.task_group.create_task(crawler_match.run(scrape_item))
            return

        if not self.real_debrid.disabled and self.real_debrid.api.is_supported(scrape_item.url):
            logger.info(f"Using RealDebrid for unsupported URL: {scrape_item.url}")
            self.manager.task_group.create_task(self.real_debrid.run(scrape_item))
            return

        try:
            await self.direct_crawler.fetch(scrape_item)
            return

        except (NoExtensionError, ValueError):
            pass

        if self.jdownloader.is_enabled_for(scrape_item.url):
            logger.info(f"Sending unsupported URL to JDownloader: {scrape_item.url}")
            success = False
            try:
                download_folder = get_download_path(self.manager, scrape_item, "jdownloader")
                relative_download_dir = download_folder.relative_to(self.manager.config.files.download_folder)
                await self.jdownloader.send(
                    scrape_item.url,
                    scrape_item.parent_title,
                    relative_download_dir,
                )
                success = True
            except JDownloaderError as e:
                logger.error(f"Failed to send {scrape_item.url} to JDownloader\n{e.message}")
                self.manager.logs.write_unsupported(
                    scrape_item.url,
                    scrape_item.parents[0] if scrape_item.parents else None,
                )
            self.manager.progress_manager.scrape_stats_progress.add_unsupported(sent_to_jdownloader=success)
            return

        logger.warning(f"Unsupported URL: {scrape_item.url}")
        self.manager.logs.write_unsupported(
            scrape_item.url,
            scrape_item.parents[0] if scrape_item.parents else None,
        )
        self.manager.progress_manager.scrape_stats_progress.add_unsupported()

    def filter_items(self, scrape_item: ScrapeItem) -> bool:
        """Pre-filter scrape items base on URL."""

        if scrape_item.url in _seen_urls:
            return False
        _seen_urls.add(scrape_item.url)

        if (
            is_in_domain_list(scrape_item, BlockedDomains.partial_match)
            or scrape_item.url.host in BlockedDomains.exact_match
        ):
            logger.info(f"Skipping {scrape_item.url} as it is a blocked domain")
            return False

        before = self.manager.parsed_args.cli_only_args.completed_before
        after = self.manager.parsed_args.cli_only_args.completed_after
        if is_outside_date_range(scrape_item, before, after):
            logger.info(f"Skipping {scrape_item.url} as it is outside of the desired date range")
            return False

        skip_hosts = self.manager.config_manager.settings_data.ignore_options.skip_hosts
        if skip_hosts and is_in_domain_list(scrape_item, skip_hosts):
            logger.info(f"Skipping URL by skip_hosts config: {scrape_item.url}")
            return False

        only_hosts = self.manager.config_manager.settings_data.ignore_options.only_hosts
        if only_hosts and not is_in_domain_list(scrape_item, only_hosts):
            logger.info(f"Skipping URL by only_hosts config: {scrape_item.url}")
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
            logger.error(f"Unable to parse URL from input file: {link} {e:!r}")


def _create_item_from_row(row: aiosqlite.Row) -> ScrapeItem:
    referer: str = row["referer"]
    url = AbsoluteHttpURL(referer, encoded="%" in referer)
    item = ScrapeItem(url=url, retry_path=Path(row["download_path"]), part_of_album=True)
    if completed_at := row["completed_at"]:
        item.completed_at = int(datetime.datetime.fromisoformat(completed_at).timestamp())
    if created_at := row["created_at"]:
        item.created_at = int(datetime.datetime.fromisoformat(created_at).timestamp())
    return item


def get_crawlers_mapping(include_generics: bool = False) -> dict[str, type[Crawler]]:
    """Returns a mapping with an instance of all crawlers.

    Crawlers are only created on the first calls. Future calls always return a reference to the same crawlers

    If manager is `None`, the `MOCK_MANAGER` will be used, which means the crawlers won't be able to actually run"""

    from cyberdrop_dl.crawlers.crawler import Registry

    global existing_crawlers
    if existing_crawlers:
        return existing_crawlers

    Registry.import_all()
    crawlers = Registry.generic | Registry.concrete

    for crawler in crawlers:
        register_crawler(existing_crawlers, crawler, include_generics)

    copy = existing_crawlers.copy()
    existing_crawlers.clear()
    existing_crawlers.update(sorted(copy.items()))
    return existing_crawlers


def register_crawler(
    existing_crawlers: dict[str, type[Crawler]],
    crawler: type[Crawler],
    include_generics: bool = False,
    from_user: bool | Literal["raise"] = False,
) -> None:
    if crawler.IS_GENERIC and include_generics:
        keys = (crawler.NAME,)
    else:
        keys = crawler.SCRAPE_MAPPER_KEYS

    for domain in keys:
        other = existing_crawlers.get(domain)
        if from_user:
            if not other and (match := match_url_to_crawler(existing_crawlers, crawler.PRIMARY_URL)):
                other = match
            if other:
                msg = (
                    f"Unable to assign {crawler.PRIMARY_URL} to generic crawler {crawler.NAME}. "
                    f"URL conflicts with URL format of builtin crawler {other.NAME}. "
                    "URL will be ignored"
                )
                if from_user == "raise":
                    raise ValueError(msg)
                logger.error(msg)
                continue
            else:
                logger.info(f"Successfully mapped {crawler.PRIMARY_URL} to crawler {crawler.NAME}")

        elif other:
            msg = f"{domain} from {crawler.NAME} already registered by {other}"
            assert domain not in existing_crawlers, msg
        existing_crawlers[domain] = crawler


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


def disable_crawlers_by_config(existing_crawlers: dict[str, type[Crawler]], crawlers_to_disable: list[str]) -> None:
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
        logger.warning(msg)

    if disabled_crawlers:
        existing_crawlers.clear()
        existing_crawlers.update(new_crawlers_mapping)
        crawlers_info = "\n".join(
            str({info.site: info.supported_domains}) for info in sorted(c.INFO for c in disabled_crawlers)
        )
        logger.info(f"Crawlers disabled by config: \n{crawlers_info}")

    log_spacer()


def match_url_to_crawler(existing_crawlers: dict[str, _T], url: AbsoluteHttpURL) -> _T | None:
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
