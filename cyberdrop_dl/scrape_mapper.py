from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar

import aiofiles

from cyberdrop_dl import aio
from cyberdrop_dl.clients.jdownloader import JDownloader
from cyberdrop_dl.constants import BlockedDomains
from cyberdrop_dl.crawlers import create_crawlers
from cyberdrop_dl.crawlers._chevereto import CheveretoCrawler
from cyberdrop_dl.crawlers.crawler import Crawler
from cyberdrop_dl.crawlers.discourse import DiscourseCrawler
from cyberdrop_dl.crawlers.http_direct import DirectHttpFile
from cyberdrop_dl.crawlers.realdebrid import RealDebridCrawler
from cyberdrop_dl.crawlers.wordpress import WordPressHTMLCrawler, WordPressMediaCrawler
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.exceptions import JDownloaderError, NoExtensionError
from cyberdrop_dl.logs import log_spacer
from cyberdrop_dl.utils.utilities import get_download_path, remove_trailing_slash

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Coroutine, Generator, Sequence

    import aiosqlite

    from cyberdrop_dl.config.global_model import GenericCrawlerInstances, GlobalSettings
    from cyberdrop_dl.managers.manager import Manager

_T = TypeVar("_T")
_CrawlerT = TypeVar("_CrawlerT", bound=Crawler)
logger = logging.getLogger(__name__)


_crawlers_disabled_at_runtime: set[str] = set()

REGEX_LINKS = re.compile(r"(?:http.*?)(?=($|\n|\r\n|\r|\s|\"|\[/URL]|']\[|]\[|\[/img]))")


def _is_outside_date_range(scrape_item: ScrapeItem, before: datetime.date | None, after: datetime.date | None) -> bool:
    skip = False
    item_date = scrape_item.completed_at or scrape_item.created_at
    if not item_date:
        return False
    date = datetime.datetime.fromtimestamp(item_date).date()
    if (after and date < after) or (before and date > before):
        skip = True

    return skip


def _is_in_domain_list(scrape_item: ScrapeItem, domain_list: Sequence[str]) -> bool:
    return any(domain in scrape_item.url.host for domain in domain_list)


@dataclasses.dataclass(slots=True)
class TaskGroups:
    scrape: asyncio.TaskGroup
    downloads: asyncio.TaskGroup


@dataclasses.dataclass(slots=True)
class ScrapeMapper:
    """This class maps links to their respective handlers, or JDownloader if they are unsupported."""

    manager: Manager
    direct_crawler: DirectHttpFile = dataclasses.field(init=False)
    jdownloader: JDownloader = dataclasses.field(init=False)
    real_debrid: RealDebridCrawler = dataclasses.field(init=False)

    using_input_file: bool = dataclasses.field(init=False, default=False)
    groups: set[str] = dataclasses.field(init=False, default_factory=set)
    count: int = dataclasses.field(init=False, default=0)
    existing_crawlers: dict[str, Crawler] = dataclasses.field(init=False, default_factory=dict)
    task_groups: TaskGroups = dataclasses.field(
        init=False, default_factory=lambda: TaskGroups(asyncio.TaskGroup(), asyncio.TaskGroup())
    )
    _seen_urls: set[AbsoluteHttpURL] = dataclasses.field(init=False, default_factory=set)

    def __post_init__(self) -> None:
        self.direct_crawler = DirectHttpFile(self.manager)
        self.jdownloader = JDownloader.from_manager(self.manager)
        self.existing_crawlers["real-debrid"] = self.real_debrid = RealDebridCrawler(self.manager)

    def create_task(self, coro: Coroutine[Any, Any, _T]) -> None:
        _ = self.task_groups.scrape.create_task(coro)

    def create_download_task(self, coro: Coroutine[Any, Any, _T]) -> None:
        _ = self.task_groups.downloads.create_task(coro)

    @property
    def group_count(self) -> int:
        return len(self.groups)

    @property
    def global_settings(self) -> GlobalSettings:
        return self.manager.global_config

    def _init_crawlers(self) -> None:
        from cyberdrop_dl import plugins

        crawlers = get_crawlers_mapping()

        for crawler in _create_generic_crawlers(self.global_settings.generic_crawlers_instances):
            register_crawler(crawlers, crawler, from_user=True)

        _disable_crawlers_by_config(crawlers, self.global_settings.general.disable_crawlers)

        self.existing_crawlers.update((domain, crawler(self.manager)) for domain, crawler in crawlers.items())

        plugins.load(self.manager)

    @classmethod
    @contextlib.asynccontextmanager
    async def managed(cls, manager: Manager) -> AsyncGenerator[Self]:
        """Creates a new scrape mapper that auto closses http session on exit"""

        self = cls(manager)
        await self.manager.client_manager.load_cookie_files()

        async with (
            self.manager.client_manager,
            self.manager.task_group,
            self.task_groups.downloads,
            self.task_groups.scrape,
        ):
            self.manager.scrape_mapper = self
            yield self

    async def run(self) -> None:
        self._init_crawlers()
        await self.manager.database.history.update_previously_unsupported(self.existing_crawlers)
        try:
            await self.jdownloader.connect()
        except JDownloaderError:
            logger.exception("Failed to connect to jDownloader")

        await self.real_debrid.__async_init__()
        self.direct_crawler.__init_downloader__()
        async for item in self.get_input_items():
            self.create_task(self._send_to_crawler(item))

    async def get_input_items(self) -> AsyncGenerator[ScrapeItem]:
        item_limit = 0
        if self.manager.parsed_args.cli_only_args.retry_any and self.manager.parsed_args.cli_only_args.max_items_retry:
            item_limit = self.manager.parsed_args.cli_only_args.max_items_retry

        if self.manager.parsed_args.cli_only_args.retry_failed:
            items_generator = load_failed_links(self.manager)
        elif self.manager.parsed_args.cli_only_args.retry_all:
            items_generator = load_all_links(self.manager)
        elif self.manager.parsed_args.cli_only_args.retry_maintenance:
            items_generator = load_all_bunkr_failed_links_via_hash(self.manager)
        else:
            items_generator = self._load_links()

        async for item in items_generator:
            item.children_limits = self.manager.config.download_options.maximum_number_of_children
            if self._should_scrape(item):
                if item_limit and self.count >= item_limit:
                    break
                yield item
                self.count += 1

        if not self.count:
            logger.warning("No valid links found")

    async def parse_input_file_groups(self) -> AsyncGenerator[tuple[str, list[AbsoluteHttpURL]]]:
        """Split URLs from input file by their groups."""
        input_file = self.manager.config.files.input_file
        if not await aio.is_file(input_file):
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

    async def _load_links(self) -> AsyncGenerator[ScrapeItem]:
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

    async def scrape(self, scrape_item: ScrapeItem) -> None:
        if self._should_scrape(scrape_item):
            await self._send_to_crawler(scrape_item)

    async def _send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Maps URLs to their respective handlers."""
        scrape_item.url = remove_trailing_slash(scrape_item.url)
        crawler_match = _best_match(self.existing_crawlers, scrape_item.url.host)

        if crawler_match:
            await crawler_match.__async_init__()
            self.create_task(crawler_match.run(scrape_item))
            return

        if not self.real_debrid.disabled and self.real_debrid.api.is_supported(scrape_item.url):
            logger.info(f"Using RealDebrid for unsupported URL: {scrape_item.url}")
            self.create_task(self.real_debrid.run(scrape_item))
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

    def _should_scrape(self, scrape_item: ScrapeItem) -> bool:
        """Pre-filter scrape items base on URL."""

        if scrape_item.url in self._seen_urls:
            return False

        self._seen_urls.add(scrape_item.url)

        if (
            _is_in_domain_list(scrape_item, BlockedDomains.partial_match)
            or scrape_item.url.host in BlockedDomains.exact_match
        ):
            logger.info(f"Skipping {scrape_item.url} as it is a blocked domain")
            return False

        before = self.manager.parsed_args.cli_only_args.completed_before
        after = self.manager.parsed_args.cli_only_args.completed_after

        if _is_outside_date_range(scrape_item, before, after):
            logger.info(f"Skipping {scrape_item.url} as it is outside of the desired date range")
            return False

        skip_hosts = self.manager.config.ignore_options.skip_hosts
        if skip_hosts and _is_in_domain_list(scrape_item, skip_hosts):
            logger.info(f"Skipping {scrape_item.url} by skip_hosts config")
            return False

        only_hosts = self.manager.config.ignore_options.only_hosts
        if only_hosts and not _is_in_domain_list(scrape_item, only_hosts):
            logger.info(f"Skipping {scrape_item.url} by only_hosts config")
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
    from cyberdrop_dl.crawlers.crawler import Registry

    Registry.import_all()

    crawlers_map: dict[str, type[Crawler]] = {}
    for crawler in sorted(Registry.generic | Registry.concrete, key=lambda c: c.NAME):
        register_crawler(crawlers_map, crawler, include_generics)

    copy = crawlers_map.copy()
    crawlers_map.clear()
    crawlers_map.update(sorted(copy.items()))
    return crawlers_map


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
            if not other and (match := _best_match(existing_crawlers, crawler.PRIMARY_URL.host)):
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


def _create_generic_crawlers(generics_config: GenericCrawlerInstances) -> Generator[type[Crawler]]:
    if generics_config.wordpress_html:
        yield from create_crawlers(generics_config.wordpress_html, WordPressHTMLCrawler)
    if generics_config.wordpress_media:
        yield from create_crawlers(generics_config.wordpress_media, WordPressMediaCrawler)
    if generics_config.discourse:
        yield from create_crawlers(generics_config.discourse, DiscourseCrawler)
    if generics_config.chevereto:
        yield from create_crawlers(generics_config.chevereto, CheveretoCrawler)


def _disable_crawlers_by_config(current_crawlers: dict[str, type[Crawler]], crawlers_to_disable: list[str]) -> None:
    if not crawlers_to_disable:
        return

    crawlers_to_disable = sorted({name.casefold() for name in crawlers_to_disable})

    new_crawlers_mapping = {
        key: crawler
        for key, crawler in current_crawlers.items()
        if crawler.INFO.site.casefold() not in crawlers_to_disable
    }
    disabled_crawlers = set(current_crawlers.values()) - set(new_crawlers_mapping.values())

    if len(disabled_crawlers) != len(crawlers_to_disable):
        msg = (
            f"{len(crawlers_to_disable)} Crawler names where provided to disable"
            f", but only {len(disabled_crawlers)} {'is' if len(disabled_crawlers) == 1 else 'are'} a valid crawler's name."
        )
        logger.warning(msg)

    if disabled_crawlers:
        current_crawlers.clear()
        current_crawlers.update(new_crawlers_mapping)
        crawlers_info = "\n".join(
            str({info.site: info.supported_domains}) for info in sorted(c.INFO for c in disabled_crawlers)
        )
        logger.info(f"Crawlers disabled by config: \n{crawlers_info}")

    log_spacer()


def _best_match(current_map: dict[str, _T], key: str) -> _T | None:
    if found := current_map.get(key):
        return found

    try:
        best_match = max((k for k in current_map if k in key), key=len)
    except (ValueError, TypeError):
        return
    else:
        current_map[key] = found = current_map[best_match]
        return found


async def load_failed_links(manager: Manager) -> AsyncGenerator[ScrapeItem]:
    async for rows in manager.database.history.get_failed_items():
        for row in rows:
            yield _create_item_from_row(row)


async def load_all_links(manager: Manager) -> AsyncGenerator[ScrapeItem]:
    after = manager.parsed_args.cli_only_args.completed_after or datetime.date.min
    before = manager.parsed_args.cli_only_args.completed_before or datetime.date.today()
    async for rows in manager.database.history.get_all_items(after, before):
        for row in rows:
            yield _create_item_from_row(row)


async def load_all_bunkr_failed_links_via_hash(manager: Manager) -> AsyncGenerator[ScrapeItem]:
    async for rows in manager.database.history.get_all_bunkr_failed():
        for row in rows:
            yield _create_item_from_row(row)
