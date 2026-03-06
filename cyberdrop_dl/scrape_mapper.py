from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import logging
import re
from collections import defaultdict
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, TypeVar

from cyberdrop_dl import aio, plugins, storage
from cyberdrop_dl.client.jdownloader import JDownloader
from cyberdrop_dl.constants import REGEX_LINKS, BlockedDomains
from cyberdrop_dl.crawlers import create_crawlers
from cyberdrop_dl.crawlers._chevereto import CheveretoCrawler
from cyberdrop_dl.crawlers.discourse import DiscourseCrawler
from cyberdrop_dl.crawlers.http_direct import DirectHTTPFile
from cyberdrop_dl.crawlers.realdebrid import RealDebridCrawler
from cyberdrop_dl.crawlers.wordpress import WordPressHTMLCrawler, WordPressMediaCrawler
from cyberdrop_dl.data_structures import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.exceptions import JDownloaderError, NoExtensionError
from cyberdrop_dl.logger import spacer
from cyberdrop_dl.utils import best_match, filepath, get_download_path, parse_url, remove_trailing_slash

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Coroutine, Generator, Iterable

    import aiosqlite

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.crawlers import Crawler
    from cyberdrop_dl.database import Database
    from cyberdrop_dl.manager import Manager

    CrawlerT = TypeVar("CrawlerT", bound=Crawler)

_seen_urls: set[AbsoluteHttpURL] = set()
_crawlers_disabled_at_runtime: set[str] = set()

logger = logging.getLogger(__name__)


def filter_by_date(scrape_item: ScrapeItem, before: datetime.date | None, after: datetime.date | None) -> bool:
    item_date = scrape_item.completed_at or scrape_item.created_at
    if not item_date:
        return False
    date = datetime.datetime.fromtimestamp(item_date).date()
    before, after = before or datetime.date.max, after or datetime.date.min
    return after < date < before


def filter_by_domain(scrape_item: ScrapeItem, domains: Iterable[str]) -> bool:
    return any(domain in scrape_item.url.host for domain in domains)


@dataclasses.dataclass(slots=True, eq=False)
class CrawlerFactory:
    manager: Manager
    _instances: dict[type[Crawler], Crawler] = dataclasses.field(default_factory=dict)

    def __getitem__(self, obj: type[CrawlerT]) -> CrawlerT:
        instance = self.get(obj)
        if instance is None:
            instance = self._instances[obj] = obj(self.manager)
        return instance

    def __contains__(self, obj: type[CrawlerT]) -> bool:
        return obj in self._instances

    def get(self, obj: type[CrawlerT]) -> CrawlerT | None:
        return self._instances.get(obj)  # pyright: ignore[reportReturnType]


@dataclasses.dataclass(slots=True)
class ScrapeStats:
    count: int = dataclasses.field(init=False, default=0)
    groups: list[str] = dataclasses.field(init=False, default_factory=list)
    url_count: dict[str, int] = dataclasses.field(init=False, default_factory=lambda: defaultdict(int))

    @property
    def unique_groups(self) -> list[str]:
        return list(dict.fromkeys(self.groups))

    def update(self, item: ScrapeItem) -> None:
        self.count += 1
        if item.parent_title:
            self.groups.append(item.parent_title)
        self.url_count[item.url.host] += 1


def parse_input(source: Iterable[AbsoluteHttpURL] | Path) -> AsyncGenerator[ScrapeItem]:
    if isinstance(source, Path):
        return from_file(source)
    return from_urls(source)


async def from_urls(source: Iterable[AbsoluteHttpURL]) -> AsyncGenerator[ScrapeItem, None]:
    for url in source:
        yield ScrapeItem(url=url)


async def from_file(file: Path) -> AsyncGenerator[ScrapeItem]:
    """Loads links from args / input file."""
    async for group_name, urls in _parse_input_file_groups(file):
        for url in urls:
            item = ScrapeItem(url=url)
            if group_name:
                item.add_to_parent_title(group_name)
                item.part_of_album = True
            yield item


async def _parse_input_file_groups(input_file: Path) -> AsyncGenerator[tuple[str | None, list[AbsoluteHttpURL]]]:
    """Split URLs from input file by their groups."""

    if not await aio.is_file(input_file):
        yield (None, [])
        return

    block_quote = False
    current_group_name = ""
    async with aio.open(input_file, encoding="utf8") as fp:
        async for line in fp:
            if line.startswith(("---", "===")):  # New group begins here
                current_group_name = line.replace("---", "").replace("===", "").strip()

            if current_group_name:
                yield (current_group_name, list(regex_links(line)))
                continue

            block_quote = not block_quote if line == "#\n" else block_quote
            if not block_quote:
                yield (None, list(regex_links(line)))


async def load_failed_links(database: Database) -> AsyncGenerator[ScrapeItem]:
    """Loads failed links from database."""
    async for rows in database.history_table.get_failed_items():
        for row in rows:
            yield _create_item_from_row(row)


async def load_bunkr_fails_via_hash(database: Database) -> AsyncGenerator[ScrapeItem]:
    """Loads all bunkr links with maintenance hash."""
    async for rows in database.history_table.get_all_bunkr_failed():
        for row in rows:
            yield _create_item_from_row(row)


class ScrapeMapper(aio.AsyncContextManagerMixin):
    """This class maps links to their respective handlers, or JDownloader if they are unsupported."""

    def __init__(self, manager: Manager) -> None:
        self.manager: Manager = manager
        self.config: Config = manager.config
        self.crawlers: dict[str, type[Crawler]] = {}
        self.factory: CrawlerFactory = CrawlerFactory(manager)
        self.direct_http: DirectHTTPFile = DirectHTTPFile(manager)
        self.jdownloader: JDownloader = JDownloader.from_config(self.config)
        self.crawlers["real-debrid"] = RealDebridCrawler
        self.real_debrid: RealDebridCrawler = self.factory[RealDebridCrawler]
        self._ready: bool = False

    @contextlib.asynccontextmanager
    async def _asyncctx_(self) -> AsyncGenerator[Self]:
        _ = filepath.MAX_FILE_LEN.set(self.config.general.max_file_name_length)
        _ = filepath.MAX_FOLDER_LEN.set(self.config.general.max_folder_name_length)

        logger.info("Starting Async Processes...")
        async with (
            self.manager.task_group,
            self.manager.client,  # TODO: with database
            storage.monitor(self.config.general.required_free_space),
        ):
            self.manager.log_app_state()
            await self.manager.client.load_cookies()
            logger.info(spacer())
            logger.info("Starting CDL...\n")
            with self.manager.tui(screen="scraping"):
                yield self

    def _create_task(self, coro: Coroutine[Any, Any, Any]) -> None:
        _ = self.manager.task_group.create_task(coro)

    async def ready(self) -> None:
        if self._ready:
            return
        self.crawlers.update(get_crawlers_mapping())
        generic_crawlers = create_generic_crawlers(self.config)
        for crawler in generic_crawlers:
            register_crawler(self.crawlers, crawler, from_user=True)

        disable_crawlers(self.crawlers, self.config)
        plugins.load(self.manager)
        _ = await asyncio.gather(self.jdownloader.ready(), self.real_debrid.ready(), self.direct_http.ready())
        self._ready = True

    async def run(self, source: Iterable[AbsoluteHttpURL] | Path) -> ScrapeStats:
        """Starts the orchestra."""
        await self.ready()
        stats = ScrapeStats()
        async for item in parse_input(source):
            stats.update(item)
            item._children_limits = self.config.download.max_children
            self._create_task(self.filter_and_send_to_crawler(item))
        return stats

    async def filter_and_send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Send scrape_item to a supported crawler."""
        if self.should_scrape(scrape_item):
            await self.send_to_crawler(scrape_item)

    async def send_to_crawler(self, scrape_item: ScrapeItem) -> None:
        """Maps URLs to their respective handlers."""
        scrape_item.url = remove_trailing_slash(scrape_item.url)

        if cls := best_match(scrape_item.url.host, self.crawlers):
            crawler = self.factory[cls]
            await crawler.ready()
            self._create_task(crawler.run(scrape_item))
            return

        if self.real_debrid.enabled and self.real_debrid.api.is_supported(scrape_item.url):
            logger.info(f"Using RealDebrid for unsupported URL: {scrape_item.url}")
            self._create_task(self.real_debrid.run(scrape_item))
            return

        try:
            await self.direct_http.fetch(scrape_item)
            return

        except (NoExtensionError, ValueError):
            pass

        if self.jdownloader.enabled and self.jdownloader.is_whitelisted(scrape_item.url):
            logger.info(f"Sending unsupported URL to JDownloader: {scrape_item.url}")

            try:
                download_folder = get_download_path(self.manager, scrape_item, "jdownloader")
                relative_download_dir = download_folder.relative_to(self.config.filesystem.download_folder)
                await self.jdownloader.send(
                    scrape_item.url,
                    scrape_item.parent_title,
                    relative_download_dir,
                )
                success = True
            except JDownloaderError as e:
                logger.error(f"Failed to send {scrape_item.url} to JDownloader\n{e.message}")
                self.manager.logs.write_unsupported(scrape_item.url, scrape_item)
                success = False

            self.manager.tui.scrape_errors.add_unsupported(sent_to_jdownloader=success)
            return

        logger.warning(f"Unsupported URL: {scrape_item.url}")
        self.manager.logs.write_unsupported(scrape_item.url, scrape_item)
        self.manager.tui.scrape_errors.add_unsupported()

    def should_scrape(self, scrape_item: ScrapeItem) -> bool:
        """Pre-filter scrape items base on URL."""

        if scrape_item.url in _seen_urls:
            return False

        _seen_urls.add(scrape_item.url)

        if (
            filter_by_domain(scrape_item, BlockedDomains.partial_match)
            or scrape_item.url.host in BlockedDomains.exact_match
        ):
            logger.info(f"Skipping {scrape_item.url} as it is a blocked domain")
            return False

        skip_hosts = self.config.ignore.skip_hosts
        if skip_hosts and filter_by_domain(scrape_item, skip_hosts):
            logger.info(f"Skipping URL by skip_hosts config: {scrape_item.url}")
            return False

        only_hosts = self.config.ignore.only_hosts
        if only_hosts and not filter_by_domain(scrape_item, only_hosts):
            logger.info(f"Skipping URL by only_hosts config: {scrape_item.url}")
            return False

        return True

    def disable_crawler(self, domain: str) -> type[Crawler] | None:
        """Disables a crawler at runtime, after the scrape mapper is already running.

        It does not remove the crawler from the crawlers map, it just sets it as `disabled"`

        This has the effect to silently ignore any URL that maps to that crawler, without any "unsupported" or "errors" log messages

        `domain` must match _exactly_, AKA: it must be the value of `crawler.DOMAIN`

        Returns the crawler class that was disabled (if Any)

        """

        if domain in _crawlers_disabled_at_runtime:
            return

        crawler = next((crawler for crawler in self.crawlers.values() if crawler.DOMAIN == domain), None)
        if not crawler or crawler.disabled:
            return

        crawler.disabled = True
        _crawlers_disabled_at_runtime.add(domain)
        if instance := self.factory.get(crawler):
            instance.disabled = True
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
            yield parse_url(link)
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
    from cyberdrop_dl.crawlers import Registry

    Registry.import_all()

    crawlers = Registry.concrete
    if include_generics:
        crawlers = crawlers | Registry.generic
    existing_crawlers: dict[str, type[Crawler]] = {}
    for crawler in crawlers:
        register_crawler(existing_crawlers, crawler, include_generics)
    return existing_crawlers


def register_crawler(
    existing_crawlers: dict[str, type[Crawler]],
    crawler: type[Crawler],
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
            if not other and (
                match := best_match(
                    crawler.PRIMARY_URL.host,
                    existing_crawlers,
                )
            ):
                other = match
            if other:
                msg = (
                    f"Unable to assign {crawler.PRIMARY_URL} to generic crawler {crawler.GENERIC_NAME}. "
                    f"URL conflicts with URL format of builtin crawler {other.NAME}. "
                    "URL will be ignored"
                )
                if from_user == "raise":
                    raise ValueError(msg)
                logger.error(msg)
                continue
            else:
                logger.info(f"Successfully mapped {crawler.PRIMARY_URL} to generic crawler {crawler.GENERIC_NAME}")

        elif other:
            msg = f"{domain} from {crawler.NAME} already registered by {other}"
            assert domain not in existing_crawlers, msg
        existing_crawlers[domain] = crawler


def get_unique_crawlers() -> list[type[Crawler]]:
    return sorted(set(get_crawlers_mapping(include_generics=True).values()), key=lambda x: x.INFO.site)


def create_generic_crawlers(config: Config) -> set[type[Crawler]]:
    new_crawlers: set[type[Crawler]] = set()
    generic_crawlers = config.generic_crawlers

    for cls, urls in {
        WordPressHTMLCrawler: generic_crawlers.wordpress_html,
        WordPressMediaCrawler: generic_crawlers.wordpress_media,
        DiscourseCrawler: generic_crawlers.discourse,
        CheveretoCrawler: generic_crawlers.chevereto,
    }.items():
        if urls:
            new_crawlers.update(create_crawlers(cls, *urls))

    return new_crawlers


def disable_crawlers(existing_crawlers: dict[str, type[Crawler]], config: Config) -> None:
    crawlers_to_disable = config.general.disable_crawlers
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
            str({info.site: info.supported_domains}) for info in sorted(crawlers.INFO for crawlers in disabled_crawlers)
        )
        logger.info(f"Crawlers disabled by config: \n{crawlers_info}")
    logger.info(spacer())
