from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import importlib
import logging
import pkgutil
import re
import weakref
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable
from functools import partial, wraps
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Concatenate,
    Literal,
    ParamSpec,
    Self,
    TypeAlias,
    TypeVar,
    final,
)

from aiolimiter import AsyncLimiter

from cyberdrop_dl.annotations import copy_signature
from cyberdrop_dl.clients.http import HTTPClient
from cyberdrop_dl.data_structures.mediaprops import ISO639Subtitle, Resolution
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem, ScrapeItem
from cyberdrop_dl.downloader.downloader import Downloader
from cyberdrop_dl.exceptions import MaxChildrenError, NoExtensionError, ScrapeError
from cyberdrop_dl.utils import (
    css,
    dates,
    error_handling_context,
    error_handling_wrapper,
    get_download_path,
    is_absolute_http_url,
    is_blob_or_svg,
    m3u8,
    parse_url,
    remove_trailing_slash,
)
from cyberdrop_dl.utils.filepath import compose_custom_filename, get_filename_and_ext, sanitize_filename
from cyberdrop_dl.utils.strings import safe_format

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Coroutine, Generator, Iterable
    from http.cookies import BaseCookie

    from bs4 import BeautifulSoup, Tag
    from curl_cffi.requests.impersonate import BrowserTypeLiteral

    from cyberdrop_dl.clients.response import AbstractResponse
    from cyberdrop_dl.manager import Manager
    from cyberdrop_dl.tui.common import ProgressHook

_P = ParamSpec("_P")
_R = TypeVar("_R")
_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
OneOrTuple: TypeAlias = _T | tuple[_T, ...]
SupportedPaths: TypeAlias = dict[str, OneOrTuple[str]]
SupportedDomains: TypeAlias = OneOrTuple[str]
RateLimit = tuple[float, float]
_DBPathBuilder = Callable[[AbsoluteHttpURL], str]
DBPathBuilder = _DBPathBuilder | Literal["url", "name", "path", "path_qs", "path_qs_frag", "path_frag"]
_DB_PATH_BUILDERS: dict[str, _DBPathBuilder] = {
    "url": lambda url: str(url),
    "name": lambda url: url.name,
    "path": lambda url: url.path,
    "path_qs": lambda url: url.path_qs,
    "path_qs_frag": lambda url: f"{url.path_qs}#{frag}" if (frag := url.fragment) else url.path_qs,
    "path_frag": lambda url: f"{url.path}#{frag}" if (frag := url.fragment) else url.path,
}


_HASH_PREFIXES = "md5:", "sha1:", "sha256:", "xxh128:"
_VALID_RESOLUTION_NAMES = "4K", "8K", "HQ", "Unknown"


@dataclasses.dataclass(slots=True, frozen=True)
class PlaceHolderConfig:
    include_file_id: bool = True
    include_video_codec: bool = True
    include_audio_codec: bool = True
    include_resolution: bool = True
    include_hash: bool = True


_placeholder_config = PlaceHolderConfig()


@dataclasses.dataclass(slots=True, order=True)
class CrawlerInfo:
    site: str
    primary_url: AbsoluteHttpURL
    supported_domains: tuple[str, ...]
    supported_paths: SupportedPaths

    @classmethod
    def generic(cls, name: str, paths: SupportedPaths) -> Self:
        return cls(name, "::GENERIC CRAWLER::", (), paths)  # pyright: ignore[reportArgumentType]

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(f.name for f in dataclasses.fields(self))


class Registry:
    abc: weakref.WeakSet[type[Crawler]] = weakref.WeakSet()
    concrete: weakref.WeakSet[type[Crawler]] = weakref.WeakSet()
    generic: weakref.WeakSet[type[Crawler]] = weakref.WeakSet()
    # generics are concrete crawlers that are not bound to any specific site
    # They can be mapped to a site by just subclassing and setting a PRIMARY URL. ex: Chevereto

    _loaded: bool = False
    _all: weakref.WeakSet[type[Crawler]] = weakref.WeakSet()

    @classmethod
    def all(cls) -> weakref.WeakSet[type[Crawler]]:
        cls.import_all()
        return cls._all

    @classmethod
    def import_all(cls) -> None:
        if cls._loaded:
            return
        cls._import(__package__ or __name__)
        cls._all.update(cls.concrete | cls.generic | cls.abc)
        cls._loaded = True

    @classmethod
    def _import(cls, pkg_name: str) -> None:
        """Import every module (and sub-package) inside *pkg_name*."""
        module = importlib.import_module(pkg_name)
        for module_info in pkgutil.iter_modules(module.__path__, pkg_name + "."):
            _ = importlib.import_module(module_info.name)
            if module_info.ispkg:
                cls._import(module_info.name)


class Crawler(ABC):
    OLD_DOMAINS: ClassVar[tuple[str, ...]] = ()
    SUPPORTED_DOMAINS: ClassVar[SupportedDomains] = ()
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {}
    DEFAULT_POST_TITLE_FORMAT: ClassVar[str] = "{date} - {id} - {title}"

    UPDATE_UNSUPPORTED: ClassVar[bool] = False
    ALLOW_EMPTY_PATH: ClassVar[bool] = False
    NEXT_PAGE_SELECTOR: ClassVar[str] = ""

    DEFAULT_TRIM_URLS: ClassVar[bool] = True
    FOLDER_DOMAIN: ClassVar[str] = ""
    DOMAIN: ClassVar[str]
    PRIMARY_URL: ClassVar[AbsoluteHttpURL]

    _RATE_LIMIT: ClassVar[RateLimit] = 25, 1
    _DOWNLOAD_SLOTS: ClassVar[int | None] = None
    _USE_DOWNLOAD_SERVERS_LOCKS: ClassVar[bool] = False
    _IMPERSONATE: ClassVar[BrowserTypeLiteral | bool | None] = None

    DB_PATH_BUILDER: ClassVar[DBPathBuilder] = "path"
    disabled: bool = False

    @abstractmethod
    async def fetch(self, scrape_item: ScrapeItem) -> None: ...

    @final
    @staticmethod
    def _assert_fields_overrides(subclass: type[Crawler], *fields: str) -> None:
        for field_name in fields:
            assert getattr(subclass, field_name, None), f"Subclass {subclass.__name__} must override: {field_name}"

    @final
    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        self.config = manager.config
        self.downloader: Downloader
        self.client: HTTPClient = manager.client
        self._startup_lock: asyncio.Lock = asyncio.Lock()
        self._ready: bool = False

        self.logged_in: bool = False
        self._scraped_items: set[str] = set()
        self.logger = logging.getLogger(type(self).__qualname__)
        self._semaphore = asyncio.Semaphore(20)
        self.__post_init__()

    def __post_init__(self) -> None:  # noqa: B027
        """Override in subclasses to add custom init logic

        This method gets called inmediatly on class creation"""

    async def _async_post_init_(self) -> None:  # noqa: B027
        """Perform additional setup that requires I/O

        ex: login, getting API tokens, etc..

        This method its called once and only if the crawler is actually going to be scrape something"""

    @final
    async def _async_init_(self) -> None:
        async with self._startup_lock:
            if self._ready:
                return

            self.downloader = self._init_downloader_()
            self.__register_rate_limits()
            self.__register_json_checks()
            try:
                await self._async_post_init_()
            finally:
                self._ready = True

    @classmethod
    def _json_resp_check_(cls, json: Any, resp: AbstractResponse, /) -> None:
        """Custom check for JSON responses.

        This method is called automatically by the `HttpClient` when a JSON response is received from `cls.DOMAIN`
        and it was **NOT** successful (`4xx` or `5xx` HTTP code).

        Override this method in subclasses to raise a custom `ScrapeError` instead of the default HTTP error

        Example:
            ```python
            if isinstance(json, dict) and json.get("status") == "error":
                raise ScrapeError(422, f"API error: {json['message']}")
            ```

        IMPORTANT:
            Cases were the response **IS** successful (200, OK) but the JSON indicates an error
            should be handled by the crawler itself
        """
        raise NotImplementedError

    @staticmethod
    def _create_db_path_(url: AbsoluteHttpURL) -> str:
        return url.path

    def __init_subclass__(
        cls, is_abc: bool = False, is_generic: bool = False, generic_name: str = "", **kwargs
    ) -> None:
        super().__init_subclass__(**kwargs)

        msg = (
            f"Subclass {cls.__name__} must not override __init__/_async_init_ method,"
            "use __post_init__ for additional setup"
            "use _async_post_init_ for setup that requires manipulating cookies or any IO (database access, https requests, etc...)"
        )
        assert cls.__init__ is Crawler.__init__, msg
        assert cls._async_init_ is Crawler._async_init_, msg
        cls.NAME: str = cls.__name__.removesuffix("Crawler")
        cls.IS_GENERIC: bool = is_generic
        cls.IS_REAL_DEBRID: bool = cls.NAME == "RealDebrid"
        cls.SUPPORTED_PATHS = _sort_supported_paths(cls.SUPPORTED_PATHS)  # pyright: ignore[reportConstantRedefinition]
        cls.IS_ABC: bool = is_abc

        if cls.IS_GENERIC:
            cls.GENERIC_NAME: str = generic_name or cls.NAME
            cls.SCRAPE_MAPPER_KEYS = ()
            cls.INFO: CrawlerInfo = CrawlerInfo.generic(cls.GENERIC_NAME, cls.SUPPORTED_PATHS)
            Registry.generic.add(cls)
            return

        if is_abc:
            Registry.abc.add(cls)
            return

        if not cls.IS_REAL_DEBRID:
            Crawler._assert_fields_overrides(cls, "PRIMARY_URL", "DOMAIN", "SUPPORTED_PATHS")

        cls.REPLACE_OLD_DOMAINS_REGEX: str | None = "|".join(cls.OLD_DOMAINS) if cls.OLD_DOMAINS else None
        if cls.OLD_DOMAINS:
            if not cls.SUPPORTED_DOMAINS:
                cls.SUPPORTED_DOMAINS = ()  # pyright: ignore[reportConstantRedefinition]
            elif isinstance(cls.SUPPORTED_DOMAINS, str):
                cls.SUPPORTED_DOMAINS = (cls.SUPPORTED_DOMAINS,)  # pyright: ignore[reportConstantRedefinition]
            cls.SUPPORTED_DOMAINS = tuple(sorted({*cls.OLD_DOMAINS, *cls.SUPPORTED_DOMAINS, cls.PRIMARY_URL.host}))  # pyright: ignore[reportConstantRedefinition]

        _validate_supported_paths(cls)
        cls.SCRAPE_MAPPER_KEYS: tuple[str, ...] = _make_scrape_mapper_keys(cls)  # pyright: ignore[reportConstantRedefinition]
        cls.FOLDER_DOMAIN = cls.FOLDER_DOMAIN or cls.DOMAIN.capitalize()  # pyright: ignore[reportConstantRedefinition]
        cls.INFO = CrawlerInfo(  # pyright: ignore[reportConstantRedefinition]
            site=cls.FOLDER_DOMAIN,
            primary_url=cls.PRIMARY_URL,
            supported_domains=_make_wiki_supported_domains(cls.SCRAPE_MAPPER_KEYS),
            supported_paths=cls.SUPPORTED_PATHS,
        )
        Registry.concrete.add(cls)

    def __register_rate_limits(self) -> None:
        self.client.rate_limiter[self.DOMAIN] = AsyncLimiter(*self._RATE_LIMIT)
        if self._DOWNLOAD_SLOTS:
            self.client.download_limiter[self.DOMAIN] = self._DOWNLOAD_SLOTS
        if self._USE_DOWNLOAD_SERVERS_LOCKS:
            self.client.download_limiter.register_server_lock(self.DOMAIN)

    @final
    async def ready(self) -> bool:
        if not self._ready:
            await self._async_init_()
        return self._ready

    @property
    def waiting_items(self) -> int:
        if self._semaphore._waiters is None:
            return 0
        return len(self._semaphore._waiters)

    @copy_signature(HTTPClient._request)
    @contextlib.asynccontextmanager
    async def request(
        self, *args, impersonate: BrowserTypeLiteral | bool | None = None, **kwargs
    ) -> AsyncGenerator[AbstractResponse]:
        if impersonate is None:
            impersonate = self._IMPERSONATE

        async with (
            self.client._request(*args, impersonate=impersonate, **kwargs) as resp,
        ):
            yield resp

    @copy_signature(request)
    async def request_json(self, *args, **kwargs) -> Any:
        async with self.request(*args, **kwargs) as resp:
            return await resp.json()

    @copy_signature(request)
    async def request_soup(self, *args, **kwargs) -> BeautifulSoup:
        async with self.request(*args, **kwargs) as resp:
            return await resp.soup()

    @copy_signature(request)
    async def request_text(self, *args, **kwargs) -> str:
        async with self.request(*args, **kwargs) as resp:
            return await resp.text()

    @final
    def create_task(self, coro: Coroutine[Any, Any, _T_co]) -> asyncio.Task[_T_co]:
        return self.manager.task_group.create_task(coro)

    @final
    def __register_json_checks(self) -> None:
        if self._json_resp_check_.__func__ is Crawler._json_resp_check_.__func__:
            return

        for host in (self.DOMAIN, self.PRIMARY_URL.host):
            self.client.json_resp_checkers[host] = self._json_resp_check_

    def _download_headers_(self, referer: AbsoluteHttpURL) -> dict[str, str]:
        return {
            "User-Agent": self.config.general.user_agent,
            "Referer": str(referer),
        }

    def _init_downloader_(self) -> Downloader:
        self.downloader = dl = Downloader(self.manager, self.DOMAIN)
        return dl

    @final
    @contextlib.contextmanager
    def disable_on_error(self, msg: str) -> Generator[None]:
        try:
            yield
        except Exception:
            self.logger.info(f"[{self.FOLDER_DOMAIN}] {msg}. Crawler has been disabled")
            self.disabled = True
            raise

    @final
    async def run(self, scrape_item: ScrapeItem) -> None:
        """Runs the crawler loop."""
        if self.disabled:
            return

        async with self._semaphore:
            with scrape_item.track_changes():
                scrape_item.url = url = self.transform_url(scrape_item.url)

            if url.path_qs in self._scraped_items:
                self.logger.info(f"Skipping {url} as it has already been scraped")
                return

            self._scraped_items.add(url.path_qs)
            with self.new_task_id(scrape_item.url):
                try:
                    if not self.ALLOW_EMPTY_PATH and scrape_item.url.path == "/":
                        raise ValueError
                    await self.fetch(scrape_item)
                except ValueError:
                    self.handle_error(scrape_item, ScrapeError("Unknown URL path"))
                except MaxChildrenError as e:
                    self.handle_error(scrape_item, e)

    @classmethod
    def transform_url(cls, url: AbsoluteHttpURL) -> AbsoluteHttpURL:
        """Transforms an URL before it reaches the fetch method

        Override it to transform thumbnail URLs into full res URLs or URLs in an old unsupported format into a new one"""
        if cls.REPLACE_OLD_DOMAINS_REGEX:
            new_host = re.sub(cls.REPLACE_OLD_DOMAINS_REGEX, cls.PRIMARY_URL.host, url.host)
            return url.with_host(new_host)
        return url

    @final
    @error_handling_wrapper
    def handle_error(self, scrape_item: ScrapeItem, exc: type[Exception] | Exception | str) -> None:
        if isinstance(exc, str):
            exc = ScrapeError(exc)
        raise exc

    @final
    def new_task_id(self, url: AbsoluteHttpURL) -> ProgressHook:
        """Creates a new task_id (shows the URL in the UI and logs)"""
        self.logger.info(f"Scraping [{self.FOLDER_DOMAIN}]: {url}")
        return self.manager.tui.scrape.new_hook(url)

    @staticmethod
    def is_subdomain(url: AbsoluteHttpURL) -> bool:
        return url.host.removeprefix("www.").count(".") > 1

    @final
    async def write_metadata(self, scrape_item: ScrapeItem, name: str, metadata: object) -> None:
        """Write general metadata (not specific to a single file) to json output"""

        filename = f"{name}.metadata"  # we won't write to fs, so we skip name sanitization
        download_folder = get_download_path(self.manager, scrape_item, self.FOLDER_DOMAIN)
        url = scrape_item.url.with_scheme("metadata")
        media_item = MediaItem.from_item(
            scrape_item,
            url,  # pyright: ignore[reportArgumentType]
            self.DOMAIN,
            db_path="",
            download_folder=download_folder,
            filename=filename,
        )
        media_item.metadata = metadata
        await self.__write_to_jsonl(media_item)

    async def handle_file(
        self,
        url: AbsoluteHttpURL,
        scrape_item: ScrapeItem,
        filename: str,
        ext: str | None = None,
        *,
        custom_filename: str | None = None,
        debrid_link: AbsoluteHttpURL | None = None,
        m3u8: m3u8.RenditionGroup | None = None,
        metadata: object = None,
    ) -> None:
        """Finishes handling the file and hands it off to the downloader."""
        ext = ext or Path(filename).suffix
        if custom_filename:
            original_filename, filename = filename, custom_filename
        else:
            original_filename = filename

        download_folder = get_download_path(self.manager, scrape_item, self.FOLDER_DOMAIN)
        media_item = MediaItem.from_item(
            scrape_item,
            url,
            self.DOMAIN,
            filename=filename,
            download_folder=download_folder,
            db_path=self._create_db_path_(url),
            original_filename=original_filename,
            ext=ext,
        )
        media_item.debrid_url = debrid_link
        media_item.headers = self._download_headers_(media_item.referer)
        if metadata is not None:
            media_item.metadata = metadata
        await self.handle_media_item(media_item, m3u8)

    @final
    async def _download(self, media_item: MediaItem, m3u8: m3u8.RenditionGroup | None) -> None:
        try:
            if m3u8:
                await self.downloader.download_hls(media_item, m3u8)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
            else:
                await self.downloader.run(media_item)

        finally:
            await self.__write_to_jsonl(media_item)

    async def __write_to_jsonl(self, media_item: MediaItem) -> None:
        if not self.config.files.dump_json:
            return

        data = [media_item.as_dict()]
        await self.manager.logs.write_jsonl(data)

    async def check_complete(self, url: AbsoluteHttpURL, referer: AbsoluteHttpURL) -> bool:
        """Checks if this URL has been download before.

        This method is called automatically on a created media item,
        but Crawler code can use it to skip unnecessary requests"""
        db_path = self._create_db_path_(url)
        check_complete = await self.manager.database.history_table.check_complete(self.DOMAIN, url, referer, db_path)
        if check_complete:
            self.logger.info(f"Skipping {url} as it has already been downloaded")
            self.manager.tui.files.add_prev_completed()
        return check_complete

    async def handle_media_item(self, media_item: MediaItem, m3u8: m3u8.RenditionGroup | None = None) -> None:
        if media_item.timestamp and not isinstance(media_item.timestamp, int):
            msg = f"Invalid datetime from '{self.FOLDER_DOMAIN}' crawler . Got {media_item.timestamp!r}, expected int."
            self.logger.info(msg)

        check_complete = await self.check_complete(media_item.url, media_item.referer)
        if check_complete:
            if media_item.album_id:
                await self.manager.database.history_table.set_album_id(self.DOMAIN, media_item)
            return

        if await self.check_skip_by_config(media_item):
            self.manager.tui.files.add_skipped()
            return

        self.create_task(self._download(media_item, m3u8))

    @final
    async def check_skip_by_config(self, media_item: MediaItem) -> bool:
        media_host = media_item.url.host

        if (hosts := self.config.ignore.skip_hosts) and any(host in media_host for host in hosts):
            self.logger.info(f"Download skipped{media_item.url} due to skip_hosts config")
            return True

        if (hosts := self.config.ignore.only_hosts) and not any(host in media_host for host in hosts):
            self.logger.info(f"Download skipped{media_item.url} due to only_hosts config")
            return True

        if (regex := self.config.ignore.filename_regex_filter) and re.search(regex, media_item.filename):
            self.logger.info(f"Download skipped{media_item.url} due to filename regex filter config")
            return True

        return False

    @final
    async def check_complete_from_referer(
        self: Crawler, item: ScrapeItem | AbsoluteHttpURL, any_crawler: bool = False
    ) -> bool:
        """Checks if the scrape item has already been scraped.

        if `any_crawler` is `True`, checks database entries for all crawlers and returns `True` if at least 1 of them has marked it as completed
        """
        url = item if isinstance(item, AbsoluteHttpURL) else item.url
        domain = None if any_crawler else self.DOMAIN
        downloaded = await self.manager.database.history_table.check_complete_by_referer(domain, url)
        if downloaded:
            self.logger.info(f"Skipping {url} as it has already been downloaded")
            self.manager.tui.files.add_prev_completed()
            return True
        return False

    @final
    async def check_complete_by_hash(
        self: Crawler, scrape_item: ScrapeItem | AbsoluteHttpURL, hash_type: Literal["md5", "sha256"], hash_value: str
    ) -> bool:
        """Returns `True` if at least 1 file with this hash is recorded on the database"""
        downloaded = await self.manager.database.hash_table.check_hash_exists(hash_type, hash_value)
        if downloaded:
            url = scrape_item if isinstance(scrape_item, AbsoluteHttpURL) else scrape_item.url
            self.logger.info(f"Skipping {url} as its hash ({hash_type}:{hash_value}) has already been downloaded")
            self.manager.tui.files.add_prev_completed()
        return downloaded

    async def get_album_results(self, album_id: str) -> dict[str, int]:
        """Checks whether an album has completed given its domain and album id."""
        return await self.manager.database.history_table.check_album(self.DOMAIN, album_id)

    def handle_external_links(self, scrape_item: ScrapeItem, reset: bool = True) -> None:
        """Maps external links to the scraper class."""
        if reset:
            scrape_item.reset()
        self.create_task(self.manager.scrape_mapper.filter_and_send_to_crawler(scrape_item))

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_filename_and_ext(
        self, filename: str, assume_ext: str | None = ".mp4", *, mime_type: str | None = None
    ) -> tuple[str, str]:
        """Wrapper around `utils.get_filename_and_ext`.
        Calls it as is.
        If that fails, appends `assume_ext` and tries again, but only if the user had exclude_files_with_no_extension = `False`
        """
        try:
            return get_filename_and_ext(filename, mime_type)
        except NoExtensionError:
            if self.config.ignore.exclude_files_with_no_extension and assume_ext:
                return get_filename_and_ext(filename + assume_ext, mime_type)
            raise

    def check_album_results(self, url: AbsoluteHttpURL, album_results: dict[str, Any]) -> bool:
        """Checks whether an album has completed given its domain and album id."""
        if not album_results:
            return False
        url_path = self._create_db_path_(url)
        if url_path in album_results and album_results[url_path] != 0:
            self.logger.info(f"Skipping {url} as it has already been downloaded")
            self.manager.tui.files.add_prev_completed()
            return True
        return False

    def create_title(self, title: str, album_id: str | None = None, thread_id: int | None = None) -> str:
        """Creates the title for the scrape item."""
        if not title:
            title = "Untitled"

        title = title.strip()
        if album_id and self.config.download.include_album_id_in_folder_name:
            title = f"{title} {album_id}"

        if thread_id and self.config.download.include_thread_id_in_folder_name:
            title = f"{title} {thread_id}"

        if not self.config.download.remove_domains_from_folder_names:
            title = f"{title} ({self.FOLDER_DOMAIN})"

        # Remove double spaces
        while True:
            title = title.replace("  ", " ")
            if "  " not in title:
                break
        return title

    @property
    def separate_posts(self) -> bool:
        return self.config.download.separate_posts

    def create_separate_post_title(
        self,
        title: str | None = None,
        id: str | None = None,
        date: datetime.datetime | datetime.date | int | None = None,
        /,
    ) -> str:
        if not self.separate_posts:
            return ""
        title_format = self.config.download.separate_posts_format
        if title_format.strip().casefold() == "{default}":
            title_format = self.DEFAULT_POST_TITLE_FORMAT
        if isinstance(date, int):
            date = datetime.datetime.fromtimestamp(date)

        post_title, _ = safe_format(title_format, id=id, number=id, date=date, title=title)
        return post_title

    @classmethod
    def parse_url(
        cls, link_str: str, relative_to: AbsoluteHttpURL | None = None, *, trim: bool | None = None
    ) -> AbsoluteHttpURL:
        """Wrapper around `utils.parse_url` to use `self.PRIMARY_URL` as base"""
        base = relative_to or cls.PRIMARY_URL
        assert is_absolute_http_url(base)
        if trim is None:
            trim = cls.DEFAULT_TRIM_URLS
        return parse_url(link_str, base, trim=trim)

    def update_cookies(self, cookies: dict[str, Any], url: AbsoluteHttpURL | None = None) -> None:
        """Update cookies for the provided URL

        If `url` is `None`, defaults to `self.PRIMARY_URL`
        """
        response_url = url or self.PRIMARY_URL
        self.client.cookies.update_cookies(cookies, response_url)

    def iter_tags(
        self,
        soup: Tag,
        selector: str,
        /,
        attribute: str = "href",
        *,
        results: dict[str, int] | None = None,
    ) -> Generator[tuple[AbsoluteHttpURL | None, AbsoluteHttpURL]]:
        """Generates tuples with an URL from the `src` value of the first image tag (AKA the thumbnail) and an URL from the value of `attribute`

        :param results: must be the output of `self.get_album_results`.
        If provided, it will be used as a filter, to only yield items that has not been downloaded before"""
        album_results = results or {}

        seen: set[str] = set()
        for tag in css.iselect(soup, selector):
            link_str: str | None = css._get_attr(tag, attribute)
            if not link_str or link_str in seen:
                continue
            seen.add(link_str)
            link = self.parse_url(link_str)
            if self.check_album_results(link, album_results):
                continue
            if t_tag := tag.select_one("img"):
                thumb_str: str | None = css._get_attr(t_tag, "src")
            else:
                thumb_str = None
            thumb = self.parse_url(thumb_str) if thumb_str and not is_blob_or_svg(thumb_str) else None
            yield thumb, link

    def iter_children(
        self,
        scrape_item: ScrapeItem,
        soup: BeautifulSoup,
        selector: str,
        /,
        attribute: str = "href",
        *,
        results: dict[str, int] | None = None,
        **kwargs: Any,
    ) -> Generator[tuple[AbsoluteHttpURL | None, ScrapeItem]]:
        """Generates tuples with an URL from the `src` value of the first image tag (AKA the thumbnail) and a new scrape item from the value of `attribute`

        :param results: must be the output of `self.get_album_results`.
        If provided, it will be used as a filter, to only yield items that has not been downloaded before
        :param **kwargs: Will be forwarded to `scrape.item.create_child`"""
        for thumb, link in self.iter_tags(soup, selector, attribute, results=results):
            new_scrape_item = scrape_item.create_child(link, **kwargs)
            yield thumb, new_scrape_item
            scrape_item.add_children()

    @final
    async def crawl_children(
        self,
        scrape_item: ScrapeItem,
        selector: str,
        /,
        attribute: str = "href",
        *,
        results: dict[str, int] | None = None,
        next_page_selector: str | None = None,
        coro_factory: Callable[[ScrapeItem], Coroutine[Any, Any, Any]] | None = None,
    ) -> None:
        """Crawls children URLs and schedules scrape tasks for them.

        Uses `self.web_pager` to iterate through pages and extracts child links based on `selector` and `attribute`. For
        each extracted URL, a new `ScrapeItem` is created, and a task
        (by default, `self.run`) is scheduled in the global task group to process it
        """

        if coro_factory is None:
            coro_factory = self.run

        async for soup in self.web_pager(scrape_item.url, next_page_selector):
            for _, new_scrape_item in self.iter_children(scrape_item, soup, selector, attribute, results=results):
                self.create_task(coro_factory(new_scrape_item))

    async def web_pager(
        self, url: AbsoluteHttpURL, next_page_selector: str | None = None, *, cffi: bool = False, **kwargs: Any
    ) -> AsyncGenerator[BeautifulSoup]:
        """Generator of website pages.

        :param next_page_selector: If `None`, `self.next_page_selector` will be used
        :param cffi: If `True`, uses `curl_cffi` to get the soup for each page. Otherwise, `aiohttp` will be used
        :param **kwargs: Will be forwarded to `self.parse_url` to parse each new page"""

        async for soup in self._web_pager(url, next_page_selector, cffi=cffi, **kwargs):
            yield soup

    async def _web_pager(
        self,
        url: AbsoluteHttpURL,
        selector: Callable[[BeautifulSoup], str | None] | str | None = None,
        *,
        cffi: bool = False,
        **kwargs: Any,
    ) -> AsyncGenerator[BeautifulSoup]:
        """Generator of website pages.

        :param next_page_selector: If `None`, `self.next_page_selector` will be used
        :param cffi: If `True`, uses `curl_cffi` to get the soup for each page. Otherwise, `aiohttp` will be used
        :param **kwargs: Will be forwarded to `self.parse_url` to parse each new page"""

        kwargs.setdefault("relative_to", url.origin())
        page_url = url
        if callable(selector):
            get_next_page = selector
        else:
            selector = selector or self.NEXT_PAGE_SELECTOR
            assert selector, f"No selector was provided and {self.DOMAIN} does define a next_page_selector"
            func = css.select_one_get_attr
            get_next_page = partial(func, selector=selector, attribute="href")

        while True:
            soup = await self.request_soup(page_url, impersonate=cffi or None)
            yield soup
            page_url_str = get_next_page(soup)
            if not page_url_str:
                break
            page_url = self.parse_url(page_url_str, **kwargs)

    @error_handling_wrapper
    async def direct_file(
        self, scrape_item: ScrapeItem, url: AbsoluteHttpURL | None = None, assume_ext: str | None = None
    ) -> None:
        """Download a direct link file. Filename will be the url slug"""
        link = url or scrape_item.url
        filename, ext = self.get_filename_and_ext(link.name or link.parent.name, assume_ext=assume_ext)
        await self.handle_file(link, scrape_item, filename, ext)

    @final
    @contextlib.asynccontextmanager
    async def new_task_group(self, scrape_item: ScrapeItem) -> AsyncGenerator[asyncio.TaskGroup]:
        async with asyncio.TaskGroup() as tg:
            with error_handling_context(self, scrape_item):
                yield tg

    @final
    def parse_date(self, date_or_datetime: str, format: str, /) -> dates.TimeStamp | None:
        try:
            parsed_date = dates.parse_format(date_or_datetime, format)
        except ValueError as e:
            self.logger.error(
                f"Date parsing for {self.DOMAIN} seems to be broken [{date_or_datetime=}, {format=}]: {e}"
            )
        else:
            return dates.to_timestamp(parsed_date)

    @final
    def parse_iso_date(self, date_or_datetime: str, /) -> dates.TimeStamp | None:
        try:
            parsed_date = dates.parse_iso(date_or_datetime)
        except ValueError as e:
            self.logger.error(f"Date parsing for {self.DOMAIN} seems to be broken [{date_or_datetime=} iso=True]: {e}")
        else:
            return dates.to_timestamp(parsed_date)

    async def _get_redirect_url(self, url: AbsoluteHttpURL) -> AbsoluteHttpURL:
        async with self.request(url) as resp:
            return resp.url

    @final
    @error_handling_wrapper
    async def follow_redirect(self, scrape_item: ScrapeItem) -> None:
        redirect = await self._get_redirect_url(scrape_item.url)
        if scrape_item.url == redirect:
            raise ScrapeError(422, "Infinite redirect")
        scrape_item.url = redirect
        _ = self.create_task(self.run(scrape_item))

    async def get_m3u8_from_playlist_url(
        self,
        m3u8_playlist_url: AbsoluteHttpURL,
        /,
        headers: dict[str, str] | None = None,
        *,
        only: Iterable[str] = (),
        exclude: Iterable[str] = ("vp09",),
    ) -> tuple[m3u8.RenditionGroup, m3u8.RenditionGroupDetails]:
        """Get m3u8 rendition group from a playlist m3u8 (variant m3u8), selecting the best format"""
        m3u8_playlist = await self._get_m3u8(m3u8_playlist_url, headers)
        rendition_group_info = m3u8.get_best_group_from_playlist(m3u8_playlist, only=only, exclude=exclude)
        renditions_urls = rendition_group_info.urls
        video = await self._get_m3u8(renditions_urls.video, headers, "video")
        audio = await self._get_m3u8(renditions_urls.audio, headers, "audio") if renditions_urls.audio else None
        subtitle = (
            await self._get_m3u8(renditions_urls.subtitle, headers, "subtitles") if renditions_urls.subtitle else None
        )
        return m3u8.RenditionGroup(video, audio, subtitle), rendition_group_info

    async def get_m3u8_from_index_url(
        self, url: AbsoluteHttpURL, /, headers: dict[str, str] | None = None
    ) -> m3u8.RenditionGroup:
        """Get m3u8 rendition group from an index that only has 1 rendition, a video (non variant m3u8)"""
        return m3u8.RenditionGroup(await self._get_m3u8(url, headers, "video"))

    async def _get_m3u8(
        self,
        url: AbsoluteHttpURL,
        /,
        headers: dict[str, str] | None = None,
        media_type: Literal["video", "audio", "subtitles"] | None = None,
    ) -> m3u8.M3U8:
        content = await self.request_text(url, headers=headers)
        return m3u8.M3U8(content, url.parent, media_type)

    def create_custom_filename(
        self,
        name: str,
        ext: str,
        /,
        *,
        file_id: str | None = None,
        video_codec: str | None = None,
        audio_codec: str | None = None,
        resolution: Resolution | str | int | None = None,
        hash_string: str | None = None,
        only_truncate_stem: bool = True,
    ) -> str:
        calling_args = {name: value for name, value in locals().items() if value is not None and name not in ("self",)}
        # remove OS separators (if any)
        stem = sanitize_filename(Path(name).as_posix().replace("/", "-")).strip().removesuffix(ext).strip()
        if resolution and not isinstance(resolution, Resolution):
            resolution = Resolution.parse(resolution)

        def extra_info():
            if _placeholder_config.include_file_id and file_id:
                yield file_id
            if _placeholder_config.include_video_codec and video_codec:
                yield video_codec
            if _placeholder_config.include_audio_codec and audio_codec:
                yield audio_codec

            if (
                _placeholder_config.include_resolution
                and resolution
                and resolution not in [Resolution.highest(), Resolution.unknown()]
            ):
                yield resolution.name

            if _placeholder_config.include_hash and hash_string:
                assert any(hash_string.startswith(x) for x in _HASH_PREFIXES), f"Invalid: {hash_string = }"
                yield hash_string

        filename, extra_info_had_invalid_chars = compose_custom_filename(
            stem,
            ext,
            self.config.general.max_file_name_length,
            *extra_info(),
            only_truncate_stem=only_truncate_stem,
        )
        if extra_info_had_invalid_chars:
            msg = (
                f"Custom filename creation for {self.FOLDER_DOMAIN} seems to be broken. "
                f"Important information was removed while creating a filename. "
                f"\n{calling_args}"
            )
            self.logger.warning(msg)
        return filename

    @final
    def get_cookies(self, partial_match_domain: bool = False) -> Iterable[tuple[str, BaseCookie[str]]]:
        if partial_match_domain:
            yield from self.client.filter_cookies_by_word_in_domain(self.DOMAIN)
        else:
            yield str(self.PRIMARY_URL.host), self.client.cookies.filter_cookies(self.PRIMARY_URL)

    @final
    def get_cookie_value(self, cookie_name: str, partial_match_domain: bool = False) -> str | None:
        def get_morsels_by_name():
            for _, cookie in self.get_cookies(partial_match_domain):
                if morsel := cookie.get(cookie_name):
                    yield morsel

        if newest := max(get_morsels_by_name(), key=lambda x: int(x["max-age"] or 0), default=None):
            return newest.value

    @final
    def handle_subs(self, scrape_item: ScrapeItem, video_filename: str, subtitles: Iterable[ISO639Subtitle]) -> None:
        counter = Counter()
        video_stem = Path(video_filename).stem
        for sub in subtitles:
            link = self.parse_url(sub.url) if isinstance(sub.url, str) else sub.url
            counter[sub.lang_code] += 1
            if (count := counter[sub.lang_code]) > 1:
                suffix = f"{sub.lang_code}.{count}{link.suffix}"
            else:
                suffix = f"{sub.lang_code}{link.suffix}"

            sub_name, ext = self.get_filename_and_ext(f"{video_stem}.{suffix}")
            new_scrape_item = scrape_item.create_new(scrape_item.url.with_fragment(sub_name))
            self.create_task(
                self.handle_file(
                    link,
                    new_scrape_item,
                    sub.name or link.name,
                    ext,
                    custom_filename=sub_name,
                )
            )

    catch_errors = error_handling_context


def _make_scrape_mapper_keys(cls: type[Crawler] | Crawler) -> tuple[str, ...]:
    if cls.SUPPORTED_DOMAINS:
        hosts = cls.SUPPORTED_DOMAINS
    else:
        hosts = cls.DOMAIN
    if isinstance(hosts, str):
        hosts = (hosts,)
    return tuple(sorted({host.removeprefix("www.") for host in hosts}))


@dataclasses.dataclass(slots=True)
class Site:
    PRIMARY_URL: AbsoluteHttpURL
    DOMAIN: str
    SUPPORTED_DOMAINS: SupportedDomains = ()
    FOLDER_DOMAIN: str = ""


_CrawlerT = TypeVar("_CrawlerT", bound=Crawler)


def create_crawlers(
    urls: Iterable[str] | Iterable[AbsoluteHttpURL], base_crawler: type[_CrawlerT]
) -> set[type[_CrawlerT]]:
    """Creates new subclasses of the base crawler from the urls"""
    return {_create_subclass(url, base_crawler) for url in urls}


def _create_subclass(url: AbsoluteHttpURL | str, base_class: type[_CrawlerT]) -> type[_CrawlerT]:
    if isinstance(url, str):
        url = AbsoluteHttpURL(url)
    assert is_absolute_http_url(url)
    primary_url = remove_trailing_slash(url)
    domain = primary_url.host.removeprefix("www.")
    class_name = _make_crawler_name(domain)
    class_attributes = dataclasses.asdict(Site(primary_url, domain))
    return type(class_name, (base_class,), class_attributes)  # type: ignore  # pyright: ignore[reportReturnType]


def _make_crawler_name(input_string: str) -> str:
    clean_string = re.sub(r"[^a-zA-Z0-9]+", " ", input_string).strip()
    cap_name = clean_string.title().replace(" ", "")
    assert cap_name and cap_name.isalnum(), (
        f"Can not generate a valid class name from {input_string}. Needs to be defined as a concrete class"
    )
    if cap_name[0].isdigit():
        cap_name = "_" + cap_name
    return f"{cap_name}Crawler"


def _validate_supported_paths(cls: type[Crawler]) -> None:
    for path_name, paths in cls.SUPPORTED_PATHS.items():
        assert path_name, f"{cls.__name__}, Invalid path: {path_name}"
        assert isinstance(paths, str | tuple), f"{cls.__name__}, Invalid path {path_name}: {type(paths)}"
        if path_name != "Direct links":
            assert paths, f"{cls.__name__} has not paths for {path_name}"

        if path_name.startswith("*"):  # note
            return
        if isinstance(paths, str):
            paths = (paths,)
        for path in paths:
            assert "`" not in path, f"{cls.__name__}, Invalid path {path_name}: {path}"


def _make_wiki_supported_domains(scrape_mapper_keys: tuple[str, ...]) -> tuple[str, ...]:
    def generalize(domain):
        if "." not in domain:
            return f"{domain}.*"
        return domain

    return tuple(sorted(generalize(domain) for domain in scrape_mapper_keys))


def _sort_supported_paths(supported_paths: SupportedPaths) -> dict[str, OneOrTuple[str]]:
    def try_sort(value: OneOrTuple[str]) -> OneOrTuple[str]:
        if isinstance(value, tuple):
            return tuple(sorted(value))
        return value

    path_pairs = ((key, try_sort(value)) for key, value in supported_paths.items())
    return dict(sorted(path_pairs, key=lambda x: x[0].casefold()))


def auto_task_id(
    func: Callable[Concatenate[_CrawlerT, ScrapeItem, _P], Coroutine[None, None, _R]],
) -> Callable[Concatenate[_CrawlerT, ScrapeItem, _P], Coroutine[None, None, _R]]:
    """Autocreate a new `task_id` from the scrape_item of the method"""

    @wraps(func)
    async def wrapper(self: _CrawlerT, scrape_item: ScrapeItem, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        with self.new_task_id(scrape_item.url):
            result = func(self, scrape_item, *args, **kwargs)
            return await result

    return wrapper
