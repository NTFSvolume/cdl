from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import ssl
import time
from base64 import b64encode
from collections import defaultdict
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal, Self

import aiohttp
import certifi
import truststore
from aiohttp import ClientResponse, ClientSession
from aiolimiter import AsyncLimiter
from curl_cffi.requests.session import AsyncSession

from cyberdrop_dl import appdata, config, constants, ddos_guard, env
from cyberdrop_dl.clients.flaresolverr import FlareSolverr
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.cookies import get_cookies_from_browser, make_simple_cookie, read_netscape_files
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import DDOSGuardError, DownloadError, ScrapeError
from cyberdrop_dl.logger import spacer
from cyberdrop_dl.utils import aio, ffmpeg

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
    from http.cookies import BaseCookie

    from aiohttp.resolver import AsyncResolver, ThreadedResolver
    from curl_cffi.requests import AsyncSession
    from curl_cffi.requests.impersonate import BrowserTypeLiteral
    from curl_cffi.requests.models import Response
    from curl_cffi.requests.models import Response as CurlResponse


_curl_import_error = None
try:
    from curl_cffi.requests import AsyncSession  # noqa: TC002
except ImportError as e:
    _curl_import_error = e

_DOWNLOAD_ERROR_ETAGS = {
    "d835884373f4d6c8f24742ceabe74946": "Imgur image has been removed",
    "65b7753c-528a": "SC Scrape Image",
    "5c4fb843-ece": "PixHost Removed Image",
    "637be5da-11d2b": "eFukt Video removed",
    "63a05f27-11d2b": "eFukt Video removed",
    "5a56b09d-1485eb": "eFukt Video removed",
}

_crawler_errors: dict[str, int] = defaultdict(int)
Domain = str
_HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"]
_MAX_REDIRECTS = 8
_NULL_CONTEXT: contextlib.nullcontext[None] = contextlib.nullcontext()
_dns_resolver: type[AsyncResolver] | type[ThreadedResolver] | None = None
logger = logging.getLogger(__name__)


def _create_ssl(config: config.Config) -> ssl.SSLContext | Literal[False]:
    ssl_context = config.general.ssl_context
    if not ssl_context:
        return False

    if ssl_context == "certifi":
        return ssl.create_default_context(cafile=certifi.where())
    if ssl_context == "truststore":
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=certifi.where())
    return ctx


@dataclasses.dataclass(slots=True)
class DownloadLimiter:
    """Class to limit the number of concurrent downloads"""

    config_per_domain_max_slots: int
    config_global_max_slots: int

    _server_locked_domains: set[Domain] = dataclasses.field(init=False, default_factory=set)
    _hardcoded_per_domain_slots: dict[Domain, int] = dataclasses.field(init=False, default_factory=dict)
    _server_locks: aio.WeakAsyncLocks[Domain] = dataclasses.field(init=False, default_factory=aio.WeakAsyncLocks)
    _per_domain_slots: dict[Domain, asyncio.Semaphore] = dataclasses.field(init=False, default_factory=dict)
    _global_slots: asyncio.Semaphore = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._global_slots = asyncio.Semaphore(self.config_global_max_slots)

    def __setitem__(self, domain: Domain, limit: int) -> None:
        self._hardcoded_per_domain_slots[domain] = limit

    def _get_limiter(self, domain: Domain) -> asyncio.Semaphore:
        if sem := self._per_domain_slots.get(domain):
            return sem

        limit = self.config_per_domain_max_slots
        if hardcoded_limit := self._hardcoded_per_domain_slots.get(domain):
            limit = min(limit, hardcoded_limit)

        self._per_domain_slots[domain] = sem = asyncio.Semaphore(limit)
        return sem

    def register_server_lock(self, domain: Domain) -> None:
        self._server_locked_domains.add(domain)

    @contextlib.asynccontextmanager
    async def acquire(self, domain: Domain, server: Domain) -> AsyncGenerator[None]:
        server_lock = _NULL_CONTEXT if domain not in self._server_locked_domains else self._server_locks[server]
        async with server_lock, self._get_limiter(domain), self._global_slots:
            yield


@dataclasses.dataclass(slots=True)
class RateLimiter:
    config_global_max_limit: float
    _global: AsyncLimiter = dataclasses.field(init=False)
    _per_domain: dict[Domain, AsyncLimiter] = dataclasses.field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._global = AsyncLimiter(self.config_global_max_limit, time_period=1)

    def __setitem__(self, domain: Domain, limiter: AsyncLimiter) -> None:
        self._per_domain[domain] = limiter

    @contextlib.asynccontextmanager
    async def acquire(self, domain: str) -> AsyncGenerator[None]:
        async with self._per_domain[domain], self._global:
            yield


class HttpClient:
    """
    Wrapper around aiohttp.ClientSession / curl.AsyncSession

    It does:
    - Setup session based on config values (proxies, ssl, etc..)
    - A rate limit on request
    - A concurrent downloads limit
    - A per server locks if required

    """

    def __init__(self, config: config.Config) -> None:
        self.config: config.Config = config
        self.ssl_context: ssl.SSLContext | Literal[False] = _create_ssl(config)
        self._cookies: aiohttp.CookieJar | None = None
        self.download_limiter: DownloadLimiter = DownloadLimiter(
            config.rate_limits.max_simultaneous_downloads_per_domain,
            config.rate_limits.max_simultaneous_downloads,
        )
        self.rate_limiter: RateLimiter = RateLimiter(config.rate_limits.rate_limit)
        self._flaresolverr: FlareSolverr = FlareSolverr(self)
        self._session: aiohttp.ClientSession
        self._json_response_checks: dict[Domain, Callable[[Any], None]] = {}
        self._curl_session: AsyncSession[Response] | None = None

    @contextlib.asynccontextmanager
    async def _request(
        self,
        url: AbsoluteHttpURL,
        /,
        method: _HttpMethod = "GET",
        headers: dict[str, str] | None = None,
        impersonate: BrowserTypeLiteral | bool | None = None,
        data: Any = None,
        json: Any = None,
        **request_params: Any,
    ) -> AsyncGenerator[AbstractResponse]:
        """
        Asynchronous context manager for HTTP requests.

        - If 'impersonate' is specified, uses curl_cffi for the request and updates cookies.
        - Otherwise, uses aiohttp with optional cache control.
        - Yield an AbstractResponse that wraps the underlying response with common methods.
        - On DDOSGuardError, retries the request using FlareSolverr.
        - Saves the HTML content to disk if the config option is enabled.
        - Closes underliying response on exit.
        """

        request_params["headers"] = headers = headers or {}
        request_params["data"] = data
        request_params["json"] = json

        if not impersonate:
            _ = headers.setdefault("user-agent", self.config.general.user_agent)

        async with self.__request_context(url, method, request_params, impersonate) as resp:
            yield await self._check_response(resp, url)

    def __sync_session_cookies(self, url: AbsoluteHttpURL) -> None:
        """
        Apply to the cookies from the `curl` session into the `aiohttp` session, filtering them by the URL

        This is mostly just to get the `cf_cleareance` cookie value into the `aiohttp` session

        The reverse (sync `aiohttp` -> `curl`) is not needed at the moment, so it is skipped
        """
        now = time.time()
        for cookie in self.curl_session.cookies.jar:
            simple_cookie = make_simple_cookie(cookie, now)
            self.cookies.update_cookies(simple_cookie, url)

    @contextlib.asynccontextmanager
    async def __request_context(
        self,
        url: AbsoluteHttpURL,
        method: _HttpMethod,
        request_params: dict[str, Any],
        impersonate: BrowserTypeLiteral | bool | None,
    ) -> AsyncGenerator[AbstractResponse]:
        logger.debug(f"Starting {method} request to {url}")

        if impersonate:
            if impersonate is True:
                impersonate = "chrome"

            request_params["impersonate"] = impersonate
            curl_resp = await self.curl_session.request(method, str(url), stream=True, **request_params)
            try:
                yield AbstractResponse.from_resp(curl_resp)
                self.__sync_session_cookies(url)
            finally:
                msg = f"Finishing {method} request to {url} [{curl_resp.status_code}]"
                logger.debug(msg)
                await curl_resp.aclose()
            return

        _ = request_params.setdefault("max_redirects", _MAX_REDIRECTS)
        async with (
            self._session.request(method, url, **request_params) as aio_resp,
        ):
            msg = f"Finishing {method} request to {url} [{aio_resp.status}]"
            logger.debug(msg)
            yield AbstractResponse.from_resp(aio_resp)

    async def _check_response(self, abs_resp: AbstractResponse, url: AbsoluteHttpURL, data: Any | None = None):
        """Checks the HTTP response status and retries DDOS Guard errors with FlareSolverr.

        Returns an AbstractResponse confirmed to not be a DDOS Guard page."""
        try:
            await self.check_http_status(abs_resp)
            return abs_resp
        except DDOSGuardError:
            flare_solution = await self._flaresolverr.request(url, data)
            return AbstractResponse.from_flaresolverr(flare_solution)

    @property
    def cookies(self) -> aiohttp.CookieJar:
        if self._cookies is None:
            self._cookies = aiohttp.CookieJar(quote_cookie=False)
        return self._cookies

    async def __aenter__(self) -> Self:
        global _dns_resolver
        if _dns_resolver is None:
            _dns_resolver = await _get_dns_resolver()
        self._session = self.create_aiohttp_session()
        return self

    async def __aexit__(self, *_) -> None:
        await self._session.close()
        if self._curl_session is None:
            return
        try:
            await self._curl_session.close()
        except Exception:
            pass

    @property
    def curl_session(self) -> AsyncSession[Response]:
        if self._curl_session is None:
            _check_curl_cffi_is_available()
            self._curl_session = self._create_curl_session()
        return self._curl_session

    @staticmethod
    def basic_auth(username: str, password: str) -> str:
        """Returns a basic auth token."""
        token = b64encode(f"{username}:{password}".encode()).decode("ascii")
        return f"Basic {token}"

    def check_allowed_filetype(self, media_item: MediaItem) -> bool:
        """Checks if the file type is allowed to download."""
        ignore_options = self.config.ignore
        ext = media_item.ext.lower()

        if ignore_options.exclude_images and ext in constants.FileFormats.IMAGE:
            return False
        if ignore_options.exclude_videos and ext in constants.FileFormats.VIDEO:
            return False
        if ignore_options.exclude_audio and ext in constants.FileFormats.AUDIO:
            return False

        return ext in constants.FileFormats.MEDIA or not ignore_options.exclude_other

    def check_allowed_date_range(self, media_item: MediaItem) -> bool:
        """Checks if the file was uploaded within the config date range"""
        datetime = media_item.datetime_obj()
        if not datetime:
            return True

        item_date = datetime.date()
        ignore_options = self.config.ignore

        if ignore_options.exclude_before and item_date < ignore_options.exclude_before:
            return False
        if ignore_options.exclude_after and item_date > ignore_options.exclude_after:
            return False
        return True

    def filter_cookies_by_word_in_domain(self, word: str) -> Iterable[tuple[str, BaseCookie[str]]]:
        """Yields pairs of `[domain, BaseCookie]` for every cookie with a domain that has `word` in it"""
        if not self.cookies:
            return
        self.cookies._do_expiration()
        for domain, _ in self.cookies._cookies:
            if word in domain:
                yield domain, self.cookies.filter_cookies(AbsoluteHttpURL(f"https://{domain}"))

    def _create_curl_session(self) -> AsyncSession[CurlResponse]:
        import warnings

        from curl_cffi.aio import AsyncCurl
        from curl_cffi.requests import AsyncSession
        from curl_cffi.utils import CurlCffiWarning

        loop = asyncio.get_running_loop()

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=CurlCffiWarning)
            acurl = AsyncCurl(loop=loop)

        proxy_or_none = str(proxy) if (proxy := self.config.general.proxy) else None

        return AsyncSession(
            loop=loop,
            async_curl=acurl,
            impersonate="chrome",
            verify=bool(self.ssl_context),
            proxy=proxy_or_none,
            timeout=self.config.rate_limits.curl_timeout,
            max_redirects=_MAX_REDIRECTS,
            cookies={cookie.key: cookie.value for cookie in self.cookies},
        )

    def create_aiohttp_session(self) -> ClientSession:
        assert _dns_resolver is not None
        tcp_conn = aiohttp.TCPConnector(
            ssl=self.ssl_context,
            resolver=_dns_resolver(),
            ttl_dns_cache=5_000,
        )
        tcp_conn._resolver_owner = True
        return ClientSession(
            headers={"user-agent": self.config.general.user_agent},
            raise_for_status=False,
            cookie_jar=self.cookies,
            timeout=self.config.rate_limits.aiohttp_timeout,
            proxy=self.config.general.proxy,
            connector=tcp_conn,
            requote_redirect_url=False,
        )

    async def load_cookie_files(self) -> None:
        if self.config.browser_cookies.auto_import:
            assert self.config.browser_cookies.browser
            get_cookies_from_browser(self.config.browser_cookies.browser, "")

        cookie_files = sorted(appdata.get().cookies_dir.glob("*.txt"))
        if not cookie_files:
            return

        async for domain, cookie in read_netscape_files(cookie_files):
            self.cookies.update_cookies(cookie, response_url=AbsoluteHttpURL(f"https://{domain}"))

        logger.info(spacer())

    async def check_http_status(
        self,
        response: ClientResponse | CurlResponse | AbstractResponse,
        *,
        download: bool = False,
    ) -> None:
        """Checks the HTTP status code and raises an exception if it's not acceptable.

        If the response is successful and has valid html, returns soup
        """
        if not isinstance(response, AbstractResponse):
            response = AbstractResponse.from_resp(response)

        if download and (e_tag := response.headers.get("ETag")) in _DOWNLOAD_ERROR_ETAGS:
            raise DownloadError(HTTPStatus.NOT_FOUND, message=_DOWNLOAD_ERROR_ETAGS[e_tag])

        if HTTPStatus.OK <= response.status < HTTPStatus.BAD_REQUEST:
            return

        await self._check_json(response)

        await ddos_guard.check(response)
        raise DownloadError(status=response.status)

    async def _check_json(self, response: AbstractResponse) -> None:
        if "json" not in response.content_type:
            return

        if check := self._json_response_checks.get(response.url.host):
            check(await response.json())
            return

        for domain, check in self._json_response_checks.items():
            if domain in response.url.host:
                self._json_response_checks[response.url.host] = check
                check(await response.json())
                return

    async def close(self) -> None:
        await self._flaresolverr.close()


def check_content_length(headers: Mapping[str, Any]) -> None:
    content_length, content_type = headers.get("Content-Length"), headers.get("Content-Type")
    if content_length is None or content_type is None:
        return
    if content_length == "322509" and content_type == "video/mp4":
        raise DownloadError(status="Bunkr Maintenance", message="Bunkr under maintenance")
    if content_length == "73003" and content_type == "video/mp4":
        raise DownloadError(410)  # Placeholder video with text "Video removed" (efukt)


async def check_file_duration(media_item: MediaItem, config: config.Config) -> bool:
    """Checks the file runtime against the config runtime limits."""
    if media_item.is_segment:
        return True

    is_video = media_item.ext.lower() in constants.FileFormats.VIDEO
    is_audio = media_item.ext.lower() in constants.FileFormats.AUDIO
    if not (is_video or is_audio):
        return True

    duration_limits = config.media_duration_limits.ranges

    async def get_duration() -> float | None:
        if media_item.downloaded:
            properties = await ffmpeg.probe(media_item.complete_file)
        else:
            properties = await ffmpeg.probe(media_item.url, headers=media_item.headers)

        if properties.format.duration:
            return properties.format.duration
        if is_video and properties.video:
            return properties.video.duration
        if is_audio and properties.audio:
            return properties.audio.duration

    if media_item.duration is None:
        media_item.duration = await get_duration()

    if media_item.duration is None:
        return True

    if is_video:
        return media_item.duration in duration_limits.video

    return media_item.duration in duration_limits.audio


async def _get_dns_resolver(
    loop: asyncio.AbstractEventLoop | None = None,
) -> type[AsyncResolver] | type[ThreadedResolver]:
    """Test aiodns with a DNS lookup."""

    # pycares (the underlying C extension library that aiodns uses) installs successfully in most cases,
    # but it fails to actually connect to DNS servers on some platforms (e.g., Android).
    try:
        import aiodns

        async with aiodns.DNSResolver(loop=loop, timeout=5.0) as resolver:
            _ = await resolver.query_dns("github.com", "A")
        return aiohttp.AsyncResolver
    except Exception as e:
        logger.warning(f"Unable to setup asynchronous DNS resolver. Falling back to thread based resolver: {e}")
        return aiohttp.ThreadedResolver


def _check_curl_cffi_is_available() -> None:
    if _curl_import_error is None:
        return

    system = "Android" if env.RUNNING_IN_TERMUX else "the system"
    msg = (
        f"curl_cffi is required to scrape this URL but a dependency it's not available on {system}.\n"
        f"See: https://github.com/lexiforest/curl_cffi/issues/74#issuecomment-1849365636\n{_curl_import_error!r}"
    )
    raise ScrapeError("Missing Dependency", msg)
