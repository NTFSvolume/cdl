from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
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
from cyberdrop_dl.clients.download_client import StreamDownloader
from cyberdrop_dl.clients.flaresolverr import FlareSolverr
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.clients.scraper_client import ScraperClient
from cyberdrop_dl.cookies import get_cookies_from_browsers, read_netscape_files
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import DownloadError, ScrapeError
from cyberdrop_dl.logger import log_debug, log_spacer
from cyberdrop_dl.utils.ffmpeg import probe

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from http.cookies import BaseCookie

    from bs4 import BeautifulSoup
    from curl_cffi.requests import AsyncSession
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


_null_context = contextlib.nullcontext()
logger = logging.getLogger(__name__)


def _create_ssl():
    ssl_context = config.get().general.ssl_context
    if not ssl_context:
        return False

    if ssl_context == "certifi":
        return ssl.create_default_context(cafile=certifi.where())
    if ssl_context == "truststore":
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cafile=certifi.where())
    return ctx


class HttpClient:
    """Creates a 'client' that can be referenced by scraping or download sessions."""

    def __init__(self, config: config.Config) -> None:
        self.config = config
        self.ssl_context: ssl.SSLContext | Literal[False] = _create_ssl()
        self.cookies: aiohttp.CookieJar = aiohttp.CookieJar(quote_cookie=False)
        self.rate_limits: dict[str, AsyncLimiter] = {}
        self.download_slots: dict[str, int] = {}

        rate_limits = config.rate_limits

        self.global_rate_limiter: AsyncLimiter = AsyncLimiter(rate_limits.rate_limit, 1)
        self.scraper_client: ScraperClient = ScraperClient(self)
        self.download_client: StreamDownloader = StreamDownloader(self)
        self.flaresolverr: FlareSolverr = FlareSolverr(self)

        self._session: aiohttp.ClientSession
        self.dl_session: aiohttp.ClientSession
        self._json_response_checks: dict[str, Callable[[Any], None]] = {}
        self._curl_session: AsyncSession[Response] | None = None

    def _startup(self) -> None:
        self._session = self.new_scrape_session()
        self.dl_session = self.new_download_session()
        if _curl_import_error is not None:
            return

    async def __aenter__(self) -> Self:
        self._startup()
        return self

    @property
    def curl_session(self) -> AsyncSession[Response]:
        if self._curl_session is None:
            self.check_curl_cffi_is_available()
            self._curl_session = self.new_curl_cffi_session()
        return self._curl_session

    async def __aexit__(self, *_) -> None:
        await self._session.close()
        await self.dl_session.close()
        if self._curl_session is None:
            return
        try:
            await self._curl_session.close()
        except Exception:
            pass

    @property
    def rate_limiting_options(self):
        return config.get().rate_limits

    def get_download_slots(self, domain: str) -> int:
        """Returns the download limit for a domain."""

        instances = self.download_slots.get(domain, self.rate_limiting_options.max_simultaneous_downloads_per_domain)

        return min(instances, self.rate_limiting_options.max_simultaneous_downloads_per_domain)

    @staticmethod
    def basic_auth(username: str, password: str) -> str:
        """Returns a basic auth token."""
        token = b64encode(f"{username}:{password}".encode()).decode("ascii")
        return f"Basic {token}"

    def check_allowed_filetype(self, media_item: MediaItem) -> bool:
        """Checks if the file type is allowed to download."""
        ignore_options = config.get().ignore
        ext = media_item.ext.lower()

        if ext in constants.FileFormats.IMAGE and ignore_options.exclude_images:
            return False
        if ext in constants.FileFormats.VIDEO and ignore_options.exclude_videos:
            return False
        if ext in constants.FileFormats.AUDIO and ignore_options.exclude_audio:
            return False

        return ext in constants.FileFormats.MEDIA or not ignore_options.exclude_other

    def check_allowed_date_range(self, media_item: MediaItem) -> bool:
        """Checks if the file was uploaded within the config date range"""
        datetime = media_item.datetime_obj()
        if not datetime:
            return True

        item_date = datetime.date()
        ignore_options = config.get().ignore

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

    async def startup(self) -> None:
        await _set_dns_resolver()

    def new_curl_cffi_session(self) -> AsyncSession[CurlResponse]:
        # Calling code should have validated if curl is actually available
        import warnings

        from curl_cffi.aio import AsyncCurl
        from curl_cffi.requests import AsyncSession
        from curl_cffi.utils import CurlCffiWarning

        loop = asyncio.get_running_loop()

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=CurlCffiWarning)
            acurl = AsyncCurl(loop=loop)

        proxy_or_none = str(proxy) if (proxy := config.get().general.proxy) else None

        return AsyncSession(
            loop=loop,
            async_curl=acurl,
            impersonate="chrome",
            verify=bool(self.ssl_context),
            proxy=proxy_or_none,
            timeout=self.rate_limiting_options._curl_timeout,
            max_redirects=constants.MAX_REDIRECTS,
            cookies={cookie.key: cookie.value for cookie in self.cookies},
        )

    def new_scrape_session(self) -> ClientSession:
        trace_configs = _create_request_log_hooks("scrape")
        return self._new_session(trace_configs=trace_configs)

    def new_download_session(self) -> ClientSession:
        trace_configs = _create_request_log_hooks("download")
        return self._new_session(trace_configs=trace_configs)

    def _new_session(self, trace_configs: list[aiohttp.TraceConfig] | None = None) -> ClientSession:
        timeout = self.rate_limiting_options.aiohttp_timeout
        return ClientSession(
            headers={"user-agent": config.get().general.user_agent},
            raise_for_status=False,
            cookie_jar=self.cookies,
            timeout=timeout,
            trace_configs=trace_configs,
            proxy=config.get().general.proxy,
            connector=self._new_tcp_connector(),
            requote_redirect_url=False,
        )

    def _new_tcp_connector(self) -> aiohttp.TCPConnector:
        assert constants.DNS_RESOLVER is not None
        conn = aiohttp.TCPConnector(ssl=self.ssl_context, resolver=constants.DNS_RESOLVER())
        conn._resolver_owner = True
        return conn

    async def load_cookie_files(self) -> None:
        if config.get().browser_cookies.auto_import:
            assert config.get().browser_cookies.browser
            get_cookies_from_browsers(browser=config.get().browser_cookies.browser)

        cookie_files = sorted(appdata.get().cookies_dir.glob("*.txt"))
        if not cookie_files:
            return

        async for domain, cookie in read_netscape_files(cookie_files):
            self.cookies.update_cookies(cookie, response_url=AbsoluteHttpURL(f"https://{domain}"))

        log_spacer(20, log_to_console=False)

    def get_rate_limiter(self, domain: str) -> AsyncLimiter:
        """Get a rate limiter for a domain."""
        if domain in self.rate_limits:
            return self.rate_limits[domain]
        return self.rate_limits["other"]

    async def check_http_status(
        self,
        response: ClientResponse | CurlResponse | AbstractResponse,
        download: bool = False,
    ) -> BeautifulSoup | None:
        """Checks the HTTP status code and raises an exception if it's not acceptable.

        If the response is successful and has valid html, returns soup
        """
        if not isinstance(response, AbstractResponse):
            response = AbstractResponse.from_resp(response)

        message = None

        def check_etag() -> None:
            if download and (e_tag := response.headers.get("ETag")) in _DOWNLOAD_ERROR_ETAGS:
                message = _DOWNLOAD_ERROR_ETAGS[e_tag]
                raise DownloadError(HTTPStatus.NOT_FOUND, message=message)

        check_etag()
        if HTTPStatus.OK <= response.status < HTTPStatus.BAD_REQUEST:
            # Check DDosGuard even on successful pages
            return

        await self._check_json(response)

        await ddos_guard.check(response)
        raise DownloadError(status=response.status, message=message)

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

    @staticmethod
    def check_content_length(headers: Mapping[str, Any]) -> None:
        content_length, content_type = headers.get("Content-Length"), headers.get("Content-Type")
        if content_length is None or content_type is None:
            return
        if content_length == "322509" and content_type == "video/mp4":
            raise DownloadError(status="Bunkr Maintenance", message="Bunkr under maintenance")
        if content_length == "73003" and content_type == "video/mp4":
            raise DownloadError(410)  # Placeholder video with text "Video removed" (efukt)

    async def close(self) -> None:
        await self.flaresolverr.close()


async def check_file_duration(media_item: MediaItem) -> bool:
    """Checks the file runtime against the config runtime limits."""
    if media_item.is_segment:
        return True

    is_video = media_item.ext.lower() in constants.FileFormats.VIDEO
    is_audio = media_item.ext.lower() in constants.FileFormats.AUDIO
    if not (is_video or is_audio):
        return True

    duration_limits = config.get().media_duration_limits.ranges

    async def get_duration() -> float | None:
        if media_item.downloaded:
            properties = await probe(media_item.complete_file)
        else:
            properties = await probe(media_item.url, headers=media_item.headers)

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


async def _set_dns_resolver(loop: asyncio.AbstractEventLoop | None = None) -> None:
    if constants.DNS_RESOLVER is not None:
        return
    try:
        await _test_async_resolver(loop)
        constants.DNS_RESOLVER = aiohttp.AsyncResolver
    except Exception as e:
        constants.DNS_RESOLVER = aiohttp.ThreadedResolver
        logger.warning(f"Unable to setup asynchronous DNS resolver. Falling back to thread based resolver: {e}")


async def _test_async_resolver(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Test aiodns with a DNS lookup."""

    # pycares (the underlying C extension library that aiodns uses) installs successfully in most cases,
    # but it fails to actually connect to DNS servers on some platforms (e.g., Android).
    import aiodns

    async with aiodns.DNSResolver(loop=loop, timeout=5.0) as resolver:
        _ = await resolver.query_dns("github.com", "A")


def _create_request_log_hooks(client_type: Literal["scrape", "download"]) -> list[aiohttp.TraceConfig]:
    async def on_request_start(*args) -> None:
        params: aiohttp.TraceRequestStartParams = args[2]
        log_debug(f"Starting {client_type} {params.method} request to {params.url}", 10)

    async def on_request_end(*args) -> None:
        params: aiohttp.TraceRequestEndParams = args[2]
        msg = f"Finishing {client_type} {params.method} request to {params.url}"
        msg += f" -> response status: {params.response.status}"
        log_debug(msg, 10)

    trace_config = aiohttp.TraceConfig()
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_end.append(on_request_end)
    return [trace_config]


def check_curl_cffi_is_available() -> None:
    if _curl_import_error is None:
        return

    system = "Android" if env.RUNNING_IN_TERMUX else "the system"
    msg = (
        f"curl_cffi is required to scrape this URL but a dependency it's not available on {system}.\n"
        f"See: https://github.com/lexiforest/curl_cffi/issues/74#issuecomment-1849365636\n{_curl_import_error!r}"
    )
    raise ScrapeError("Missing Dependency", msg)
