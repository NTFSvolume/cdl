from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cyberdrop_dl import constants, ddos_guard
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.exceptions import DDOSGuardError
from cyberdrop_dl.utils.cookie_management import make_simple_cookie
from cyberdrop_dl.utils.utilities import sanitize_filename

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping

    from curl_cffi.requests.impersonate import BrowserTypeLiteral
    from curl_cffi.requests.session import HttpMethod

    from cyberdrop_dl.clients import flaresolverr
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
    from cyberdrop_dl.managers.client_manager import ClientManager

logger = logging.getLogger(__name__)


class ScraperClient:
    """AIOHTTP / CURL operations for scraping."""

    def __init__(self, client_manager: ClientManager) -> None:
        self.client_manager = client_manager
        self._save_pages_html = client_manager.manager.config_manager.settings_data.files.save_pages_html
        self._resp_folder = self.client_manager.manager.path_manager.pages_folder

    @contextlib.asynccontextmanager
    async def _limiter(self, domain: str) -> AsyncGenerator[None]:
        with self.client_manager.request_context(domain):
            domain_limiter = self.client_manager.get_rate_limiter(domain)
            async with self.client_manager.global_rate_limiter, domain_limiter:
                await self.client_manager.manager.states.RUNNING.wait()
                yield

    @contextlib.asynccontextmanager
    async def _request(
        self: object,
        url: AbsoluteHttpURL,
        /,
        method: HttpMethod = "GET",
        headers: dict[str, str] | None = None,
        impersonate: BrowserTypeLiteral | bool | None = None,
        data: Any = None,
        json: Any = None,
        cache_disabled: bool = False,
        **request_params: Any,
    ) -> AsyncGenerator[AbstractResponse[Any]]:
        """
        Asynchronous context manager for HTTP requests.

        - If 'impersonate' is specified, uses curl_cffi for the request and updates cookies.
        - Otherwise, uses aiohttp with optional cache control.
        - Yield an AbstractResponse that wraps the underlying response with common methods.
        - On DDOSGuardError, retries the request using FlareSolverr.
        - Saves the HTML content to disk if the config option is enabled.
        - Closes underliying response on exit.
        """
        self = cast("ScraperClient", self)
        request_params["headers"] = headers = headers or {}
        request_params["data"] = data
        request_params["json"] = json

        if method == "GET" and (data or json):
            method = "POST"

        impersonate = self.client_manager.manager.parsed_args.cli_only_args.impersonate or impersonate
        if impersonate:
            self.client_manager.check_curl_cffi_is_available()
            if impersonate is True:
                impersonate = "chrome"
            request_params["impersonate"] = impersonate

        else:
            _ = headers.setdefault("user-agent", self.client_manager.manager.global_config.general.user_agent)
            request_params.setdefault("max_redirects", constants.MAX_REDIRECTS)

        request_id = str(uuid.uuid4())
        logger.debug("Starting {} request to {} [id={}]\n{}", method, url, request_id, request_params)

        async with self.__request(url, method, request_params, impersonate=bool(impersonate)) as resp:
            exc = None
            try:
                yield await self._check_response(resp, url)
            except Exception as e:
                exc = e
                raise
            finally:
                logger.debug("Finishing {} request [id={}]\n{}", method, request_id, resp)
                if self._save_pages_html:
                    _ = self.client_manager.manager.task_group.create_task(
                        asyncio.to_thread(
                            _write_resp_to_disk,
                            self._resp_folder,
                            url,
                            resp,
                            exc,
                        )
                    )

    def __sync_session_cookies(self, url: AbsoluteHttpURL) -> None:
        """
        Apply to the cookies from the `curl` session into the `aiohttp` session, filtering them by the URL

        This is mostly just to get the `cf_cleareance` cookie value into the `aiohttp` session

        The reverse (sync `aiohttp` -> `curl`) is not needed at the moment, so it is skipped
        """
        now = time.time()
        for cookie in self.client_manager._curl_session.cookies.jar:
            simple_cookie = make_simple_cookie(cookie, now)
            self.client_manager.cookies.update_cookies(simple_cookie, url)

    @contextlib.asynccontextmanager
    async def __request(
        self,
        url: AbsoluteHttpURL,
        method: HttpMethod,
        request_params: Mapping[str, Any],
        *,
        impersonate: bool,
    ) -> AsyncGenerator[AbstractResponse[Any]]:

        if impersonate:
            async with contextlib.aclosing(
                await self.client_manager._curl_session.request(method, str(url), stream=True, **request_params)
            ) as curl_resp:
                yield AbstractResponse.create(curl_resp)
                self.__sync_session_cookies(url)

            return

        async with (
            self.client_manager._session.request(method, url, **request_params) as aio_resp,
        ):
            yield AbstractResponse.create(aio_resp)

    async def _check_response(self, abs_resp: AbstractResponse[Any], url: AbsoluteHttpURL, data: Any | None = None):
        """Checks the HTTP response status and retries DDOS Guard errors with FlareSolverr.

        Returns an AbstractResponse confirmed to not be a DDOS Guard page."""
        try:
            await self.client_manager.check_http_status(abs_resp)
            return abs_resp
        except DDOSGuardError as e:
            if not (flare := self.client_manager.flaresolverr):
                raise

            try:
                solution = await flare.request(url, data)
            except RuntimeError:
                raise e from None

            self.client_manager.cookies.update_cookies(solution.cookies)
            await _check_flaresolverr_resp(self.client_manager.manager.global_config.general.user_agent, solution)
            return AbstractResponse.create(solution)


async def _check_flaresolverr_resp(cdl_user_agent: str, solution: flaresolverr.Solution) -> None:
    mismatch_ua_msg = (
        "Config user_agent and flaresolverr user_agent do not match:"
        f"\n  Cyberdrop-DL: '{cdl_user_agent}'"
        f"\n  Flaresolverr: '{solution.user_agent}'"
    )

    try:
        await ddos_guard.check(solution.content)
    except DDOSGuardError:
        if solution.user_agent != cdl_user_agent:
            raise DDOSGuardError(mismatch_ua_msg) from None

    if solution.user_agent != cdl_user_agent:
        logger.warning(f"{mismatch_ua_msg}\n Response was successful but cookies will not be valid")


def _write_resp_to_disk(
    folder: Path,
    url: AbsoluteHttpURL,
    response: AbstractResponse[Any],
    exc: Exception | None = None,
) -> None:

    max_stem_len = 245 - len(str(folder)) + len(constants.STARTUP_TIME_STR) + 10

    log_date = response.created_at.strftime(constants.LOGS_DATETIME_FORMAT)
    url_str = str(url)
    clean_url = sanitize_filename(Path(url_str).as_posix().replace("/", "-"))
    filename = f"{clean_url[:max_stem_len]}_{log_date}.html"
    file = folder / filename
    content = response.create_report(exc)
    try:
        _ = file.write_text(content, "utf8")
    except OSError:
        pass
