from __future__ import annotations

import asyncio
import dataclasses
import itertools
import logging
import time
from http.cookies import SimpleCookie
from typing import TYPE_CHECKING, Any

import aiohttp
from multidict import CIMultiDict, CIMultiDictProxy

from cyberdrop_dl.compat import StrEnum
from cyberdrop_dl.data_structures import AbsoluteHttpURL
from cyberdrop_dl.exceptions import DDOSGuardError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


logger = logging.getLogger(__name__)


class _Command(StrEnum):
    CREATE_SESSION = "sessions.create"
    DESTROY_SESSION = "sessions.destroy"
    LIST_SESSIONS = "sessions.list"

    GET_REQUEST = "request.get"
    POST_REQUEST = "request.post"


@dataclasses.dataclass(slots=True)
class FlareSolverrSolution:
    content: str
    cookies: SimpleCookie
    headers: CIMultiDictProxy[str]
    url: AbsoluteHttpURL
    user_agent: str
    status: int

    @staticmethod
    def from_dict(solution: dict[str, Any]) -> FlareSolverrSolution:
        return FlareSolverrSolution(
            status=int(solution["status"]),
            cookies=_parse_cookies(solution.get("cookies") or ()),
            user_agent=solution["userAgent"],
            content=solution["response"],
            url=AbsoluteHttpURL(solution["url"]),
            headers=CIMultiDictProxy(CIMultiDict(solution["headers"])),
        )


@dataclasses.dataclass(slots=True)
class _FlareSolverrResponse:
    status: str
    message: str
    solution: FlareSolverrSolution | None

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    @staticmethod
    def from_dict(resp: dict[str, Any]) -> _FlareSolverrResponse:
        return _FlareSolverrResponse(
            resp["status"],
            resp["message"],
            solution=FlareSolverrSolution.from_dict(sol) if (sol := resp.get("solution")) else None,
        )


@dataclasses.dataclass(slots=True)
class FlareSolverr:
    """Class that handles communication with flaresolverr."""

    url: AbsoluteHttpURL
    _session: aiohttp.ClientSession

    _session_id: str = dataclasses.field(init=False, default="")
    _session_lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock)
    _request_lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock)
    _next_request_id: Callable[[], int] = dataclasses.field(
        init=False, default_factory=lambda: itertools.count(1).__next__
    )

    def __post_init__(self) -> None:
        self.url = self.url.origin() / "v1"

    async def aclose(self) -> None:
        try:
            await self._destroy_session()
        except Exception as e:
            logger.error(f"Unable to destroy flaresolver session ({e})")

    async def request(self, url: AbsoluteHttpURL, data: Any = None) -> FlareSolverrSolution:
        invalid_response_error = DDOSGuardError("Invalid response from flaresolverr")
        try:
            if not self._session_id:
                async with self._session_lock:
                    if not self._session_id:
                        await self._create_session()

            resp = await self._request(
                _Command.POST_REQUEST if data else _Command.GET_REQUEST,
                url=str(url),
                data=data,
                session=self._session_id,
            )

        except (TypeError, KeyError) as e:
            raise invalid_response_error from e

        if not resp.ok:
            raise DDOSGuardError(f"Failed to resolve URL with flaresolverr. {resp.message}")

        if not resp.solution:
            raise invalid_response_error

        return resp.solution

    async def _request(self, command: _Command, /, data: Any = None, **kwargs: Any) -> _FlareSolverrResponse:
        kwargs = {}
        if command is _Command.CREATE_SESSION:
            kwargs.update(timeout=aiohttp.ClientTimeout(total=5 * 60, connect=60))  # 5 minutes to create session

        #  timeout in milliseconds (60s)
        playload: dict[str, Any] = {"cmd": command, "maxTimeout": 60_000} | kwargs

        if data:
            assert command is _Command.POST_REQUEST
            playload["postData"] = aiohttp.FormData(data)().decode()

        async with self._request_lock:
            logger.debug(f"Waiting For FlareSolverr response ({self._next_request_id()})")
            async with self._session.post(self.url, json=playload, **kwargs) as response:
                return _FlareSolverrResponse.from_dict(await response.json())

    async def _create_session(self) -> None:
        session_id = "cyberdrop-dl"
        kwargs = {}

        if proxy := self._session._default_proxy:
            kwargs.update(proxy={"url": str(proxy)})

        resp = await self._request(_Command.CREATE_SESSION, session=session_id, **kwargs)
        if not resp.ok:
            raise DDOSGuardError(f"Failed to create flaresolverr session: {resp.message}")
        self._session_id = session_id

    async def _destroy_session(self) -> None:
        if self._session_id:
            _ = await self._request(_Command.DESTROY_SESSION)
            self._session_id = ""


def _parse_cookies(cookies: Iterable[dict[str, Any]]) -> SimpleCookie:
    simple_cookie = SimpleCookie()
    now = time.time()
    for cookie in cookies:
        name: str = cookie["name"]
        simple_cookie[name] = cookie["value"]
        morsel = simple_cookie[name]
        morsel["domain"] = cookie["domain"]
        morsel["path"] = cookie["path"]
        morsel["secure"] = "TRUE" if cookie["secure"] else ""
        if expires := cookie["expires"]:
            morsel["max-age"] = str(max(0, int(expires) - int(now)))
    return simple_cookie
