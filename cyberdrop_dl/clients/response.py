from __future__ import annotations

import asyncio
import dataclasses
from abc import ABC, abstractmethod
from json import loads as json_loads
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

import aiohttp.multipart
from aiohttp import ClientResponse
from aiohttp.client_reqrep import ContentDisposition
from bs4 import BeautifulSoup
from multidict import CIMultiDict, CIMultiDictProxy
from propcache import under_cached_property
from typing_extensions import TypeIs

from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.exceptions import InvalidContentTypeError, ScrapeError
from cyberdrop_dl.utils import parse_url

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from curl_cffi.requests.models import Response as CurlResponse

    from cyberdrop_dl.clients.flaresolverr import FlareSolverrSolution

    _ResponseT = TypeVar("_ResponseT", bound=ClientResponse | CurlResponse | None)
else:
    _ResponseT = object
    CurlResponse = object


@dataclasses.dataclass(slots=True, weakref_slot=True)
class AbstractResponse(ABC, Generic[_ResponseT]):
    """
    Class to represent common methods and attributes between:
        - `aiohttp.ClientResponse`
        - `curl_cffi.Response`
        - `FlareSolverrSolution`
    """

    content_type: str
    status: int
    headers: CIMultiDictProxy[str]
    url: AbsoluteHttpURL
    location: AbsoluteHttpURL | None

    _resp: _ResponseT
    _text: str
    _cache: dict[str, Any] = dataclasses.field(init=False, default_factory=dict)
    _read_lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} [{self.status}] ({self.url})>"

    @staticmethod
    def is_aiohttp(resp: AbstractResponse[Any]) -> TypeIs[_AIOHTTPResponse]:
        return isinstance(resp._resp, ClientResponse)

    @abstractmethod
    async def _read(self) -> bytes: ...

    @abstractmethod
    async def _read_text(self, encoding: str | None = None) -> str: ...

    @abstractmethod
    def iter_chunked(self, size: int) -> AsyncIterator[bytes]: ...

    def _check_content_type(self, *content_types: str, expecting: str) -> None:
        if not any(type_ in self.content_type for type_ in content_types):
            msg = f"Received {self.content_type}, was expecting {expecting}"
            raise InvalidContentTypeError(message=msg)

    @final
    @staticmethod
    def from_resp(response: ClientResponse | CurlResponse) -> _AIOHTTPResponse | _CurlResponse:
        if isinstance(response, ClientResponse):
            status = response.status
            headers = response.headers
            cls = _AIOHTTPResponse
        else:
            status = response.status_code
            multi_items = ((k, v) for k, v in response.headers.multi_items() if v is not None)
            headers = CIMultiDictProxy(CIMultiDict(multi_items))
            cls = _CurlResponse

        url = AbsoluteHttpURL(response.url)
        content_type, location = cls.parse_headers(url, headers)
        return cls(
            content_type=content_type,
            status=status,
            headers=headers,
            url=url,
            location=location,
            _text="",
            _resp=response,  # pyright: ignore[reportArgumentType]
        )

    @final
    @staticmethod
    def from_flaresolverr(solution: FlareSolverrSolution) -> _FlareSolverrResponse:
        content_type, location = AbstractResponse.parse_headers(solution.url, solution.headers)
        return _FlareSolverrResponse(
            content_type=content_type,
            status=solution.status,
            headers=solution.headers,
            url=solution.url,
            location=location,
            _text=solution.content,
            _resp=None,
        )

    @final
    @staticmethod
    def parse_headers(url: AbsoluteHttpURL, headers: CIMultiDictProxy[str]) -> tuple[str, AbsoluteHttpURL | None]:
        if location := headers.get("location"):
            location = parse_url(location, url.origin(), trim=False)
        else:
            location = None

        content_type = (headers.get("Content-Type") or "").lower()
        return content_type, location

    @final
    @under_cached_property
    def content_disposition(self) -> ContentDisposition:
        header = self.headers["Content-Disposition"]
        disposition_type, params = aiohttp.multipart.parse_content_disposition(header)
        params = MappingProxyType(params)
        filename = aiohttp.multipart.content_disposition_filename(params)
        return ContentDisposition(disposition_type, params, filename)

    @final
    @property
    def filename(self) -> str:
        assert self.content_disposition.filename
        return self.content_disposition.filename

    @property
    def consumed(self) -> bool:
        return bool(self._text)

    @property
    def ok(self) -> bool:
        """Returns `True` if `status` is less than `400`, `False` if not.

        This is **not** a check for ``200 OK``
        """
        return self.status < 400

    @final
    async def read(self) -> bytes:
        async with self._read_lock:
            return await self._read()

    @final
    async def text(self, encoding: str | None = None) -> str:
        if self._text:
            return self._text

        async with self._read_lock:
            if self._text:
                return self._text
            self._text = await self._read_text(encoding)
            return self._text

    @final
    async def soup(self, encoding: str | None = None) -> BeautifulSoup:
        self._check_content_type("text", "html", expecting="HTML")
        content = await self.text(encoding)
        if not content:
            raise ScrapeError(204, "Received empty html response")
        return BeautifulSoup(content, "html.parser")

    @final
    async def json(self, encoding: str | None = None, content_type: str | bool | None = True) -> Any:
        if self.status == 204:
            raise ScrapeError(204)

        if content_type:
            if isinstance(content_type, str):
                check = (content_type,)
            else:
                check = ("text/plain", "json")

            self._check_content_type(*check, expecting="JSON")

        return json_loads(await self.text(encoding))


class _FlareSolverrResponse(AbstractResponse[None]):
    __slots__ = ()

    async def _read(self) -> bytes:
        return self._text.encode()

    async def _read_text(self, encoding: str | None = None) -> str:
        return self._text

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        yield await self._read()


class _AIOHTTPResponse(AbstractResponse[ClientResponse]):
    __slots__ = ()

    async def _read(self) -> bytes:
        return await self._resp.read()

    async def _read_text(self, encoding: str | None = None) -> str:
        return await self._resp.text(encoding)

    def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        return self._resp.content.iter_chunked(size)


class _CurlResponse(AbstractResponse[CurlResponse]):
    __slots__ = ()

    async def _read(self) -> bytes:
        return await self._resp.acontent()

    async def _read_text(self, encoding: str | None = None) -> str:
        if encoding:
            self._resp.encoding = encoding
        return await self._resp.atext()

    def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        # Curl does not support size. We get chunks as they come
        return self._resp.aiter_content()
