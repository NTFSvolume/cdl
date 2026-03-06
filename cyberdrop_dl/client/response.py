from __future__ import annotations

import asyncio
import dataclasses
import datetime
import json
from abc import ABC, abstractmethod
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, Literal, final

import aiohttp.multipart
from aiohttp import hdrs
from aiohttp.client_reqrep import ClientResponse, ContentDisposition
from bs4 import BeautifulSoup
from multidict import CIMultiDict, CIMultiDictProxy
from propcache import under_cached_property
from typing_extensions import TypeVar

from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.exceptions import InvalidContentTypeError, ScrapeError
from cyberdrop_dl.utils import parse_url

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from curl_cffi.requests.models import Response as CurlResponse

    from cyberdrop_dl.client.flaresolverr import FlareSolverrSolution

else:
    CurlResponse = object
    FlareSolverrSolution = object

__all__ = ["AbstractResponse"]

_ResponseT = TypeVar("_ResponseT", bound=ClientResponse | CurlResponse | None, default=None)


@dataclasses.dataclass(slots=True, eq=False)
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
    _text: str = ""
    created_at: datetime.datetime = dataclasses.field(
        init=False, default_factory=lambda: datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    )
    _cache: dict[str, Any] = dataclasses.field(init=False, default_factory=dict)
    _read_lock: asyncio.Lock = dataclasses.field(init=False, default_factory=asyncio.Lock, repr=False)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} [{self.status}] ({self.url})>"

    @abstractmethod
    async def _read(self) -> bytes: ...

    @abstractmethod
    async def _read_text(self, encoding: str | None = None) -> str: ...

    @abstractmethod
    def iter_chunked(self, size: int) -> AsyncIterator[bytes]: ...

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
        if location := headers.get(hdrs.LOCATION):
            location = parse_url(location, url.origin(), trim=False)
        else:
            location = None

        content_type = (headers.get(hdrs.CONTENT_TYPE) or "").lower()
        return content_type, location

    @final
    @under_cached_property
    def content_disposition(self) -> ContentDisposition:
        try:
            header = self.headers[hdrs.CONTENT_DISPOSITION]
        except KeyError:
            msg = f"No content dispotition header found for response from {self.url}"
            raise ScrapeError(422, msg) from None
        disposition_type, params = aiohttp.multipart.parse_content_disposition(header)
        params = MappingProxyType(params)
        filename = aiohttp.multipart.content_disposition_filename(params)
        return ContentDisposition(disposition_type, params, filename)

    @final
    @property
    def filename(self) -> str:
        name = self.content_disposition.filename
        if not name:
            msg = "No content dispotition has no filename information"
            raise ScrapeError(422, msg) from None
        return name

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

    def __check_content_type(self, content_type: str, *additional_content_types: str, expecting: str) -> None:
        if not any(type_ in self.content_type for type_ in (content_type, *additional_content_types)):
            msg = f"Received {self.content_type}, was expecting {expecting}"
            raise InvalidContentTypeError(message=msg)

    @final
    async def soup(self, encoding: str | None = None) -> BeautifulSoup:
        self.__check_content_type("text", "html", expecting="HTML")
        content = await self.text(encoding)
        if not content:
            raise ScrapeError(204, "Received empty HTML response")
        return BeautifulSoup(content, "html.parser")

    @final
    async def json(
        self,
        encoding: str | None = None,
        content_type: tuple[str, ...] | str | Literal[False] | None = ("text/plain", "json"),
    ) -> Any:
        if self.status == 204:
            raise ScrapeError(204)

        if content_type:
            if isinstance(content_type, str):
                content_type = (content_type,)

            self.__check_content_type(*content_type, expecting="JSON")

        return json.loads(await self.text(encoding))

    def __str__(self) -> str:
        return self.create_report()

    @final
    def create_report(self, exc: Exception | None = None, *extras: Any) -> str:
        assert self.consumed

        info = {
            "url": str(self.url),
            "status_code": self.status,
            "datetime": self.created_at.isoformat(),
            "response_headers": dict(self.headers),
        }
        if exc:
            info |= {"error": str(exc), "exception": repr(exc)}
        if extras:
            info |= extras

        if "json" in self.content_type:
            info["content"] = json.loads(self._text)
            return json.dumps(info, indent=2, ensure_ascii=False)

        elif "html" in self.content_type:
            body = BeautifulSoup(self._text, "html.parser").prettify(formatter="html")

        else:
            body = self._text

        resp_info = json.dumps(info, indent=2, ensure_ascii=False)
        return f"<!-- cyberdrop-dl request response \n{resp_info}\n-->\n{body}"


class _FlareSolverrResponse(AbstractResponse[None]):
    __slots__ = ()

    async def _read(self) -> bytes:
        return self._text.encode()

    async def _read_text(self, encoding: str | None = None) -> str:
        return self._text

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        yield self._text.encode()


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
