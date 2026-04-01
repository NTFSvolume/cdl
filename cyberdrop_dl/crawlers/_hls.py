from __future__ import annotations

import asyncio
import dataclasses
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal

from cyberdrop_dl.utils import m3u8

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import aiohttp

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL


class HLSParser(ABC):
    """Class to fetch and parse HTTP live streams

    For multi variant m3u8, the best resolution will be automatically selected"""

    @abstractmethod
    async def request_text(
        self,
        url: AbsoluteHttpURL,
        /,
        method: Literal["GET"] = "GET",
        headers: Mapping[str, str] | None = None,
    ) -> str: ...

    async def request_m3u8(
        self,
        url: AbsoluteHttpURL,
        /,
        headers: Mapping[str, str] | None = None,
        *,
        only: Iterable[str] = (),
        exclude: Iterable[str] = ("vp09",),
    ) -> tuple[m3u8.RenditionGroup, m3u8.RenditionGroupDetails | None]:
        m3u8_obj = await self._request_m3u8(url, headers)
        if m3u8_obj.is_variant:
            return await self.__select_best_rendition(m3u8_obj, headers, only=only, exclude=exclude)
        m3u8_obj.media_type = "video"
        return m3u8.RenditionGroup(m3u8_obj), None

    async def __select_best_rendition(
        self,
        m3u8_playlist: m3u8.M3U8,
        /,
        headers: Mapping[str, str] | None = None,
        *,
        only: Iterable[str] = (),
        exclude: Iterable[str] = (),
    ):
        details = m3u8.get_best_group_from_playlist(m3u8_playlist, only=only, exclude=exclude)
        video, *audio_and_subs = await asyncio.gather(
            *(
                self._request_m3u8(url, headers, name)
                for name, url in zip(
                    ("video", "audio", "subtitle"),
                    details.urls,
                    strict=True,
                )
                if url
            )
        )

        return m3u8.RenditionGroup(video, *audio_and_subs), details

    async def _request_m3u8(
        self,
        url: AbsoluteHttpURL,
        /,
        headers: Mapping[str, str] | None = None,
        media_type: Literal["video", "audio", "subtitle"] | None = None,
    ) -> m3u8.M3U8:
        content = await self.request_text(url, headers=headers)
        return m3u8.M3U8(content, url.parent, media_type)


@dataclasses.dataclass(slots=True)
class SimpleHLSParser(HLSParser):
    """A simple parser that does not depend on the manager.

    DO NOT USE. This is only for testing"""

    _session: aiohttp.ClientSession

    async def request_text(
        self,
        url: AbsoluteHttpURL,
        /,
        method: Literal["GET"] = "GET",
        headers: Mapping[str, str] | None = None,
    ) -> str:
        async with self._session.get(url, headers=headers) as resp:
            return await resp.text()
