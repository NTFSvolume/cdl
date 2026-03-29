from __future__ import annotations

import base64
import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.data_structures.mediaprops import Resolution
from cyberdrop_dl.exceptions import ScrapeError
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, ScrapeItem


@dataclasses.dataclass(slots=True)
class Video:
    title: str
    thumb: AbsoluteHttpURL
    post_date: str
    src: AbsoluteHttpURL
    resolution: Resolution


class TubeCorporateCrawler(Crawler, is_abc=True):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "Video": (
            "/videos/<video_id>/...",
            "/embed/<video_id>/...",
        )
    }

    def __init_subclass__(cls, **kwargs: Any) -> None:
        domains = cls.PRIMARY_URL.host, *cls.SUPPORTED_DOMAINS
        domains = *domains, *(d.replace(".com", ".tube") for d in domains)
        old_domains = *cls.OLD_DOMAINS, *(d.replace(".com", ".tube") for d in cls.OLD_DOMAINS)
        cls.SUPPORTED_DOMAINS = tuple(sorted(set(domains)))
        cls.OLD_DOMAINS = tuple(sorted(set(old_domains)))
        super().__init_subclass__(**kwargs)

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["videos" | "embed", video_id, *_]:
                return await self.video(scrape_item, video_id)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def video(self, scrape_item: ScrapeItem, video_id: str) -> None:
        if await self.check_complete_from_referer(scrape_item):
            return

        origin = scrape_item.url.origin()
        video = await self._request_video(origin, video_id)
        scrape_item.possible_datetime = self.parse_iso_date(video.post_date)
        ext = ".mp4"
        custom_filename = self.create_custom_filename(video.title, ext, file_id=video_id, resolution=video.resolution)

        return await self.handle_file(
            scrape_item.url,
            scrape_item,
            video.title,
            ext,
            custom_filename=custom_filename,
            debrid_link=video.src,
            metadata=video,
        )

    async def _request_video(self, origin: AbsoluteHttpURL, video_id: str) -> Video:
        res, src = await self._request_stream(origin, video_id)

        mil_index = int(1e6 * (int(video_id) // 1e6))
        k_index = 1_000 * (int(video_id) // 1_000)
        lifetime = 86_400

        json_url = origin / f"api/json/video/{lifetime}/{mil_index}/{k_index}/{video_id}.json"

        video: dict[str, Any] = (await self.request_json(json_url))["video"]

        return Video(
            title=video["title"],
            thumb=self.parse_url(video["thumbsrc"]),
            post_date=video["post_date"],
            resolution=res,
            src=src,
        )

    async def _request_stream(self, origin: AbsoluteHttpURL, video_id: str) -> tuple[Resolution, AbsoluteHttpURL]:
        formats: list[dict[str, str]] | dict[str, str] = await self.request_json(
            (origin / "api/videofile.php").with_query(
                video_id=video_id,
                lifetime=8_640_000,
            )
        )
        if isinstance(formats, dict):
            if formats.get("error"):
                error = formats["msg"]
                if "not_found" in error:
                    error = 404
                elif "private" in error:
                    error = 403

                raise ScrapeError(error)

            raise ScrapeError(422, f"Expected list response, got {formats = !r}")

        def get_res(format: str) -> Resolution:
            height = {
                "_sd.mp4": 480,
                "_hq.mp4": 720,
                "_hd.mp4": 720,
                "_fhd.mp4": 1080,
            }.get(format)
            return Resolution.parse(height)

        res, url = max((get_res(f["format"]), f["video_url"]) for f in formats)

        return res, self.parse_url(_decode_url(url))


def _decode_url(url: str) -> str:
    return base64.b64decode(
        url.translate(
            url.maketrans(
                {
                    "\u0405": "S",
                    "\u0406": "I",
                    "\u0408": "J",
                    "\u0410": "A",
                    "\u0412": "B",
                    "\u0415": "E",
                    "\u041a": "K",
                    "\u041c": "M",
                    "\u041d": "H",
                    "\u041e": "O",
                    "\u0420": "P",
                    "\u0421": "C",
                    "\u0425": "X",
                    ",": "/",
                    ".": "+",
                    "~": "=",
                }
            )
        )
    ).decode()
