from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, override

from cyberdrop_dl.clients.http import HTTPConfig
from cyberdrop_dl.crawlers.crawler import API, Crawler, SupportedPaths
from cyberdrop_dl.exceptions import ScrapeError
from cyberdrop_dl.mediaprops import Resolution
from cyberdrop_dl.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import dates
from cyberdrop_dl.utils.dataclass import struct
from cyberdrop_dl.utils.errors import error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from cyberdrop_dl.url_objects import ScrapeItem


class ArteTVCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "Video": (
            "/<lang>/videos/<id>/...",
            "/embeds/<lang>/<id>",
        ),
    }
    DOMAIN: ClassVar[str] = "arte.tv"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://www.arte.tv")

    @staticmethod
    @override
    def __db_path__(url: AbsoluteHttpURL, /) -> str:
        match url.parts[1:]:
            case [lang, "videos", video_id, *_]:
                return f"/{lang}/{video_id}"
            case _:
                return url.path

    def __post_init__(self) -> None:
        self.api: ArteTVAPI = ArteTVAPI.from_crawler(self)

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["de" | "fr" | "en" | "es" | "pl" | "it" as lang, "videos", video_id, *_]:
                await self.video(scrape_item, lang, video_id)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def video(self, scrape_item: ScrapeItem, lang: str, video_id: str) -> None:
        if await self.check_complete(scrape_item.url):
            return

        video = await self.api.video(lang, video_id)
        best_stream = max(video.streams, key=_score_streams(lang))
        scrape_item.setup_as_album(self.create_title(video.title))
        if len(video.streams) > 1:
            self.log.warning(
                "Found multiple streams versions for %s[%s], selected version: %s\n%s",
                video_id,
                lang,
                best_stream.version.code,
                video.streams,
            )

        m3u8 = debrid_url = info = None
        if best_stream.protocol == "hls":
            m3u8, info = await self.request_m3u8_playlist(best_stream.url)
        else:
            debrid_url = best_stream.url

        # For series/docs, title is the name of the series and subtitle is the name of the episode
        # For movies, subtitle is 'null'
        full_title = " - ".join(filter(None, (video.title, video.subtitle)))
        scrape_item.uploaded_at = video.uploaded

        await self.handle_file(
            scrape_item.url,
            scrape_item,
            full_title,
            ext := ".mp4",
            m3u8=m3u8,
            custom_filename=self.create_custom_filename(
                video.subtitle or video.title,
                ext,
                file_id=video_id,
                resolution=info.resolution if info else best_stream.resolution,
                video_codec=info and info.codecs.video,
                audio_codec=best_stream.version.audioLanguage,
            ),
            debrid_link=debrid_url,
            metadata=video,
            thumbnail=video.thumb,
        )


@struct(extras="ignore")
class Version:
    code: str
    label: str
    shortLabel: str
    audioLanguage: str
    subtitleLanguage: str
    closedCaptioning: bool
    audioDescription: bool

    @property
    def has_subs(self) -> bool:
        return "-ST" in self.code


@struct
class Stream:
    protocol: Literal["hls", "https"]
    url: AbsoluteHttpURL
    version: Version
    resolution: Resolution


# ruff: noqa: N815
@struct(extras="ignore")
class Video:
    providerId: str
    language: str
    title: str
    description: str
    uploaded: int
    thumb: str | None
    subtitle: str | None = None
    streams: tuple[Stream, ...]


@HTTPConfig(rate_limit=(3, 10))
class ArteTVAPI(API):
    ENTRYPOINT: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://api.arte.tv/api/player/v2")

    async def video(self, lang: str, video_id: str) -> Video:
        url = self.ENTRYPOINT / "config" / lang / video_id
        resp = await self.request_json(url, headers={"x-validated-age": "18"})
        attrs = resp["data"]["attributes"]
        if attrs.get("live"):
            raise ScrapeError(422, "Live streams are not supported")
        try:
            geo = attrs["restriction"]["geoblocking"]
        except LookupError:
            pass
        else:
            if geo.get("restrictedArea"):
                raise ScrapeError(403, f"Video restricted to {geo['code']!r}")

        try:
            thumb = attrs["metadata"]["images"][0]["url"]
        except LookupError:
            thumb = None

        return Video(
            **attrs["metadata"],
            uploaded=int(dates.parse_iso(attrs["rights"]["begin"]).timestamp()),
            streams=tuple(_parse_streams(attrs["streams"])),
            thumb=thumb,
        )


def _parse_streams(streams: list[dict[str, Any]]) -> Generator[Stream]:
    for stream in streams:
        match stream["protocol"]:
            case "HTTPS":
                proto = "https"
            case str(name) if "HLS" in name:
                proto = "hls"
            case "RTMP":
                continue
            case _:
                ArteTVCrawler.get_logger().warning(
                    "Ignoring unknown unsupported stream protocol [%s]:\n%s", stream["protocol"], stream
                )
                continue

        version = Version(**stream["versions"][0])

        try:
            res = Resolution.parse(stream["mainQuality"]["label"])
        except (LookupError, ValueError):
            res = Resolution.unknown()

        yield Stream(
            protocol=proto,
            version=version,
            url=ArteTVCrawler.parse_url(stream["url"]),
            resolution=res,
        )


def _score_streams(desired_lang: str) -> Callable[[Stream], tuple[bool, ...]]:

    def score(stream: Stream):
        version = stream.version
        return (
            version.audioLanguage == desired_lang,
            not version.audioDescription,
            "original" in version.label.lower(),
            version.subtitleLanguage == desired_lang,
            not version.has_subs,
            not version.closedCaptioning,
            stream.protocol != "hls",
        )

    return score
