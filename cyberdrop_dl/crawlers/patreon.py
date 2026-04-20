from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any, ClassVar

from bs4 import BeautifulSoup

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import DictDataclass, css, error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import Generator

    from cyberdrop_dl.url_objects import ScrapeItem


@dataclasses.dataclass(slots=True)
class Media:
    id: str
    name: str | None
    url: AbsoluteHttpURL
    props: dict[str, Any]


@dataclasses.dataclass(slots=True)
class User(DictDataclass):
    full_name: str
    url: str


class PatreonCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "Post": "/posts/<slug>",
    }

    DOMAIN: ClassVar[str] = "patreon"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://www.patreon.com")

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["posts", _]:
                return await self.post(scrape_item)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def post(self, scrape_item: ScrapeItem) -> None:
        soup = await self.request_soup(scrape_item.url, impersonate=True)
        bootstrap = _extract_bootstrap(soup)
        post = _flatten_post(bootstrap["post"])
        title = self.create_title(post["title"])
        scrape_item.setup_as_album(title)
        scrape_item.uploaded_at = self.parse_iso_date(post["published_at"])
        for media in self._parse_media(post):
            self.create_task(self._media(scrape_item, media))
            scrape_item.add_children()

    @error_handling_wrapper
    async def _media(self, scrape_item: ScrapeItem, media: Media):
        if media.url.suffix == ".m3u8":
            return await self._m3u8_asset(scrape_item, media)

        name = media.name
        if not name:
            async with self.request(media.url) as resp:
                name = resp.content_disposition.filename

        filename, ext = self.get_filename_and_ext(name)
        await self.handle_file(media.url, scrape_item, name, ext, custom_filename=filename)

    async def _m3u8_asset(self, scrape_item: ScrapeItem, media: Media):
        m3u8, info = await self.request_m3u8_playlist(media.url)
        filename = self.create_custom_filename(
            media.url.name.removesuffix(".m3u8"),
            ext := ".mp4",
            resolution=info.resolution,
            video_codec=info.codecs.video,
            audio_codec=info.codecs.audio,
        )
        await self.handle_file(media.url, scrape_item, filename, ext, m3u8=m3u8)

    def _parse_media(self, post: dict[str, Any]) -> Generator[Media]:
        media_ids: set[str] = set()
        if post_file := post.get("post_file"):
            media_id = str(post_file["media_id"])
            media_ids.add(media_id)
            url = self.parse_url(post_file["url"])
            yield Media(media_id, post_file.get("name"), url, post_file)

        included: dict[str, Any]
        for included in post["included"]:
            asset_type: str = included["type"]
            attributes: dict[str, Any] = included["attributes"]

            match asset_type:
                case "media" if url := attributes.get("download_url"):
                    media_id: str = str(included["id"])
                    if media_id in media_ids:
                        continue

                    media_ids.add(media_id)
                    yield Media(media_id, attributes.get("file_name"), self.parse_url(url), attributes)
                case _:
                    continue

        if not post["content"]:
            return

        return
        soup = BeautifulSoup(post["content"], "html.parser")
        for media_id in css.iselect(soup, "[data-media-id]", "data-media-id"):
            if media_id in media_ids:
                continue
            self.log.warning("Found extra media id %s", media_id)
            media_ids.add(media_id)


def _extract_bootstrap(soup: BeautifulSoup) -> dict[str, Any]:
    data = json.loads(css.select_text(soup, "#__NEXT_DATA__"))
    envelope = data["props"]["pageProps"]["bootstrapEnvelope"]
    return envelope.get("pageBootstrap") or envelope["bootstrap"]


def _parse_post(post: dict[str, Any]) -> Generator[tuple[str, Any]]:
    post_data = post["data"]
    yield "id", int(post_data["id"])
    yield from _parse_attributes(post_data["attributes"])
    yield "included", post["included"]


def _parse_attributes(attributes: dict[str, Any]) -> Generator[tuple[str, Any]]:
    json_string = "_json_string"
    json_keys = tuple(key for key in attributes if key.endswith(json_string))

    for json_key in json_keys:
        name = json_key.removesuffix(json_string)
        value = attributes.pop(name, None)
        json_value = attributes.pop(json_key, None)
        # TODO: convert to html
        if not value and json_value:
            value = json.loads(json_value)

        yield name, value

    yield from attributes.items()


def _flatten_post(post: dict[str, Any]) -> dict[str, Any]:
    return dict(sorted(_parse_post(post)))
