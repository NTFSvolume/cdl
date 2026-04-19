from __future__ import annotations

import asyncio
import dataclasses
import json
from typing import TYPE_CHECKING, Any, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import css, error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from bs4 import BeautifulSoup

    from cyberdrop_dl.url_objects import ScrapeItem


@dataclasses.dataclass(slots=True)
class Asset:
    name: str | None
    url: AbsoluteHttpURL
    props: dict[str, Any]


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
        await self._post(scrape_item, post)

    async def _post(self, scrape_item: ScrapeItem, post: dict[str, Any]):
        hashes: set[str] = set()

        def process_asset(item: ScrapeItem, asset: Asset) -> None:
            hash = _md5(asset.url)
            if hash:
                if hash in hashes:
                    return
                hashes.add(hash)
            self.create_task(self._asset(item, asset))
            scrape_item.add_children()

        for name, get_assets in (("images", self._images), ("postfile", self._post_file)):
            new_item = scrape_item.copy()
            new_item.add_to_parent_title(name)
            for asset in get_assets(post):
                process_asset(new_item, asset)

        new_item = scrape_item.copy()
        new_item.add_to_parent_title("attachments")
        async for asset in self._attachments(post):
            process_asset(new_item, asset)

    @error_handling_wrapper
    async def _asset(self, scrape_item: ScrapeItem, asset: Asset):
        if asset.url.suffix == ".m3u8":
            return await self._m3u8_asset(scrape_item, asset)

        name = asset.name
        if not name:
            async with self.request(asset.url) as resp:
                name = resp.filename

        filename, ext = self.get_filename_and_ext(name)
        await self.handle_file(asset.url, scrape_item, filename, ext)

    async def _m3u8_asset(self, scrape_item: ScrapeItem, asset: Asset):
        m3u8, info = await self.request_m3u8_playlist(asset.url)
        filename = self.create_custom_filename(
            asset.url.name.removesuffix(".m3u8"),
            ext := ".mp4",
            resolution=info.resolution,
            video_codec=info.codecs.video,
            audio_codec=info.codecs.audio,
        )
        await self.handle_file(asset.url, scrape_item, filename, ext, m3u8=m3u8)

    def _images(self, post: dict[str, Any]) -> Generator[Asset]:
        for image in post["images"]:
            if url := image.get("download_url"):
                yield Asset(image.get("file_name"), self.parse_url(url), image)

    def _post_file(self, post: dict[str, Any]) -> Generator[Asset]:
        if postfile := post.get("post_file"):
            url = self.parse_url(postfile["url"])
            yield Asset(postfile.get("name"), url, postfile)

    async def _attachments(self, post: dict[str, Any]) -> AsyncGenerator[Asset]:
        for fut in asyncio.as_completed(tuple(map(self._resolve_attachment, post["attachments"]))):
            result = await fut
            if result is not None:
                yield result

        for attachment in post["attachments_media"]:
            if url := attachment.get("download_url"):
                yield Asset(attachment["file_name"], self.parse_url(url), attachment)

    async def _resolve_attachment(self, attachment: dict[str, str]) -> Asset | None:
        try:
            url = await self._get_redirect_url(self.parse_url(attachment["url"]))
        except Exception:
            self.log.exception(f"Unable to resolve {attachment = }")
        else:
            return Asset(attachment["name"], url, attachment)


def _extract_bootstrap(soup: BeautifulSoup) -> dict[str, Any]:
    data = json.loads(css.select_text(soup, "#__NEXT_DATA__"))
    envelope = data["props"]["pageProps"]["bootstrapEnvelope"]
    return envelope.get("pageBootstrap") or envelope["bootstrap"]


def _flatten_included(included: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    flatten = {}
    for asset in included:
        flatten.setdefault(asset["type"], {})[asset["id"]] = asset["attributes"]
    return flatten


def _parse_post(post: dict[str, Any]) -> Generator[tuple[str, Any]]:
    included = _flatten_included(post["included"])
    post_data = post["data"]
    relationships = post_data["relationships"]
    campaign_id = relationships["campaign"]["data"]["id"]

    yield "id", int(post_data["id"])
    yield from _parse_attributes(post_data["attributes"])

    yield "campaign", included["campaign"].pop(campaign_id)
    yield from _parse_files(relationships, included)
    yield "relationships", relationships
    yield "included", included


def _parse_files(
    relationships: dict[str, dict[str, Any]], included: dict[str, dict[str, Any]]
) -> Generator[tuple[str, Any]]:
    def extract_files(files: dict[str, list[dict[str, str]]]):
        for file in files.get("data") or ():
            yield included[file["type"]].pop(file["id"])

    for key in (
        "images",
        "attachments",
        "attachments_media",
    ):
        files = relationships.pop(key, {})
        yield key, tuple(extract_files(files))


def _parse_attributes(attributes: dict[str, Any]) -> Generator[tuple[str, Any]]:
    json_string = "_json_string"
    json_keys = tuple(key for key in attributes if key.endswith(json_string))

    for json_key in json_keys:
        value = attributes.pop(json_key.removesuffix(json_string), None)
        json_value = attributes.pop(json_key, None)
        if not value and json_value:
            value = json.loads(json_value)

        yield json_key, value

    yield from attributes.items()


def _flatten_post(post: dict[str, Any]) -> dict[str, Any]:
    return dict(sorted(_parse_post(post)))


def _md5(url: AbsoluteHttpURL) -> str | None:
    for part in reversed(url.parts):
        if len(part) == 32:
            return part
