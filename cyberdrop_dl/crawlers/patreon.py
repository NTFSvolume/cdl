from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import css, error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import Generator

    from bs4 import BeautifulSoup

    from cyberdrop_dl.url_objects import ScrapeItem


class PatreonCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "Post": "/posts/<slug>",
    }

    DOMAIN: ClassVar[str] = "patreon"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://www.patreon.com")

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["posts", _]:
                return await self._post(scrape_item)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def _post(self, scrape_item: ScrapeItem) -> None:
        soup = await self.request_soup(scrape_item.url, impersonate=True)
        bootstrap = _extract_bootstrap(soup)
        post = _flatten_post(bootstrap["post"])
        if post:
            _path = Path("post.json")
            pass
            # _path.write_text(json.dumps(post, indent=2, ensure_ascii=False))


def _extract_bootstrap(soup: BeautifulSoup) -> dict[str, Any]:
    data = json.loads(css.select_text(soup, "#__NEXT_DATA__"))
    envelope = data["props"]["pageProps"]["bootstrapEnvelope"]
    return envelope.get("pageBootstrap") or envelope["bootstrap"]


def _flatten_included(included: dict[str, Any]) -> dict[str, dict[str, Any]]:
    flatten = {}
    for file in included:
        flatten.setdefault(file["type"], {})[file["id"]] = file["attributes"]
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
    json_keys = tuple(key.removesuffix(json_string) for key in attributes if key.endswith(json_string))

    for key in json_keys:
        value = attributes.pop(key, None)
        json_value = attributes.pop(key + json_string, None)
        if not value and json_value:
            value = json.loads(json_value)

        yield key, value

    yield from attributes.items()


def _flatten_post(post: dict[str, Any]) -> dict[str, Any]:
    return dict(sorted(_parse_post(post)))
