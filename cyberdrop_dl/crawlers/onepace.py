from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import dataclasses

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import next_js
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from bs4 import BeautifulSoup

    from cyberdrop_dl.data_structures.url_objects import ScrapeItem


_PREFER_DUB = False


@dataclasses.dataclass(slots=True, frozen=True)
class PlayGroup:
    sub: str | None
    dub: str
    playlists: list[Playlist]

    @property
    def score(self) -> tuple[int, int]:
        langs = ("ja", "en") if _PREFER_DUB else ("en", "ja")
        return langs.index(self.dub), (None, "en").index(self.sub)


@dataclasses.dataclass(slots=True, frozen=True)
class Playlist:
    id: str
    resolution: int


@dataclasses.dataclass(slots=True, frozen=True)
class Episode:
    slug: str
    playGroups: list[PlayGroup]  # noqa: N815


_PD_BASE = AbsoluteHttpURL("https://pixeldrain.com/l/")


class OnePaceCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {"All episodes": "videos"}
    DOMAIN: ClassVar[str] = "onepace.net"
    FOLDER_DOMAIN: ClassVar[str] = "OnePace"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://onepace.net")

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        return await self.all_episodes(scrape_item)

    @error_handling_wrapper
    async def all_episodes(self, scrape_item: ScrapeItem) -> None:
        soup = await self.request_soup(self.PRIMARY_URL / "en/watch")
        episodes = _extract_episodes(soup)

        await self.write_metadata(scrape_item, "one_pace_episodes", {"episodes": episodes})
        scrape_item.setup_as_profile(self.FOLDER_DOMAIN)
        for episode in episodes:
            self._episode(scrape_item.copy(), Episode(**episode))
            scrape_item.add_children()

    def _episode(self, scrape_item: ScrapeItem, ep: Episode) -> None:
        scrape_item.url = scrape_item.url.with_fragment(ep.slug)
        best_group = max(ep.playGroups, key=lambda x: x.score)
        self.log(f"Downloading {scrape_item.url} with subs={best_group.sub} and lang={best_group.dub}")
        best = max(best_group.playlists, key=lambda x: x.resolution)
        child = scrape_item.create_child(_PD_BASE / best.id)
        self.handle_external_links(child, reset=False)


def _extract_episodes(soup: BeautifulSoup) -> list[dict[str, Any]]:
    episodes: list[dict[str, Any]] = next_js.extract(soup)["5"][3]["data"]

    for ep in episodes:
        for bd in ep["backdrops"]:
            for key in bd:
                if key.startswith("blur"):
                    del bd[key]
        for group in ep["playGroups"]:
            for playlist in group["playlists"]:
                playlist["url"] = _PD_BASE / playlist["id"]

    return episodes
