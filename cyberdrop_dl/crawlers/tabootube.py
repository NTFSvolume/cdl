from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from cyberdrop_dl.crawlers._kvs import KernelVideoSharingCrawler
from cyberdrop_dl.url_objects import AbsoluteHttpURL, ScrapeItem

if TYPE_CHECKING:
    from cyberdrop_dl.crawlers.crawler import SupportedPaths


class TabooTubeCrawler(KernelVideoSharingCrawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {"Video": "/video/..."}
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://www.tabootube.xxx")
    DOMAIN: ClassVar[str] = "tabootube"
    FOLDER_DOMAIN: ClassVar[str] = "TabooTube"

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["video", *_]:
                return await self.video(scrape_item)
            case _:
                raise ValueError
