from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedDomains, SupportedPaths
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import css, jsunpacker
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import Generator

    from cyberdrop_dl.data_structures.url_objects import ScrapeItem


class MixDropCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "File": (
            "/e/<file_id>",
            "/f/<file_id>",
        )
    }
    SUPPORTED_DOMAINS: ClassVar[SupportedDomains] = "mxdrop", "mixdrop", "m1xdrop"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://mixdrop.sb")
    DOMAIN: ClassVar[str] = "mixdrop"
    FOLDER_DOMAIN: ClassVar[str] = "MixDrop"

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["f" | "e", file_id]:
                return await self.file(scrape_item, file_id)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem, file_id: str) -> None:
        video_url = MixDropCrawler.PRIMARY_URL / "f" / file_id

        if await self.check_complete(video_url, video_url):
            return

        scrape_item.url = video_url
        soup = await self.request_soup(video_url)
        title = css.select_text(soup, "div.tbl-c.title b")
        link = await self._request_download(file_id)
        filename, ext = self.get_filename_and_ext(title)
        await self.handle_file(video_url, scrape_item, title, ext, custom_filename=filename, debrid_link=link)

    async def _request_download(self, file_id: str) -> AbsoluteHttpURL:
        embed_url = MixDropCrawler.PRIMARY_URL / "e" / file_id
        html = await self.request_text(embed_url)
        info = dict(_extract_info(html))
        return self.parse_url(info["wurl"])


def _extract_info(html: str) -> Generator[tuple[str, str]]:
    content = jsunpacker.unpack(html)
    for line in content.split(";MDCore."):
        name, _, value = line.partition("=")
        yield name.removeprefix("MDCore."), value.strip('"').strip()
