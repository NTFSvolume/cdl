from __future__ import annotations  #

from typing import TYPE_CHECKING, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import css, open_graph
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.data_structures.url_objects import ScrapeItem

_CDN_URL = AbsoluteHttpURL("https://cache1.filester.me")


class Selector:
    @staticmethod
    def file_attr(name: str) -> str:
        return f"#detailsContent span:-soup-contains({name}) + span"

    MIME_TYPE = file_attr("Type")
    SHA_256 = file_attr("SHA-256")
    UPLOAD_DATE = file_attr("Uploaded")


class FilesterCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "File": "/d/<file_id>",
    }
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://filester.me")
    DOMAIN: ClassVar[str] = "filester"

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["d", slug]:
                return await self.file(scrape_item, slug)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem, slug: str) -> None:
        if await self.check_complete_from_referer(scrape_item):
            return

        soup = await self.request_soup(scrape_item.url)
        checksum = css.select_text(soup, Selector.SHA_256)
        if await self.check_complete_by_hash(scrape_item, "sha256", checksum):
            return

        dl_link = await self._request_download(slug)
        name = open_graph.title(soup)
        mime_type = css.select_text(soup, Selector.MIME_TYPE)
        scrape_item.possible_datetime = self.parse_iso_date(css.select_text(soup, Selector.UPLOAD_DATE))
        filename, ext = self.get_filename_and_ext(name, mime_type=mime_type)
        await self.handle_file(dl_link, scrape_item, name, ext, custom_filename=filename)

    async def _request_download(self, slug: str) -> AbsoluteHttpURL:
        resp = await self.request_json(
            self.PRIMARY_URL / "api/public/download",
            method="POST",
            json={"file_slug": slug},
        )
        return _CDN_URL.with_path(resp["download_url"]).with_query(download="true")
