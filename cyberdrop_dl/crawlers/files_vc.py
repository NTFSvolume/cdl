from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import error_handling_wrapper

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from cyberdrop_dl.data_structures.url_objects import ScrapeItem


API_ENTRYPOINT = AbsoluteHttpURL("https://api.files.vc/api")

PRIMARY_URL = AbsoluteHttpURL("https://files.vc")


class FilesVcCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {"Direct links": ""}
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = PRIMARY_URL
    DOMAIN: ClassVar[str] = "files.vc"
    FOLDER_DOMAIN: ClassVar[str] = "FilesVC"
    _RATE_LIMIT = 1, 1

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        if scrape_item.url.path == "/d/dl" and scrape_item.url.query.get("hash"):
            return await self.file(scrape_item)
        raise ValueError

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem) -> None:
        if await self.check_complete_from_referer(scrape_item):
            return

        hash = scrape_item.url.query["hash"]
        api_url = API_ENTRYPOINT.joinpath("info").with_query(hash=hash)

        json_resp: dict[str, Any] = await self.request_json(api_url)

        filename, ext = self.get_filename_and_ext(json_resp["filename"], assume_ext=".zip")
        scrape_item.timestamp = self.parse_date(json_resp["upload_time"])
        link = self.parse_url(json_resp["file_url"])
        await self.handle_file(link, scrape_item, filename, ext)
