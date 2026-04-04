from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from cyberdrop_dl.crawlers.crawler import Crawler
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.utils import css, open_graph
from cyberdrop_dl.utils.utilities import error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.data_structures.url_objects import ScrapeItem


class Selector:
    UPLOAD_DATE = "span.hidden.sm\\:inline"
    ALBUM_FILES = "a[\\:href*='javascript:void(0)']", ":href"
    USER_FILES = "a.group.relative[href]"
    NEXT_PAGE = "a[rel=next]"
    ARCHIVE = "[x-data='archiveViewer()']"
    DIRECT_DL = "#embed-direct"


class ImagePondCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[dict[str, str | tuple[str, ...]]] = {
        "Image / Video / Archive": (
            "/i/<slug>",
            "/img/<slug>",
            "/image/<slug>",
            "/video/<slug>",
            "/videos/<slug>",
        ),
        "Album": "/a/<slug>",
        "User": (
            "/user/<user_name>",
            "/<user_name>",
        ),
        "Direct links": "/media/<slug>",
    }
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://imagepond.net")
    DOMAIN: ClassVar[str] = "imagepond.net"
    FOLDER_DOMAIN: ClassVar[str] = "ImagePond"
    DEFAULT_TRIM_URLS: ClassVar[bool] = False
    NEXT_PAGE_SELECTOR: ClassVar[str] = Selector.NEXT_PAGE

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["i" | "img" | "image" | "video" | "videos", _]:
                return await self.file(scrape_item)
            case ["a", _]:
                return await self.album(scrape_item)
            case ["media", _] if self.is_subdomain(scrape_item.url):
                return await self.direct_file(scrape_item)
            case ["user", user_name] | [user_name]:
                return await self.user(scrape_item, user_name)
            case _:
                raise ValueError

    @classmethod
    def transform_url(cls, url: AbsoluteHttpURL):
        url = super().transform_url(url)
        match url.parts[1:]:
            case [a, b, "download", *_]:
                return url.origin() / a / b
            case _:
                return url

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem) -> None:
        if await self.check_complete_from_referer(scrape_item):
            return

        async with self.request(scrape_item.url) as resp:
            if resp.url != scrape_item.url and await self.check_complete_from_referer(resp.url):
                return

            scrape_item.url = resp.url
            soup = await resp.soup()

        og = open_graph.parse(soup)

        if soup.select_one(Selector.ARCHIVE):
            # source = self.parse_url(scrape_item.url / "download/file")
            source = self.parse_url(css.select(soup, Selector.DIRECT_DL, "value"))
        else:
            source = self.parse_url(og.video or og.image)

        scrape_item.uploaded_at = self.parse_date(css.select_text(soup, Selector.UPLOAD_DATE), "%b %d, %Y")
        filename, ext = self.get_filename_and_ext(og.title, assume_ext=".jpg", mime_type=og.get("video_type"))
        await self.handle_file(source, scrape_item, og.title, ext, custom_filename=filename)

    @error_handling_wrapper
    async def album(self, scrape_item: ScrapeItem) -> None:
        title: str = ""
        async for soup in self.web_pager(scrape_item.url):
            if not title:
                title = css.select_text(soup, "h1")
                scrape_item.setup_as_album(self.create_title(title), album_id=scrape_item.url.name)

            for js in css.iselect(soup, *Selector.ALBUM_FILES):
                web_url = js[js.index("'http") :].strip("'")
                new_item = scrape_item.create_child(self.parse_url(web_url))
                self.create_task(self.run(new_item))
                scrape_item.add_children()

    @error_handling_wrapper
    async def user(self, scrape_item: ScrapeItem, user_name: str) -> None:
        title = self.create_title(f"{user_name} [user]")
        scrape_item.setup_as_profile(title)

        async for soup in self.web_pager(scrape_item.url):
            for _, new_item in self.iter_children(scrape_item, soup, Selector.USER_FILES):
                self.create_task(self.run(new_item))
