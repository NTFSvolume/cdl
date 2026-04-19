from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, ClassVar

from cyberdrop_dl.crawlers._kvs import KernelVideoSharingCrawler
from cyberdrop_dl.exceptions import DownloadError
from cyberdrop_dl.url_objects import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.utils import css, error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.crawlers.crawler import SupportedPaths
    from cyberdrop_dl.url_objects import ScrapeItem


class Selector:
    MEMBER_NAME = "div.channel_logo > h2.title"
    MODEL_NAME = ".brand_inform > .title"
    TAG_NAME = "h1.title"
    TITLE = ", ".join((MEMBER_NAME, MODEL_NAME, TAG_NAME))

    THUMBS = "div.item.thumb > a.th"
    NEXT_PAGE = "div.item.pager.next > a"


class Rule34VideoCrawler(KernelVideoSharingCrawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "Members": "/members/...",
        "Models": "/models/...",
        "Search": "/search/...",
        "Tags": "/tags/...",
        "Video": (
            "/video/<id>/<name>",
            "/videos/<id>/<name>",
        ),
    }
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://rule34video.com/")
    NEXT_PAGE_SELECTOR: ClassVar[str] = Selector.NEXT_PAGE
    DOMAIN: ClassVar[str] = "rule34video"
    FOLDER_DOMAIN: ClassVar[str] = "Rule34Video"

    async def __async_post_init__(self) -> None:
        self.update_cookies(
            {
                "kt_rt_popAccess": 1,
                "kt_tcookie": 1,
            }
        )

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["video" | "videos", _, *_]:
                return await self.video(scrape_item)
            case ["tags" | "search" | "categories" | "members" | "models" as type_, _, *_]:
                return await self.collection(scrape_item, type_)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def collection(self, scrape_item: ScrapeItem, type_: str) -> None:
        soup = await self.request_soup(scrape_item.url)
        title = css.select_text(soup, Selector.TITLE, decompose="span")
        for trash in ("Videos for: ", "Tagged with "):
            title = title.removeprefix(trash)

        title = self.create_title(f"{title} [{type_}]")
        scrape_item.setup_as_album(title)

        for _, new_scrape_item in self.iter_children(scrape_item, soup, Selector.THUMBS):
            self.create_task(self.run(new_scrape_item))

        async for soup in self._ajax_pagination(
            scrape_item.url,
            block_id="list_videos_uploaded_videos",
            from_param_name="from_videos",
        ):
            for _, new_scrape_item in self.iter_children(scrape_item, soup, Selector.THUMBS):
                self.create_task(self.run(new_scrape_item))

    async def _ajax_pagination(
        self,
        url: AbsoluteHttpURL,
        block_id: str,
        *,
        last_page: int | None = None,
        mode: str = "async",
        function: str = "get_block",
        is_private: int = 0,
        sort_by: str = "",
        from_param_name: str = "from",
        **kwargs: int | str,
    ):
        page_url = url.with_query(
            mode=mode,
            function=function,
            block_id=block_id,
            is_private=is_private,
            sort_by=sort_by,
        )
        if kwargs:
            page_url = page_url.update_query(kwargs)

        for page in itertools.count(2):
            if last_page is not None and page > last_page:
                break
            page_url = page_url.update_query({from_param_name: page})
            try:
                soup = await self.request_soup(page_url)
            except DownloadError as e:
                if e.status == 404:
                    break
                raise

            yield soup
