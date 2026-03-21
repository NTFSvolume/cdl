from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Required, TypedDict

from cyberdrop_dl.crawlers.crawler import Crawler, SupportedPaths
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
from cyberdrop_dl.exceptions import ScrapeError
from cyberdrop_dl.utils.utilities import error_handling_wrapper, parse_url

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from cyberdrop_dl.data_structures.url_objects import ScrapeItem
    from cyberdrop_dl.utils.dates import TimeStamp

# Primary URL needs `www.` to prevent redirect
PRIMARY_URL = AbsoluteHttpURL("https://www.redgifs.com/")
API_ENTRYPOINT = AbsoluteHttpURL("https://api.redgifs.com/v2")


class Links(TypedDict, total=False):
    sd: Required[str]
    hd: str


@dataclasses.dataclass(frozen=True, slots=True)
class Gif:
    id: str
    urls: Links
    date: TimeStamp
    url: AbsoluteHttpURL
    title: str | None = None

    @staticmethod
    def from_dict(gif: dict[str, Any]) -> Gif:
        urls: Links = gif["urls"]
        url = parse_url(urls.get("hd") or urls["sd"], relative_to=PRIMARY_URL)
        return Gif(gif["id"], urls, gif["createDate"], url, gif.get("title"))


class RedGifsCrawler(Crawler):
    SUPPORTED_PATHS: ClassVar[SupportedPaths] = {
        "User": "/users/<user>",
        "Gif": "/watch/<gif_id>",
        "Image": "/i/<image_id>",
        "Embeds": "/ifr/<gif_id>",
    }
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = PRIMARY_URL
    DOMAIN: ClassVar[str] = "redgifs"
    FOLDER_DOMAIN: ClassVar[str] = "RedGifs"

    @classmethod
    def _json_response_check(cls, json_resp: Any) -> None:
        if error := json_resp.get("error"):
            raise ScrapeError(422, error["message"])

    def __post_init__(self) -> None:
        self.headers: dict[str, str] = {}

    async def async_startup(self) -> None:
        await self.get_auth_token(API_ENTRYPOINT / "auth/temporary")

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        match scrape_item.url.parts[1:]:
            case ["users", user_name]:
                return await self.user(scrape_item, _id(user_name))
            case ["i" | "watch" | "ifr", gif_id]:
                return await self.gif(scrape_item, _id(gif_id))
            case [_, _] if self.is_self_subdomain(scrape_item.url):
                scrape_item.url = _canonical_url(scrape_item.url.name)
                return await self.gif(scrape_item, scrape_item.url.name)
            case _:
                raise ValueError

    @error_handling_wrapper
    async def user(self, scrape_item: ScrapeItem, user_id: str) -> None:
        title = self.create_title(user_id)
        scrape_item.setup_as_album(title)

        async for gifs in self._profile_pager(user_id, init_page=int(scrape_item.url.query.get("page", 1))):
            for gif in gifs:
                new_scrape_item = scrape_item.create_child(_canonical_url(gif.id))
                await self._handle_gif(new_scrape_item, gif)
                scrape_item.add_children()

    async def _profile_pager(self, user_id: str, init_page: int = 1) -> AsyncGenerator[list[Gif]]:
        total_gifs: int = 0
        gif_ids: set[str] = set()
        page_limit = 100
        page_count = 100

        api_url = (API_ENTRYPOINT / "users" / user_id / "search").with_query(count=page_count)

        async def request_gifs(order: Literal["new", "old"], page: int) -> list[Gif]:
            nonlocal total_gifs
            resp: dict[str, Any] = await self.request_json(
                api_url.update_query(order=order, page=page),
                headers=self.headers,
            )
            if not total_gifs:
                total_gifs = resp["users"][0]["gifs"]

            return [Gif.from_dict(gif) for gif in resp["gifs"]]

        for page in range(init_page, page_limit + 1):
            gifs = await request_gifs("new", page)
            gif_ids.update(gif.id for gif in gifs)
            yield gifs

            if len(gif_ids) >= total_gifs:
                return

        # fetch gifs in reverse order to bypass API limit
        for page in range(1, page_limit + 1):
            gifs = [gif for gif in await request_gifs("old", page) if gif.id not in gif_ids]
            gif_ids.update(gif.id for gif in gifs)
            yield gifs

            if len(gifs) < page_count:
                break

    @error_handling_wrapper
    async def gif(self, scrape_item: ScrapeItem, post_id: str) -> None:
        canonical_url = _canonical_url(post_id)
        if await self.check_complete_from_referer(canonical_url):
            return

        scrape_item.url = canonical_url
        api_url = API_ENTRYPOINT / "gifs" / post_id
        resp: dict[str, dict[str, Any]] = await self.request_json(api_url, headers=self.headers)
        gif = Gif.from_dict(resp["gif"])
        if gif.title:
            scrape_item.setup_as_album(self.create_title(gif.title))
        await self._handle_gif(scrape_item, gif)

    async def _handle_gif(self, scrape_item: ScrapeItem, gif: Gif) -> None:
        scrape_item.possible_datetime = gif.date
        filename, ext = self.get_filename_and_ext(gif.url.name)
        await self.handle_file(gif.url, scrape_item, filename, ext)

    @error_handling_wrapper
    async def get_auth_token(self, token_url: AbsoluteHttpURL) -> None:
        json_obj: dict[str, Any] = await self.request_json(token_url)
        token: str = json_obj["token"]
        self.headers = {"Authorization": f"Bearer {token}"}


def _id(name: str) -> str:
    # PaleturquoiseLostStickinsect-mobile.m4s -> paleturquoiseLoststickinsect
    # Id needs to be lower case for requests to the api, but final files (media.redgifs) need each word capitalized
    return name.lower().split(".", 1)[0].split("-", 1)[0]


def _canonical_url(name_or_id: str) -> AbsoluteHttpURL:
    return PRIMARY_URL / "watch" / _id(name_or_id)


def fix_db_referer(referer: str) -> str:
    url = AbsoluteHttpURL(referer)
    name = url.name or url.parent.name
    return str(_canonical_url(name))
