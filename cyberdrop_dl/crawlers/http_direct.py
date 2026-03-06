from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from typing_extensions import override

from cyberdrop_dl import constants
from cyberdrop_dl.crawlers import Crawler
from cyberdrop_dl.exceptions import NoExtensionError, ScrapeError
from cyberdrop_dl.utils.filepath import get_filename_and_ext

if TYPE_CHECKING:
    from cyberdrop_dl.data_structures import ScrapeItem


class DirectHTTPFile(Crawler, is_generic=True):
    DOMAIN: ClassVar[str] = "no_crawler"

    @override
    async def ready(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        if not self._ready:
            self._ready = True
        return self._ready

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        try:
            filename, ext = get_filename_and_ext(scrape_item.url.name)
        except NoExtensionError:
            filename, ext = get_filename_and_ext(scrape_item.url.name, xenforo=True)

        if ext not in constants.FileExt.MEDIA:
            raise ValueError

        scrape_item.add_to_parent_title("Loose Files")
        scrape_item.part_of_album = True
        await self.handle_file(
            scrape_item.url,
            scrape_item,
            scrape_item.url.name,
            ext,
            custom_filename=filename,
        )

    @override
    def handle_error(self, scrape_item: ScrapeItem, exc: type[Exception] | Exception | str) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        if isinstance(exc, str):
            exc = ScrapeError(exc)
        raise exc
