from __future__ import annotations

import asyncio
import dataclasses
import shutil
from typing import TYPE_CHECKING

import rich
from rich.layout import Layout

from cyberdrop_dl import env
from cyberdrop_dl.progress import LiveUI
from cyberdrop_dl.progress.scraping.downloads import DownloadsPanel
from cyberdrop_dl.progress.scraping.errors import DownloadErrorsPanel, ScrapeErrorsPanel
from cyberdrop_dl.progress.scraping.files import FileStatsPanel
from cyberdrop_dl.progress.scraping.panel import ScrapingPanel, StatusMessage

if TYPE_CHECKING:
    from collections.abc import Iterator

_PANEL_PADDING = 5


@dataclasses.dataclass(slots=True, frozen=True)
class Screen:
    horizontal: Layout
    vertical: Layout

    def __iter__(self) -> Iterator[Layout]:
        return iter((self.horizontal, self.vertical))

    def __rich__(self) -> Layout:
        return self.vertical if terminal_is_in_portrait() else self.horizontal


@dataclasses.dataclass(slots=True, frozen=True)
class ScrapingUI(LiveUI):
    files: FileStatsPanel = dataclasses.field(default_factory=FileStatsPanel)
    scrape_errors: ScrapeErrorsPanel = dataclasses.field(default_factory=ScrapeErrorsPanel)
    download_errors: DownloadErrorsPanel = dataclasses.field(default_factory=DownloadErrorsPanel)

    scrape: ScrapingPanel = dataclasses.field(default_factory=ScrapingPanel)
    downloads: DownloadsPanel = dataclasses.field(default_factory=DownloadsPanel)
    status: StatusMessage = dataclasses.field(default_factory=StatusMessage)
    _screen: Screen = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_screen", self._create_screen())

    def __rich__(self) -> Screen:
        return self._screen

    def _create_screen(self) -> Screen:
        horizontal, vertical = Layout(), Layout()

        top = (
            Layout(self.files, name="files"),
            Layout(self.scrape_errors, name="scrape_errors"),
            Layout(self.download_errors, name="download_errors"),
        )

        bottom = (
            Layout(self.scrape, name="scrape", size=self.scrape.max_rows + _PANEL_PADDING),
            Layout(self.downloads, name="downloads"),
            Layout(self.status, name="status", size=2),
        )

        horizontal.split_column(Layout(name="top", size=self.scrape_errors.max_rows + _PANEL_PADDING), *bottom)
        vertical.split_column(Layout(name="top", ratio=60), *bottom)

        horizontal["top"].split_row(*top)
        vertical["top"].split_column(*top)
        return Screen(horizontal, vertical)

    def hide_scrape_panel(self) -> None:
        for layout in self._screen:
            layout["scrape"].visible = False
            layout["downloads"].ratio = 2

        free_rows = self.scrape.max_rows + _PANEL_PADDING

        self.downloads.max_rows += free_rows
        for _ in range(free_rows):
            try:
                self.downloads._push_one_invisible()
            except IndexError:
                break

    async def simulate(self) -> None:

        try:
            async with asyncio.timeout(30):
                async with asyncio.TaskGroup() as tg:
                    for panel in (
                        self.files,
                        self.scrape_errors,
                        self.download_errors,
                        self.downloads,
                        self.status,
                    ):
                        _ = tg.create_task(panel.simulate())

                    async def scrape() -> None:
                        await self.scrape.simulate()
                        self.hide_scrape_panel()

                    _ = tg.create_task(scrape())

        except TimeoutError:
            pass


def terminal_is_in_portrait() -> bool:

    if env.PORTRAIT_MODE:
        return True

    terminal_size = shutil.get_terminal_size()
    width, height = terminal_size.columns, terminal_size.lines
    aspect_ratio = width / height

    # High aspect ratios are likely to be in landscape mode
    if aspect_ratio >= 3.2:
        return False

    # Check for mobile device in portrait mode
    if (aspect_ratio < 1.5 and height >= 40) or (aspect_ratio < 2.3 and width <= 85):
        return True

    # Assume landscape mode for other cases
    return False


if __name__ == "__main__":
    scrape_tui = ScrapingUI()
    rich.print(scrape_tui._screen.horizontal.tree)
    _ = input("press <Enter> to continue")

    with scrape_tui(transient=False):
        asyncio.run(scrape_tui.simulate())
