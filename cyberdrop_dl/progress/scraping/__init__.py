import asyncio
import dataclasses
import shutil

from rich.layout import Layout

from cyberdrop_dl import env
from cyberdrop_dl.progress import create_live
from cyberdrop_dl.progress.scraping.downloads import DownloadsPanel
from cyberdrop_dl.progress.scraping.errors import DownloadErrorsPanel, ScrapeErrorsPanel
from cyberdrop_dl.progress.scraping.files import FileStatsPanel
from cyberdrop_dl.progress.scraping.panel import ScrapingPanel, StatusMessage


@dataclasses.dataclass(slots=True, frozen=True)
class Screen:
    horizontal: Layout
    vertical: Layout

    def __rich__(self) -> Layout:
        return self.vertical if is_terminal_in_portrait() else self.horizontal


@dataclasses.dataclass(slots=True, frozen=True)
class ScrapingUI:
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
        horizontal = Layout()
        vertical = Layout()
        top = (
            Layout(self.files, ratio=1, minimum_size=9),
            Layout(self.scrape_errors, ratio=1),
            Layout(self.download_errors, ratio=1),
        )

        bottom = (
            Layout(self.scrape, ratio=20),
            Layout(self.downloads, ratio=20),
            Layout(self.status, ratio=2),
        )

        horizontal.split_column(Layout(name="top", ratio=20), *bottom)
        vertical.split_column(Layout(name="top", ratio=60), *bottom)

        horizontal["top"].split_row(*top)
        vertical["top"].split_column(*top)

        return Screen(horizontal, vertical)

    async def simulate(self) -> None:
        try:
            async with asyncio.timeout(20):
                async with asyncio.TaskGroup() as tg:
                    for panel in (
                        self.files,
                        self.scrape_errors,
                        self.download_errors,
                        self.scrape,
                        self.downloads,
                        self.status,
                    ):
                        _ = tg.create_task(panel.simulate())
        except TimeoutError:
            pass


def is_terminal_in_portrait() -> bool:
    """Check if CDL is being run in portrait mode based on a few conditions."""

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
    ui = ScrapingUI()
    with create_live(ui):
        asyncio.run(ui.simulate())
