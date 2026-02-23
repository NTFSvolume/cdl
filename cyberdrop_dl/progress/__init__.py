from __future__ import annotations

import dataclasses
import logging
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from pydantic import ByteSize
from rich.columns import Columns
from rich.console import Group, RenderableType
from rich.layout import Layout
from rich.progress import Progress, SpinnerColumn
from rich.text import Text
from yarl import URL

from cyberdrop_dl import __version__, config
from cyberdrop_dl.progress._common import ProgressProxy
from cyberdrop_dl.progress.errors import DownloadErrors, ScrapeErrors
from cyberdrop_dl.progress.files import FileStats
from cyberdrop_dl.progress.hashing import HashingPanel
from cyberdrop_dl.progress.scrape import DownloadsPanel, ScrapingPanel
from cyberdrop_dl.progress.sorting import SortingPanel

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.progress.errors import UIFailure


logger = logging.getLogger(__name__)


class StatusMessage(ProgressProxy):
    _columns = (
        SpinnerColumn(style="green", spinner_name="dots"),
        "[progress.description]{task.description}",
    )

    def __init__(self) -> None:
        super().__init__()
        self.activity = Progress(*self._columns)
        _ = self.activity.add_task(f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0)
        self._task_id = self._progress.add_task("", total=100, completed=0, visible=False)
        self._panel = Columns([self.activity, self._progress])

    def __rich__(self) -> Columns:
        return self._panel

    def update(self, description: str | None = None) -> None:
        self._progress.update(self._task_id, description=description, visible=bool(description))

    def __str__(self) -> str:
        return self._tasks[self._task_id].description

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self!s})"

    @asynccontextmanager
    async def show(self, msg: str | None) -> AsyncGenerator[None]:
        try:
            self.update(msg)
            yield
        finally:
            self.update()


@dataclasses.dataclass(slots=True, frozen=True)
class UILayouts:
    horizontal: Layout
    vertical: Layout
    simple: Group
    hashing: RenderableType
    sorting: RenderableType

    @classmethod
    def build(cls, progress: ProgressManager) -> Self:
        horizontal = Layout()
        vertical = Layout()

        top = (
            Layout(progress.files, ratio=1, minimum_size=9),
            Layout(progress.scrape_errors, ratio=1),
            Layout(progress.download_errors, ratio=1),
        )

        bottom = (
            Layout(progress.scrape, ratio=20),
            Layout(progress.downloads, ratio=20),
            Layout(progress.status, ratio=2),
        )

        horizontal.split_column(Layout(name="top", ratio=20), *bottom)
        vertical.split_column(Layout(name="top", ratio=60), *bottom)

        horizontal["top"].split_row(*top)
        vertical["top"].split_column(*top)

        simple = Group(progress.status.activity, progress.files.simple)
        return cls(horizontal, vertical, simple, progress.hashing, progress.sorting)


@dataclasses.dataclass(slots=True)
class ProgressManager:
    manager: Manager

    portrait: bool

    layouts: UILayouts = dataclasses.field(init=False)
    status: StatusMessage = dataclasses.field(default_factory=StatusMessage)

    downloads: DownloadsPanel = dataclasses.field(default_factory=DownloadsPanel)
    scrape: ScrapingPanel = dataclasses.field(default_factory=ScrapingPanel)
    hashing: HashingPanel = dataclasses.field(default_factory=HashingPanel)
    sorting: SortingPanel = dataclasses.field(default_factory=SortingPanel)

    files: FileStats = dataclasses.field(default_factory=FileStats)
    download_errors: DownloadErrors = dataclasses.field(default_factory=DownloadErrors)
    scrape_errors: ScrapeErrors = dataclasses.field(default_factory=ScrapeErrors)

    def __post_init__(self) -> None:
        self.layouts = UILayouts.build(self)

    @property
    def layout(self) -> Layout:
        if self.portrait:
            return self.layouts.vertical
        return self.layouts.horizontal

    def print_stats(self, start_time: float) -> None:
        """Prints the stats of the program."""
        # if not self.manager.parsed_args.cli_only_args.print_stats:
        #    return
        from cyberdrop_dl.utils.logger import log_spacer

        end_time = time.perf_counter()
        runtime = timedelta(seconds=int(end_time - start_time))
        total_data_written = ByteSize(self.manager.storage_manager.total_data_written).human_readable(decimal=True)

        log_spacer(20)
        logger.info("Printing Stats...\n")
        logger.info("Run Stats")
        logger.info(f"  Input File: {config.get().source}")
        logger.info(f"  Input URLs: {self.manager.scrape_mapper.count:,}")
        logger.info(f"  Input URL Groups: {self.manager.scrape_mapper.group_count:,}")
        # logger.info(f"  Log Folder: {log_folder_text}")
        logger.info(f"  Total Runtime: {runtime}")
        logger.info(f"  Total Downloaded Data: {total_data_written}")

        logger.info("Download Stats:")
        logger.info(f"  Downloaded: {self.files.completed_files:,} files")
        logger.info(f"  Skipped (By Config): {self.files.skipped_files:,} files")
        logger.info(f"  Skipped (Previously Downloaded): {self.files.previously_completed:,} files")
        logger.info(f"  Failed: {self.download_errors.failed_files:,} files")

        logger.info("Unsupported URLs Stats:")
        logger.info(f"  Sent to Jdownloader: {self.scrape_errors.sent_to_jdownloader:,}")
        logger.info(f"  Skipped: {self.scrape_errors.unsupported_urls_skipped:,}")

        self.print_dedupe_stats()

        logger.info("Sort Stats:")
        logger.info(f"  Audios: {self.sorting.audio_count:,}")
        logger.info(f"  Images: {self.sorting.image_count:,}")
        logger.info(f"  Videos: {self.sorting.video_count:,}")
        logger.info(f"  Other Files: {self.sorting.other_count:,}")

        last_padding = log_failures(self.scrape_errors.return_totals(), "Scrape Failures:")
        log_failures(self.download_errors.return_totals(), "Download Failures:", last_padding)

    def print_dedupe_stats(self) -> None:
        logger.info("Dupe Stats:")
        logger.info(f"  Newly Hashed: {self.hashing.hashed_files:,} files")
        logger.info(f"  Previously Hashed: {self.hashing.prev_hashed_files:,} files")
        logger.info(f"  Removed (Downloads): {self.hashing.removed_files:,} files")


def log_failures(failures: list[UIFailure], title: str = "Failures:", last_padding: int = 0) -> int:
    logger.info(title)
    if not failures:
        logger.info("  None")
        return 0
    error_padding = last_padding
    error_codes = [f.error_code for f in failures if f.error_code is not None]
    if error_codes:
        error_padding = max(len(str(max(error_codes))), error_padding)
    for f in failures:
        error = f.error_code if f.error_code is not None else ""
        logger.info(f"  {error:>{error_padding}}{' ' if error_padding else ''}{f.msg}: {f.total:,}")
    return error_padding


def _get_console_hyperlink(file_path: Path, text: str = "") -> Text:
    full_path = file_path
    show_text = text or full_path
    file_url = URL(full_path.as_posix()).with_scheme("file")
    return Text(str(show_text), style=f"link {file_url}")
