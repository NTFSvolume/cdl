from __future__ import annotations

import dataclasses
import logging
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Self

from pydantic import ByteSize
from rich.columns import Columns
from rich.console import Group
from rich.layout import Layout
from rich.progress import Progress, SpinnerColumn
from rich.text import Text
from yarl import URL

from cyberdrop_dl import __version__, config
from cyberdrop_dl.progress.downloads_progress import DownloadsProgress
from cyberdrop_dl.progress.errors import DownloadErrors, ScrapeErrors
from cyberdrop_dl.progress.hash_progress import HashProgress
from cyberdrop_dl.progress.panels import DownloadsPanel, ScrapingPanel
from cyberdrop_dl.progress.sorting import SortingPanel
from cyberdrop_dl.utils.logger import log_spacer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.progress.errors import UIFailure


spinner = SpinnerColumn(style="green", spinner_name="dots")
logger = logging.getLogger(__name__)


class StatusMessage:
    def __init__(self) -> None:
        self.progress: Progress = Progress(spinner, "[progress.description]{task.description}")
        self._task_id = self.progress.add_task("", total=100, completed=0, visible=False)

    def update(self, description: str | None = None) -> None:
        self.progress.update(self._task_id, description=description, visible=bool(description))

    @property
    def msg(self) -> str:
        return self.progress._tasks[self._task_id].description

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self.msg!r})"


@dataclasses.dataclass(slots=True)
class UILayouts:
    horizontal: Layout
    vertical: Layout
    simple: Group

    @classmethod
    def build(cls, progress: ProgressManager) -> Self:
        horizontal = Layout()
        vertical = Layout()

        activity = Progress(spinner, "[progress.description]{task.description}")
        _ = activity.add_task(f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0)

        upper_layouts = (
            Layout(progress.download_progress, name="Files", ratio=1, minimum_size=9),
            Layout(progress.scrape_stats_progress, name="Scrape Failures", ratio=1),
            Layout(progress.download_stats_progress, name="Download Failures", ratio=1),
        )

        lower_layouts = (
            Layout(progress.scraping_progress, name=progress.scraping_progress.title, ratio=20),
            Layout(progress.file_progress, name=progress.file_progress.title, ratio=20),
            Layout(Columns([activity, progress.status.progress]), name="status_message", ratio=2),
        )

        horizontal.split_column(Layout(name="upper", ratio=20), *lower_layouts)
        vertical.split_column(Layout(name="upper", ratio=60), *lower_layouts)

        horizontal["upper"].split_row(*upper_layouts)
        vertical["upper"].split_column(*upper_layouts)

        simple = Group(activity, progress.download_progress.simple_progress)
        return cls(horizontal, vertical, simple)


class ProgressManager:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager

        self.portrait = True
        self.file_progress = DownloadsPanel()
        self.scraping_progress = ScrapingPanel()
        self.status = StatusMessage()

        self.download_progress: DownloadsProgress = DownloadsProgress()
        self.download_stats_progress: DownloadErrors = DownloadErrors()
        self.scrape_stats_progress: ScrapeErrors = ScrapeErrors()
        self.hash_progress: HashProgress = HashProgress(manager)
        self.sorting: SortingPanel = SortingPanel(1)

        self.layouts = UILayouts.build(self)
        self.hash_remove_layout = self.hash_progress.get_removed_progress()
        self.hash_layout = self.hash_progress.get_renderable()
        self.sort_layout = self.sorting.get_renderable()

    @asynccontextmanager
    async def show_status_msg(self, msg: str | None) -> AsyncGenerator[None]:
        try:
            self.status.update(msg)
            yield
        finally:
            self.status.update()

    @property
    def layout(self) -> Layout:
        if self.portrait:
            return self.layouts.vertical
        return self.layouts.horizontal

    def print_stats(self, start_time: float) -> None:
        """Prints the stats of the program."""
        # if not self.manager.parsed_args.cli_only_args.print_stats:
        #    return
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
        logger.info(f"  Downloaded: {self.download_progress.completed_files:,} files")
        logger.info(f"  Skipped (By Config): {self.download_progress.skipped_files:,} files")
        logger.info(f"  Skipped (Previously Downloaded): {self.download_progress.previously_completed:,} files")
        logger.info(f"  Failed: {self.download_stats_progress.failed_files:,} files")

        logger.info("Unsupported URLs Stats:")
        logger.info(f"  Sent to Jdownloader: {self.scrape_stats_progress.sent_to_jdownloader:,}")
        logger.info(f"  Skipped: {self.scrape_stats_progress.unsupported_urls_skipped:,}")

        self.print_dedupe_stats()

        logger.info("Sort Stats:")
        logger.info(f"  Audios: {self.sorting.audio_count:,}")
        logger.info(f"  Images: {self.sorting.image_count:,}")
        logger.info(f"  Videos: {self.sorting.video_count:,}")
        logger.info(f"  Other Files: {self.sorting.other_count:,}")

        last_padding = log_failures(self.scrape_stats_progress.return_totals(), "Scrape Failures:")
        log_failures(self.download_stats_progress.return_totals(), "Download Failures:", last_padding)

    def print_dedupe_stats(self) -> None:
        logger.info("Dupe Stats:")
        logger.info(f"  Newly Hashed: {self.hash_progress.hashed_files:,} files")
        logger.info(f"  Previously Hashed: {self.hash_progress.prev_hashed_files:,} files")
        logger.info(f"  Removed (Downloads): {self.hash_progress.removed_files:,} files")


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
