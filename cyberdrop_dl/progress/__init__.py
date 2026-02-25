from __future__ import annotations

import contextlib
import dataclasses
import logging
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Literal

from pydantic import ByteSize
from rich.live import Live
from rich.text import Text

from cyberdrop_dl import config
from cyberdrop_dl.progress.errors import DownloadErrors, ScrapeErrors
from cyberdrop_dl.progress.files import FileStats
from cyberdrop_dl.progress.hashing import HashingPanel
from cyberdrop_dl.progress.scrape import DownloadsPanel, ScrapingPanel, StatusMessage
from cyberdrop_dl.progress.screens import AppScreens
from cyberdrop_dl.progress.sorting import SortingPanel

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from rich.console import RenderableType

    from cyberdrop_dl.progress.errors import UIFailure


logger = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class ProgressManager:
    refresh_rate: int

    status: StatusMessage = dataclasses.field(init=False, default_factory=StatusMessage)
    downloads: DownloadsPanel = dataclasses.field(init=False, default_factory=DownloadsPanel)
    scrape: ScrapingPanel = dataclasses.field(init=False, default_factory=ScrapingPanel)
    hashing: HashingPanel = dataclasses.field(init=False, default_factory=HashingPanel)
    sorting: SortingPanel = dataclasses.field(init=False, default_factory=SortingPanel)

    files: FileStats = dataclasses.field(init=False, default_factory=FileStats)
    download_errors: DownloadErrors = dataclasses.field(init=False, default_factory=DownloadErrors)
    scrape_errors: ScrapeErrors = dataclasses.field(init=False, default_factory=ScrapeErrors)

    _screens: AppScreens = dataclasses.field(init=False)
    _live: Live = dataclasses.field(init=False)
    _current_screen: RenderableType = dataclasses.field(init=False, default="")

    def __post_init__(self) -> None:
        self._screens = AppScreens.build(self)
        self._live = Live(
            refresh_per_second=self.refresh_rate,
            transient=True,
            screen=True,
            auto_refresh=True,
            get_renderable=lambda: self._current_screen,
        )

    @contextlib.contextmanager
    def get_live(self, name: Literal["scraping", "sorting", "hashing"]) -> Generator[None]:
        self._current_screen = self._screens[name]
        self._live.start()
        try:
            yield
        finally:
            self._current_screen = ""
            self._live.stop()

    def print_stats(self, start_time: float) -> None:
        """Prints the stats of the program."""
        # if not self.manager.parsed_args.cli_only_args.print_stats:
        #    return
        from cyberdrop_dl.utils.logger import log_spacer

        end_time = time.perf_counter()
        runtime = timedelta(seconds=int(end_time - start_time))
        total_data_written = ByteSize(self.downloads.total_data_written).human_readable(decimal=True)

        log_spacer(20)
        logger.info("Printing Stats...\n")
        logger.info("Run Stats")
        logger.info(f"  Input File: {config.get().source}")
        # logger.info(f"  Input URLs: {self.manager.scrape_mapper.count:,}")
        # logger.info(f"  Input URL Groups: {self.manager.scrape_mapper.group_count:,}")
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

        last_padding = _log_failures(self.scrape_errors.return_totals(), "Scrape Failures:")
        _log_failures(self.download_errors.return_totals(), "Download Failures:", last_padding)

    def print_dedupe_stats(self) -> None:
        logger.info("Dupe Stats:")
        logger.info(f"  Newly Hashed: {self.hashing.hashed_files:,} files")
        logger.info(f"  Previously Hashed: {self.hashing.prev_hashed_files:,} files")
        logger.info(f"  Removed (Downloads): {self.hashing.removed_files:,} files")


def _log_failures(failures: list[UIFailure], title: str = "Failures:", last_padding: int = 0) -> int:
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
    return Text(str(show_text), style=f"link {full_path.as_uri()}")
