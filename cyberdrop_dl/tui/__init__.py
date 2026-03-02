from __future__ import annotations

import contextlib
import dataclasses
import itertools
import logging
import time
from contextvars import ContextVar
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Self

from pydantic import ByteSize
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.text import Text

from cyberdrop_dl.constants import HashAlgorithm
from cyberdrop_dl.logger import spacer
from cyberdrop_dl.tui.errors import DownloadErrors, ScrapeErrors, UIFailure
from cyberdrop_dl.tui.files import FileStatsPanel
from cyberdrop_dl.tui.hashing import HashingPanel
from cyberdrop_dl.tui.scrape import DownloadsPanel, ScrapingPanel, StatusMessage
from cyberdrop_dl.tui.screens import AppScreens, Screen
from cyberdrop_dl.tui.sorting import SortingPanel

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from cyberdrop_dl.config import Config


logger = logging.getLogger(__name__)

_tui: ContextVar[TUI] = ContextVar("_tui")


@dataclasses.dataclass(slots=True)
class TUI:
    refresh_rate: int = 10
    enabled_hashes: tuple[HashAlgorithm, ...] = (HashAlgorithm.xxh128,)
    disabled: bool = False

    # main scraping panels
    files: FileStatsPanel = dataclasses.field(init=False, default_factory=FileStatsPanel)
    download_errors: DownloadErrors = dataclasses.field(init=False, default_factory=DownloadErrors)
    scrape_errors: ScrapeErrors = dataclasses.field(init=False, default_factory=ScrapeErrors)
    scrape: ScrapingPanel = dataclasses.field(init=False, default_factory=ScrapingPanel)
    downloads: DownloadsPanel = dataclasses.field(init=False, default_factory=DownloadsPanel)
    status: StatusMessage = dataclasses.field(init=False, default_factory=StatusMessage)

    # Alternative screens
    sorting: SortingPanel = dataclasses.field(init=False, default_factory=SortingPanel)
    hashing: HashingPanel = dataclasses.field(init=False)

    _screens: AppScreens = dataclasses.field(init=False)
    _live: Live = dataclasses.field(init=False)
    _current_screen: Screen | Literal[""] = dataclasses.field(init=False, default="")

    @classmethod
    def from_config(cls, config: Config) -> Self:
        return cls(
            refresh_rate=config.ui_options.refresh_rate,
            enabled_hashes=config.dedupe.hashes,
        )

    def __post_init__(self) -> None:
        self.hashing = HashingPanel(*self.enabled_hashes)
        self._screens = _create_screens(self)
        self._live = Live(
            refresh_per_second=self.refresh_rate,
            transient=True,
            screen=True,
            auto_refresh=True,
            get_renderable=lambda: self._current_screen,
        )

    @contextlib.contextmanager
    def __call__(self, *, screen: Literal["scraping", "sorting", "hashing"]) -> Generator[None]:
        token = _tui.set(self)
        if not self.disabled:
            self._current_screen = self._screens[screen]
            self._live.start()
        try:
            yield
        finally:
            _tui.reset(token)
            if not self.disabled:
                self._current_screen = ""
                self._live.stop()

    def print_stats(self, start_time: float) -> None:
        """Prints the stats of the program."""
        # if not self.manager.parsed_args.cli_only_args.print_stats:
        #    return

        runtime = timedelta(seconds=int(time.monotonic() - start_time))
        total_data_written = ByteSize(self.downloads.total_data_written).human_readable(decimal=True)

        logger.info(spacer())
        logger.info("Printing Stats...\n")
        logger.info("Run Stats")
        # logger.info(f"  Input File: {config.get().source}")
        # logger.info(f"  Input URLs: {self.manager.scrape_mapper.count:,}")
        # logger.info(f"  Input URL Groups: {self.manager.scrape_mapper.group_count:,}")
        # logger.info(f"  Log Folder: {log_folder_text}")
        logger.info(f"  Total Runtime: {runtime}")
        logger.info(f"  Total Downloaded Data: {total_data_written}")

        logger.info("Download Stats:")
        logger.info(f"  Downloaded: {self.files.completed_files:,} files")
        logger.info(f"  Skipped (By Config): {self.files.skipped_files:,} files")
        logger.info(f"  Skipped (Previously Downloaded): {self.files.previously_completed:,} files")
        logger.info(f"  Failed: {self.download_errors._error_count:,} files")

        logger.info("Unsupported URLs Stats:")
        logger.info(f"  Sent to Jdownloader: {self.scrape_errors._sent_to_jdownloader:,}")
        logger.info(f"  Skipped: {self.scrape_errors._skipped:,}")

        self.print_dedupe_stats()

        logger.info("Sort Stats:")
        logger.info(f"  Audios: {self.sorting.audio_count:,}")
        logger.info(f"  Images: {self.sorting.image_count:,}")
        logger.info(f"  Videos: {self.sorting.video_count:,}")
        logger.info(f"  Other Files: {self.sorting.other_count:,}")

        _log_errors(self.scrape_errors.results(), self.download_errors.results())

    def print_dedupe_stats(self) -> None:
        logger.info("Dupe Stats:")
        logger.info(f"  Newly Hashed: {self.hashing.hashed_files:,} files")
        logger.info(f"  Previously Hashed: {self.hashing.prev_hashed_files:,} files")
        logger.info(f"  Removed (Downloads): {self.hashing.removed_files:,} files")


def _create_screens(tui: TUI) -> AppScreens:
    horizontal = Layout()
    vertical = Layout()
    top = (
        Layout(tui.files, ratio=1, minimum_size=9),
        Layout(tui.scrape_errors, ratio=1),
        Layout(tui.download_errors, ratio=1),
    )

    bottom = (
        Layout(tui.scrape, ratio=20),
        Layout(tui.downloads, ratio=20),
        Layout(tui.status, ratio=2),
    )

    horizontal.split_column(Layout(name="top", ratio=20), *bottom)
    vertical.split_column(Layout(name="top", ratio=60), *bottom)

    horizontal["top"].split_row(*top)
    vertical["top"].split_column(*top)

    return AppScreens(
        scraping=Screen(horizontal, vertical),
        simple=Screen(Group(tui.status.activity, tui.files.simple)),
        hashing=Screen(tui.hashing),
        sorting=Screen(tui.sorting),
    )


def _log_errors(scrape_errors: list[UIFailure], download_errors: list[UIFailure]) -> None:
    error_codes = (error.code for error in itertools.chain(scrape_errors, download_errors) if error.code is not None)
    try:
        padding = len(str(max(error_codes)))
    except ValueError:
        padding = 0

    def log(name: str, errors: list[UIFailure]) -> None:
        logger.info(name)
        if not errors:
            logger.info(f"  {'None':>{padding}}")
            return

        for error in scrape_errors:
            error_code = error.code if error.code is not None else ""
            logger.info(f"  {error_code:>{padding}}{' ' if padding else ''}{error.msg}: {error.count:,}")

    log("Scrape Failures:", scrape_errors)
    log("Download Failures:", download_errors)


def _create_hyperlink(file_path: Path, text: str | None = None) -> Text:
    return Text(text or str(file_path), style=f"link {file_path.as_uri()}")


def show_msg(msg: object):
    return _tui.get().status(str(msg))
