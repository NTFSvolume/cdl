from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import time
from contextvars import ContextVar
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Self

from pydantic import ByteSize
from rich.console import Group
from rich.layout import Layout
from rich.live import Live

from cyberdrop_dl.constants import HashAlgorithm
from cyberdrop_dl.logger import spacer
from cyberdrop_dl.progress.common import ProgressHook
from cyberdrop_dl.progress.downloads import DownloadsPanel
from cyberdrop_dl.progress.errors import DownloadErrors, ScrapeErrors, UIFailure
from cyberdrop_dl.progress.files import FileStatsPanel
from cyberdrop_dl.progress.hashing import HashingPanel
from cyberdrop_dl.progress.scraping import ScrapingPanel, StatusMessage
from cyberdrop_dl.progress.screens import AppScreens, Screen
from cyberdrop_dl.progress.sorting import SortingPanel

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence
    from pathlib import Path

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.scrape_mapper import ScrapeStats

__all__ = ["TUI", "ProgressHook"]
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
            refresh_rate=config.ui.refresh_rate,
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

    def show_stats(self, stats: ScrapeStats) -> None:
        """Prints the stats of the program."""

        runtime = timedelta(seconds=int(time.monotonic() - stats.start_time))
        total_data_written = ByteSize(self.downloads.total_data_written).human_readable(decimal=True)

        logger.info(spacer())
        logger.info("Printing Stats...\n")
        logger.info("Run Stats")

        source = _hyperlink(stats.source) if stats.source else "--links"
        logger.info(f"  Input File: {source}", extra={"markup": True})
        logger.info(f"  Input URLs: {stats.count:,}")
        logger.info(f"  Input URL Groups: {len(stats.unique_groups):,}")

        stats_ = stats.domain_stats
        url_stats = json.dumps(stats_, indent=2, ensure_ascii=False) if stats_ else None
        logger.info(f"  Input URL Stats: {url_stats}")
        logger.info(f"  Total Runtime: {runtime}")
        logger.info(f"  Total Downloaded Data: {total_data_written}")

        logger.info("Download Stats:")
        logger.info(f"  Downloaded: {self.files.completed_files:,} files")
        logger.info(f"  Skipped (By Config): {self.files.skipped_files:,} files")
        logger.info(f"  Skipped (Previously Downloaded): {self.files.previously_completed:,} files")
        logger.info(f"  Failed: {self.download_errors.error_count:,} files")

        logger.info("Unsupported URLs Stats:")
        logger.info(f"  Sent to Jdownloader: {self.scrape_errors.sent_to_jdownloader:,}")
        logger.info(f"  Skipped: {self.scrape_errors.skipped:,}")

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


def _log_errors(scrape_errors: Sequence[UIFailure], download_errors: Sequence[UIFailure]) -> None:
    error_codes = (error.code for error in (*scrape_errors, *download_errors) if error.code is not None)

    try:
        padding = len(str(max(error_codes)))
    except ValueError:
        padding = 0

    for title, errors in (
        ("Scrape Failures:", scrape_errors),
        ("Download Failures:", download_errors),
    ):
        logger.info(title, extra={"color": "red"})
        if not errors:
            logger.info(f"  {'None':>{padding}}")
            return

        for error in scrape_errors:
            error_code = error.code if error.code is not None else ""
            logger.info(f"  {error_code:>{padding}}{' ' if padding else ''}{error.msg}: {error.count:,}")


def _hyperlink(file_path: Path, text: str | None = None) -> str:
    return f"[link={file_path.as_uri()}]{text or file_path}[/link]"


def show_msg(msg: object):
    return _tui.get().status(str(msg))


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
