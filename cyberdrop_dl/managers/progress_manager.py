from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import ByteSize
from rich.columns import Columns
from rich.console import Group
from rich.layout import Layout
from rich.progress import Progress, SpinnerColumn, TaskID
from rich.text import Text
from yarl import URL

from cyberdrop_dl import __version__
from cyberdrop_dl.ui.progress.downloads_progress import DownloadsProgress
from cyberdrop_dl.ui.progress.file_progress import FileProgress
from cyberdrop_dl.ui.progress.hash_progress import HashProgress
from cyberdrop_dl.ui.progress.scraping_progress import ScrapingProgress
from cyberdrop_dl.ui.progress.sort_progress import SortProgress
from cyberdrop_dl.ui.progress.statistic_progress import DownloadStatsProgress, ScrapeStatsProgress
from cyberdrop_dl.utils.logger import capture_logs, log_spacer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence
    from pathlib import Path

    from rich.panel import Panel

    from cyberdrop_dl.managers.manager import Manager
    from cyberdrop_dl.ui.progress.statistic_progress import UiFailureTotal


logger = logging.getLogger(__name__)


spinner = SpinnerColumn(style="green", spinner_name="dots")


class ProgressManager:
    def __init__(self, manager: Manager) -> None:
        # File Download Bars
        self.manager = manager
        ui_options = manager.config_manager.global_settings_data.ui_options
        self.portrait = manager.parsed_args.cli_only_args.portrait
        self.file_progress = FileProgress(manager)
        self.scraping_progress = ScrapingProgress(manager)

        # Overall Progress Bars & Stats
        self.download_progress: DownloadsProgress = DownloadsProgress(manager)
        self.download_stats_progress: DownloadStatsProgress = DownloadStatsProgress()
        self.scrape_stats_progress: ScrapeStatsProgress = ScrapeStatsProgress()
        self.hash_progress: HashProgress = HashProgress(manager)
        self.sort_progress: SortProgress = SortProgress(1, manager)

        self.ui_refresh_rate: int = ui_options.refresh_rate

        activity = Progress(spinner, "[progress.description]{task.description}")
        self.status_message: Progress = Progress(spinner, "[progress.description]{task.description}")

        self.status_message_task_id: TaskID = self.status_message.add_task("", total=100, completed=0, visible=False)
        self.activity_task_id: TaskID = activity.add_task(
            f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0
        )
        self.activity: Progress = activity

        simple_layout = Group(activity, self.download_progress.simple_progress)

        status_message_columns = Columns([activity, self.status_message], expand=False)

        horizontal_layout = Layout()
        vertical_layout = Layout()

        upper_layouts = (
            Layout(renderable=self.download_progress.get_progress(), name="Files", ratio=1, minimum_size=9),
            Layout(renderable=self.scrape_stats_progress.get_progress(), name="Scrape Failures", ratio=1),
            Layout(renderable=self.download_stats_progress.get_progress(), name="Download Failures", ratio=1),
        )

        lower_layouts = (
            Layout(renderable=self.scraping_progress.get_renderable(), name="Scraping", ratio=20),
            Layout(renderable=self.file_progress.get_renderable(), name="Downloads", ratio=20),
            Layout(renderable=status_message_columns, name="status_message", ratio=2),
        )

        horizontal_layout.split_column(Layout(name="upper", ratio=20), *lower_layouts)
        vertical_layout.split_column(Layout(name="upper", ratio=60), *lower_layouts)

        horizontal_layout["upper"].split_row(*upper_layouts)
        vertical_layout["upper"].split_column(*upper_layouts)

        self.horizontal_layout: Layout = horizontal_layout
        self.vertical_layout: Layout = vertical_layout
        self.activity_layout: Progress = activity
        self.simple_layout: Group = simple_layout
        self.hash_remove_layout: Panel = self.hash_progress.get_removed_progress()
        self.hash_layout: Panel = self.hash_progress.get_renderable()
        self.sort_layout: Panel = self.sort_progress.get_renderable()

    @asynccontextmanager
    async def show_status_msg(self, msg: str | None) -> AsyncGenerator[None]:
        try:
            self.status_message.update(self.status_message_task_id, description=msg, visible=bool(msg))
            yield
        finally:
            self.status_message.update(self.status_message_task_id, visible=False)

    @property
    def fullscreen_layout(self) -> Layout:
        if self.portrait:
            return self.vertical_layout
        return self.horizontal_layout

    def print_stats(self, start_time: float) -> str:
        if not self.manager.parsed_args.cli_only_args.print_stats:
            return ""

        log_spacer()
        logger.info("Printing Stats...\n")

        with capture_logs() as stream:
            self._print_stats(start_time)

        return stream.getvalue()

    def _print_stats(self, start_time: float) -> None:

        end_time = time.perf_counter()
        runtime = timedelta(seconds=int(end_time - start_time))
        total_data_written = ByteSize(self.file_progress.total_data_written).human_readable(decimal=True)

        config_path = self.manager.appdata.configs / self.manager.config_manager.loaded_config
        config_path_text = get_console_hyperlink(config_path, text=self.manager.config_manager.loaded_config)
        input_file_text = get_input(self.manager)
        log_folder_text = get_console_hyperlink(self.manager.config.logs.log_folder)

        logger.info("Run Stats: ", config_path_text)
        logger.info("  Input File: ", input_file_text)
        logger.info(f"  Input URLs: {self.manager.scrape_mapper.count:,}")
        logger.info(f"  Input URL Groups: {self.manager.scrape_mapper.group_count:,}")
        logger.info("  Log Folder: ", log_folder_text)
        logger.info(f"  Total Runtime: {runtime}")
        logger.info(f"  Total Downloaded Data: {total_data_written}")

        log_spacer()
        logger.info("Download Stats:")
        logger.info(f"  Downloaded: {self.download_progress.completed_files:,} files")
        logger.info(f"  Skipped (By Config): {self.download_progress.skipped_files:,} files")
        logger.info(f"  Skipped (Previously Downloaded): {self.download_progress.previously_completed_files:,} files")
        logger.info(f"  Failed: {self.download_stats_progress.failed_files:,} files")

        log_spacer()
        logger.info("Unsupported URLs Stats:")
        logger.info(f"  Sent to Jdownloader: {self.scrape_stats_progress.sent_to_jdownloader:,}")
        logger.info(f"  Skipped: {self.scrape_stats_progress.unsupported_urls_skipped:,}")

        self.print_dedupe_stats()

        log_spacer()
        logger.info("Sort Stats:")
        logger.info(f"  Audios: {self.sort_progress.audio_count:,}")
        logger.info(f"  Images: {self.sort_progress.image_count:,}")
        logger.info(f"  Videos: {self.sort_progress.video_count:,}")
        logger.info(f"  Other Files: {self.sort_progress.other_count:,}")

        _log_errors(self.scrape_stats_progress.return_totals(), self.download_stats_progress.return_totals())

    def print_dedupe_stats(self) -> None:
        log_spacer()
        logger.info("Dupe Stats:")
        logger.info(f"  Newly Hashed: {self.hash_progress.hashed_files:,} files")
        logger.info(f"  Previously Hashed: {self.hash_progress.prev_hashed_files:,} files")
        logger.info(f"  Removed (Downloads): {self.hash_progress.removed_files:,} files")


def _log_errors(scrape_errors: Sequence[UiFailureTotal], download_errors: Sequence[UiFailureTotal]) -> None:
    error_codes = (error.code for error in (*scrape_errors, *download_errors) if error.code is not None)

    try:
        padding = len(str(max(error_codes)))
    except ValueError:
        padding = 0

    for title, errors in (
        ("Scrape Failures:", scrape_errors),
        ("Download Failures:", download_errors),
    ):
        log_spacer()
        logger.info(title, extra={"color": "red"})
        if not errors:
            logger.info(f"  {'None':>{padding}}")
            return

        for error in scrape_errors:
            error_code = error.code if error.code is not None else ""
            logger.info(f"  {error_code:>{padding}}{' ' if padding else ''}{error.msg}: {error.count:,}")


def get_input(manager: Manager) -> Text | str:
    if manager.parsed_args.cli_only_args.retry_all:
        return "--retry-all"
    if manager.parsed_args.cli_only_args.retry_failed:
        return "--retry-failed"
    if manager.parsed_args.cli_only_args.retry_maintenance:
        return "--retry-maintenance"
    if manager.scrape_mapper.using_input_file:
        return get_console_hyperlink(manager.config.files.input_file)
    return "--links (CLI args)"


def get_console_hyperlink(file_path: Path, text: str = "") -> Text:
    full_path = file_path
    show_text = text or full_path
    file_url = URL(full_path.as_posix()).with_scheme("file")
    return Text(str(show_text), style=f"link {file_url}")
