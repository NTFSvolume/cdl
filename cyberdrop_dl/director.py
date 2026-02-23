from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from datetime import datetime
from enum import IntEnum
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from cyberdrop_dl import config, constants, env
from cyberdrop_dl.dependencies import browser_cookie3
from cyberdrop_dl.managers import Manager
from cyberdrop_dl.scrape_mapper import ScrapeMapper
from cyberdrop_dl.utils.apprise import send_apprise_notifications
from cyberdrop_dl.utils.logger import LogHandler, QueuedLogger, log, log_spacer, log_with_color
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.updates import check_latest_pypi
from cyberdrop_dl.utils.utilities import check_partials_and_empty_folders
from cyberdrop_dl.utils.webhook import send_webhook_message

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Sequence

P = ParamSpec("P")
R = TypeVar("R")


class ExitCode(IntEnum):
    OK = 0
    ERROR = 1


_C = ExitCode


def _ui_error_handling_wrapper(
    func: Callable[P, Coroutine[None, None, R]],
) -> Callable[P, Coroutine[None, None, R | None]]:
    """Wrapper handles errors from the main UI."""

    @wraps(func)
    async def wrapper(*args, **kwargs) -> R | None:
        try:
            return await func(*args, **kwargs)
        except* Exception as e:
            exceptions = [e]
            if isinstance(e, ExceptionGroup):
                exceptions = e.exceptions
            if not isinstance(exceptions[0], browser_cookie3.BrowserCookieError):
                msg = "An error occurred, please report this to the developer with your logs file:"
                log_with_color(msg, "bold red", 50, show_in_stats=False)
            for exc in exceptions:
                log_with_color(f"  {exc}", "bold red", 50, show_in_stats=False, exc_info=exc)

    return wrapper


@_ui_error_handling_wrapper
async def _run_manager(manager: Manager) -> None:
    debug_log_file_path = _setup_debug_logger(manager)
    start_time = manager.start_time
    _setup_main_logger(manager)
    log(f"Using Debug Log: {debug_log_file_path}", 10)
    log("Starting Async Processes...", 10)
    await manager.async_startup()
    log_spacer(10)

    log("Starting CDL...\n", 20)

    await _scheduler(manager)

    manager.progress_manager.print_stats(start_time)

    log_spacer(20)
    log("Checking for Updates...", 20)
    check_latest_pypi()
    log_spacer(20)
    log("Closing Program...", 20)
    log_with_color("Finished downloading. Enjoy :)", "green", 20, show_in_stats=False)

    await send_webhook_message(manager)
    await send_apprise_notifications(manager)


async def _scheduler(manager: Manager) -> None:
    for func in (_runtime, _post_runtime):
        if manager.states.SHUTTING_DOWN.is_set():
            return

        try:
            await func(manager)
        except asyncio.CancelledError:
            if not manager.states.SHUTTING_DOWN.is_set():
                raise


async def _runtime(manager: Manager) -> None:
    """Main runtime loop for the program, this will run until all scraping and downloading is complete."""

    manager.states.RUNNING.set()
    with manager.live_manager.get_main_live(stop=True):
        async with ScrapeMapper.managed(manager) as scrape_mapper:
            await scrape_mapper.run()


async def _post_runtime(manager: Manager) -> None:
    """Actions to complete after main runtime, and before ui shutdown."""
    log_spacer(20, log_to_console=False)
    msg = "Running Post-Download Processes"
    log_with_color(msg, "green", 20)

    await manager.hash_manager.hash_client.cleanup_dupes_after_download()

    if config.get().sorting.sort_downloads:
        sorter = Sorter(manager)
        await sorter.run()

    check_partials_and_empty_folders(manager)


def _setup_debug_logger(manager: Manager) -> Path | None:
    if not env.DEBUG_VAR:
        return

    debug_logger = logging.getLogger("cyberdrop_dl_debug")
    log_level = 10
    settings_data = config.get()
    settings_data.runtime_options.log_level = log_level
    debug_logger.setLevel(log_level)
    debug_log_file_path = Path(__file__).parents[1] / "cyberdrop_dl_debug.log"
    if env.DEBUG_LOG_FOLDER:
        debug_log_folder = Path(env.DEBUG_LOG_FOLDER)
        if not debug_log_folder.is_dir():
            msg = "Value of env var 'CDL_DEBUG_LOG_FOLDER' is invalid."
            msg += f" Folder '{debug_log_folder}' does not exists"
            raise FileNotFoundError(None, msg, env.DEBUG_LOG_FOLDER)
        date = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_log_file_path = debug_log_folder / f"cyberdrop_dl_debug_{date}.log"

    file_io = debug_log_file_path.open("w", encoding="utf8")

    file_handler = LogHandler(level=log_level, file=file_io, width=500, debug=True)
    queued_logger = QueuedLogger(manager, file_handler, "debug")
    debug_logger.addHandler(queued_logger.handler)

    # aiosqlite_log = logging.getLogger("aiosqlite")
    # aiosqlite_log.setLevel(log_level)
    # aiosqlite_log.addHandler(file_handler_debug)

    return debug_log_file_path.resolve()


def _setup_main_logger(manager: Manager) -> None:
    logger = logging.getLogger("cyberdrop_dl")
    file_io = config.get().logs.main_log.open("w", encoding="utf8")
    log_level = config.get().runtime_options.log_level
    logger.setLevel(log_level)

    logger.addHandler(constants.console_handler)
    logger.addHandler(
        QueuedLogger(manager, LogHandler(level=log_level, file=file_io, width=500)).handler,
    )


def _loop_factory() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    if sys.version_info > (3, 12):
        loop.set_task_factory(asyncio.eager_task_factory)
    return loop


class Director:
    """Creates a manager and runs it"""

    def __init__(self, args: Sequence[str] | None = None) -> None:
        self.manager: Manager = Manager()

    def run(self) -> int:
        return self._run()

    async def async_run(self) -> None:
        try:
            await _run_manager(self.manager)
        finally:
            await self.manager.close()

    def _run(self) -> int:
        exit_code = _C.ERROR
        with contextlib.suppress(Exception):
            with asyncio.Runner(loop_factory=_loop_factory) as runner:
                runner.run(self.async_run())
            exit_code = _C.OK

        return exit_code
