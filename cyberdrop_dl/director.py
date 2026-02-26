from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import sys
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from cyberdrop_dl import config
from cyberdrop_dl.dependencies import browser_cookie3
from cyberdrop_dl.logger import setup_logging, spacer
from cyberdrop_dl.managers import Manager
from cyberdrop_dl.scrape_mapper import ScrapeMapper
from cyberdrop_dl.updates import check_latest_pypi
from cyberdrop_dl.utils.apprise import send_notifications
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import check_partials_and_empty_folders
from cyberdrop_dl.utils.webhook import send_webhook_message

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Sequence

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


logger = logging.getLogger(__name__)


def _task_group_error_wrapper(
    func: Callable[_P, Coroutine[None, None, _R]],
) -> Callable[_P, Coroutine[None, None, _R | None]]:
    """Wrapper handles errors from the main UI."""

    @functools.wraps(func)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R | None:
        try:
            return await func(*args, **kwargs)
        except* Exception as e:
            exceptions = e.exceptions
            if not isinstance(exceptions[0], browser_cookie3.BrowserCookieError):
                logger.critical(
                    "An error occurred, please report this to the developer with your logs file:",
                    extra={"color": "red"},
                )

            for exc in exceptions:
                logger.critical(e, exc_info=exc)

    return wrapper


@_task_group_error_wrapper
async def _run_manager(manager: Manager) -> None:
    config_ = config.get()
    with setup_logging(config_.logs.main_log, config_.runtime.log_level, config_.runtime.console_log_level):
        logger.info("Starting Async Processes...")
        await manager.async_startup()

        logger.info("Starting CDL...\n")
        await _scheduler(manager)
        manager.progress_manager.print_stats(1)

        logger.info(spacer())

        await check_latest_pypi()
        logger.info(spacer())
        logger.info("Closing program...")
        logger.info("Finished downloading. Enjoy :)", extra={"color": "green"})

        await send_webhook_message(manager)
        await send_notifications("", attachment=config_.logs.main_log)


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
    logger.info(spacer())
    logger.info("Running Post-Download Processes", extra={"color": "green"})

    await manager.hash_manager.hash_client.cleanup_dupes_after_download()

    if config.get().sorting.sort_downloads:
        sorter = Sorter(manager)
        await sorter.run()

    check_partials_and_empty_folders(manager)


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
        exit_code = 1
        with contextlib.suppress(Exception):
            with asyncio.Runner(loop_factory=_loop_factory) as runner:
                runner.run(self.async_run())
            exit_code = 0

        return exit_code
