from __future__ import annotations

import asyncio
import functools
import logging
import sys
import time
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from cyberdrop_dl.dependencies import browser_cookie3
from cyberdrop_dl.logger import setup_logging, spacer
from cyberdrop_dl.manager import Manager
from cyberdrop_dl.notifications import send_apprise_notifications, send_webhook_notification
from cyberdrop_dl.sorting import Sorter
from cyberdrop_dl.updates import check_latest_pypi
from cyberdrop_dl.utils import check_partials_and_empty_folders

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from pathlib import Path

    from cyberdrop_dl.data_structures import AbsoluteHttpURL

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
async def scrape(manager: Manager, source: list[AbsoluteHttpURL] | Path) -> None:
    manager.config.resolve_paths()
    main_log = manager.config.logs.main_log
    start_time = time.monotonic()
    with setup_logging(
        main_log,
        level=manager.config.runtime.log_level,
        console_level=manager.config.runtime.console_log_level,
    ):
        async with manager.scrape_mapper as scrapper:
            await scrapper.run(source)

        await _post_runtime(manager)
        manager.tui.show_stats(start_time)

        logger.info(spacer())

        async with manager.client.create_aiohttp_session() as session:
            await check_latest_pypi(session)
            logger.info(spacer())
            logger.info("Closing program...")
            logger.info("Finished downloading. Enjoy :)", extra={"color": "green"})

            if webhook := manager.config.logs.webhook:
                await send_webhook_notification(session, webhook, main_log)
            await send_apprise_notifications("TODO: cappture contetx from stats", main_log=main_log)


async def _post_runtime(manager: Manager) -> None:
    """Actions to complete after the main scrape process"""
    logger.info(spacer())
    logger.info("Running Post-Download Processes", extra={"color": "green"})
    await manager.hasher.post_download_hash(manager.completed_downloads)
    await manager.hasher.dedupe()

    if manager.config.sorting.sort_downloads:
        sorter = Sorter.from_config(manager.tui, manager.config)
        await sorter.run()

    check_partials_and_empty_folders(manager.config)


def _loop_factory() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    if sys.version_info > (3, 12):
        loop.set_task_factory(asyncio.eager_task_factory)
    return loop


def main() -> None:
    manager = Manager()
    with asyncio.Runner(loop_factory=_loop_factory) as runner:
        runner.run(scrape(manager))
