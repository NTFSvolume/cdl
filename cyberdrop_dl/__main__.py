from __future__ import annotations

import contextlib
import logging
import sys
from typing import TYPE_CHECKING

from rich.traceback import install as install_rich_tracebacks

from cyberdrop_dl import aio, storage, webhook
from cyberdrop_dl.managers.manager import Manager
from cyberdrop_dl.scraper.scrape_mapper import ScrapeMapper
from cyberdrop_dl.ui import program_ui
from cyberdrop_dl.updates import check_latest_pypi
from cyberdrop_dl.utils.apprise import send_apprise_notifications
from cyberdrop_dl.utils.logger import log_spacer, setup_logging
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import check_partials_and_empty_folders

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger("cyberdrop_dl")

_ = install_rich_tracebacks(width=None)


async def _scrape(manager: Manager) -> None:
    manager.config.resolve_paths()
    manager.logs.delete_old_logs()
    start_time = manager.start_time

    with setup_logging(manager.config.logs.main_log):
        await manager.async_startup()

        log_spacer()
        async with manager.database:
            logger.info("Starting CDL...\n")

            await _runtime(manager)
            await _post_runtime(manager)

            stats = manager.progress_manager.print_stats(start_time)

            log_spacer()
            check_latest_pypi()
            log_spacer()
            logger.info("Closing program...")
            logger.info("Finished downloading. Enjoy :)", extra={"color": "green"})

            if manager.config.logs.webhook:
                await webhook.send_notification(manager.config.logs.webhook, stats)

            await send_apprise_notifications(manager)


async def _runtime(manager: Manager) -> None:
    async with storage.monitor(manager.global_config.general.required_free_space):
        with manager.live_manager.get_main_live(stop=True):
            async with ScrapeMapper.managed(manager) as scrape_mapper:
                await scrape_mapper.run()


async def _post_runtime(manager: Manager) -> None:
    """Actions to complete after main runtime, and before UI shutdown."""
    log_spacer()
    logger.info("Running Post-Download Processes", extra={"color": "green"})

    await manager.hasher.cleanup_dupes_after_download()

    if manager.config.sorting.sort_downloads and not manager.parsed_args.cli_only_args.retry_any:
        sorter = Sorter.from_manager(manager)
        await sorter.run()

    check_partials_and_empty_folders(manager)

    if manager.config.runtime_options.update_last_forum_post:
        await manager.logs.update_last_forum_post(manager.config.files.input_file)


async def _run(manager: Manager) -> None:
    try:
        await _scrape(manager)
    finally:
        await manager.close()


def main(args: Sequence[str] | None = None) -> str | int | None:
    manager = Manager(args)
    manager.startup()
    if not manager.parsed_args.cli_only_args.download:
        program_ui.run(manager)

    exit_code = 1
    with contextlib.suppress(Exception):
        try:
            aio.run(_run(manager))
        except KeyboardInterrupt:
            logger.info("Exiting (Ctrl + C) ...")

        exit_code = 0

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
