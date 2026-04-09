# ruff: noqa: E402
from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

from rich.traceback import install as install_rich_tracebacks

from cyberdrop_dl.cli import parse_args
from cyberdrop_dl.config import Config
from cyberdrop_dl.config.merge import merge_cli_and_config_args

_ = install_rich_tracebacks(width=None)

from cyberdrop_dl import aio, webhook
from cyberdrop_dl.logs import log_spacer, setup_console_logging, setup_file_logging
from cyberdrop_dl.managers.manager import AppData, Manager
from cyberdrop_dl.scrape_mapper import ScrapeMapper
from cyberdrop_dl.ui import program_ui
from cyberdrop_dl.utils import apprise, check_latest_pypi
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import check_partials_and_empty_folders

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger("cyberdrop_dl")


async def _scrape(manager: Manager) -> None:
    with setup_file_logging(manager.config.settings.logs.main_log):
        await manager.async_startup()

        log_spacer()
        async with manager.database:
            log_spacer()
            logger.info("Starting CDL...")
            with manager.live_manager.get_main_live(stop=True):
                async with ScrapeMapper(manager)() as scrape_mapper:
                    stats = await scrape_mapper.run()

            log_spacer()
            await _post_runtime(manager)

            stats_summary = manager.progress_manager.print_stats(stats)

            log_spacer()
            await check_latest_pypi()
            log_spacer()
            logger.info("Closing program...")
            logger.info("Finished downloading. Enjoy :)", extra={"color": "green"})

            if manager.config.settings.logs.webhook:
                await webhook.send_notification(manager.config.settings.logs.webhook, stats_summary)

            if manager.config.apprise_urls:
                await apprise.send_notifications(manager.config.apprise_urls, stats_summary)


async def _post_runtime(manager: Manager) -> None:
    """Actions to complete after main runtime, and before UI shutdown."""
    logger.info("Running Post-Download Processes\n ", extra={"color": "green"})

    await manager.hasher.cleanup_dupes_after_download()

    if manager.config.settings.sorting.sort_downloads and not manager.cli_args.retry_any:
        sorter = Sorter.from_manager(manager)
        await sorter.run()

    check_partials_and_empty_folders(manager)

    if manager.config.settings.runtime_options.update_last_forum_post:
        await manager.logs.update_last_forum_post(manager.config.settings.files.input_file)


async def _run(manager: Manager) -> None:
    try:
        await _scrape(manager)
    finally:
        await manager.close()


def main(args: Sequence[str] | None = None) -> int:
    with setup_console_logging():
        parsed_args = parse_args(args)
        appdata = (
            AppData.from_path(parsed_args.cli_only_args.appdata_folder)
            if parsed_args.cli_only_args.appdata_folder
            else AppData.default()
        )

        config = Config.create(appdata, parsed_args.cli_only_args.config_file)

        merge_cli_and_config_args(config, parsed_args)
        manager = Manager(parsed_args.cli_only_args, appdata, config)
        manager.resolve_paths()
        if not manager.cli_args.download:
            program_ui.run(manager)

        try:
            aio.run(_run(manager))

        except KeyboardInterrupt:
            logger.info("Exiting (Ctrl + C) ...")

        return 0


if __name__ == "__main__":
    sys.exit(main())
