import asyncio
import datetime
import logging
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Annotated, Literal, ParamSpec, TypeVar

import cyclopts
import pydantic
from cyclopts import Parameter
from rich.traceback import install

from cyberdrop_dl import __version__
from cyberdrop_dl.annotations import copy_signature
from cyberdrop_dl.config import Config
from cyberdrop_dl.data_structures import AbsoluteHttpURL
from cyberdrop_dl.logger import setup_logging, spacer
from cyberdrop_dl.manager import Manager
from cyberdrop_dl.models import format_validation_error
from cyberdrop_dl.models.types import HttpURL
from cyberdrop_dl.notifications import send_apprise_notifications, send_webhook_notification
from cyberdrop_dl.sorting import Sorter
from cyberdrop_dl.updates import check_latest_pypi
from cyberdrop_dl.utils import check_partials_and_empty_folders

_P = ParamSpec("_P")
_R = TypeVar("_R")


logger = logging.getLogger(__name__)

install(width=200)


async def scrape(manager: Manager, source: Iterable[AbsoluteHttpURL] | Path) -> None:
    manager.config.resolve_paths()
    main_log = manager.config.logs.main_log
    with setup_logging(
        main_log,
        level=manager.config.runtime.log_level,
        console_level=manager.config.runtime.log_level,
    ):
        async with manager.scrape_mapper as scrapper:
            await scrapper.run(source)

        await _post_runtime(manager)
        if manager.config.ui.show_stats:
            manager.tui.show_stats(scrapper.stats)

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
    await manager.hasher.hash_post_download(manager.downloader.successful_downloads)
    await manager.hasher.dedupe()

    if manager.config.sort.enabled:
        sorter = Sorter.from_config(manager.tui, manager.config)
        await sorter.run()

    check_partials_and_empty_folders(manager.config)


def _loop_factory() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    if sys.version_info > (3, 12):
        loop.set_task_factory(asyncio.eager_task_factory)
    return loop


class App(cyclopts.App):
    @copy_signature(cyclopts.App._parse_known_args)
    def _parse_known_args(self, *args, **kwargs):
        try:
            return super()._parse_known_args(*args, **kwargs)
        except cyclopts.ValidationError as e:
            if isinstance(e.__cause__, pydantic.ValidationError):
                e.exception_message = format_validation_error(e.__cause__, title="CLI arguments")
            raise


app = App(
    name="cyberdrop-dl",
    help="Bulk asynchronous downloader for multiple file hosts",
    version=f"{__version__}.NTFS",
    default_parameter=Parameter(negative_iterable=[]),
)


@app.command()
def download(
    links: Annotated[
        list[HttpURL] | None,
        Parameter(
            name="links",
            negative=[],
            help="link(s) to content to download",
        ),
    ] = None,
    /,
    *,
    input_file: Annotated[
        Path | None,
        Parameter(
            alias="i",
            help="The path to the text file containing the URLs you want to download. Each line should be a single URL",
        ),
    ] = None,
    appdata_folder: Path | None = None,
    config_file: Annotated[Path | None, Parameter(name="config")] = None,
    impersonate: (
        Literal[
            "chrome",
            "edge",
            "safari",
            "safari_ios",
            "chrome_android",
            "firefox",
        ]
        | None
    ) = None,
    print_stats: bool = False,
    cli_options: Config = Config(),  # noqa: B008
):
    """Scrape and download files from a list of URLs (from a file or stdin)"""
    source = links or input_file or []
    if config_file:
        config = Config.load(config_file).update(cli_options)
    else:
        config = cli_options

    if impersonate:
        pass
    if print_stats:
        pass

    manager = Manager(config, appdata_folder)
    with asyncio.Runner(loop_factory=_loop_factory) as runner:
        runner.run(scrape(manager, source))


@app.command()
def show() -> None:
    """Show a list of all supported sites"""
    from cyberdrop_dl.supported_sites import get_crawlers_info_as_rich_table

    table = get_crawlers_info_as_rich_table()
    app.console.print(table)


@app.command()
def retry(
    choice: Literal["all", "failed", "maintenance"],
    /,
    *,
    completed_after: datetime.date | None = None,
    completed_before: datetime.date | None = None,
    max_items_retry: int = 0,
):
    "Retry downloads from the database"
    return


def main() -> None:
    app()


if __name__ == "__main__":
    main()
