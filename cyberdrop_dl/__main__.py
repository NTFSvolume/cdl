import datetime
import logging
from collections.abc import AsyncGenerator, Callable, Iterable
from pathlib import Path
from typing import Annotated, Literal, ParamSpec, TypeVar

import aiosqlite
import cyclopts
import pydantic
from cyclopts import Parameter
from rich.traceback import install

from cyberdrop_dl import __version__, aio
from cyberdrop_dl.annotations import copy_signature
from cyberdrop_dl.config import Config
from cyberdrop_dl.data_structures import AbsoluteHttpURL, ScrapeItem
from cyberdrop_dl.logger import capture_logs, setup_logging, spacer
from cyberdrop_dl.manager import Manager
from cyberdrop_dl.models import format_validation_error
from cyberdrop_dl.models.types import HttpURL
from cyberdrop_dl.notifications import send_notifications
from cyberdrop_dl.sorting import Sorter
from cyberdrop_dl.updates import check_latest_pypi
from cyberdrop_dl.utils import check_partials_and_empty_folders

_P = ParamSpec("_P")
_R = TypeVar("_R")


logger = logging.getLogger(__name__)

install(width=200)


async def scrape(
    manager: Manager, source: Iterable[AbsoluteHttpURL] | Path | Callable[[], AsyncGenerator[ScrapeItem]]
) -> None:
    manager.config.resolve_paths()
    with setup_logging(manager.config.logs.main_log, level=manager.config.runtime.log_level):
        async with manager.scrape_mapper as scrapper:
            await scrapper.run(source)

        await _post_runtime(manager)
        with capture_logs() as export:
            if manager.config.ui.show_stats:
                manager.tui.show_stats(scrapper.stats)

        stats = export()
        logger.info(spacer())

        async with manager.client.create_aiohttp_session() as session:
            await check_latest_pypi(session)

        logger.info(spacer())
        logger.info("Closing program...")
        logger.info("Finished downloading. Enjoy :)", extra={"color": "green"})

        await send_notifications(manager, stats)


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


def _filter_by_date(scrape_item: ScrapeItem, before: datetime.date, after: datetime.date) -> bool:
    item_date = scrape_item.completed_at or scrape_item.created_at
    if not item_date:
        return False
    date = datetime.datetime.fromtimestamp(item_date).date()
    return after < date < before


def _create_item_from_row(row: aiosqlite.Row) -> ScrapeItem:
    referer: str = row["referer"]
    url = AbsoluteHttpURL(referer, encoded="%" in referer)
    item = ScrapeItem(url=url, retry_path=Path(row["download_path"]), part_of_album=True)
    if completed_at := row["completed_at"]:
        item.completed_at = int(datetime.datetime.fromisoformat(completed_at).timestamp())
    if created_at := row["created_at"]:
        item.created_at = int(datetime.datetime.fromisoformat(created_at).timestamp())
    return item


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
    cli_options: Config = Config(),  # noqa: B008
):
    """Scrape and download files from a list of URLs (from a file or stdin)"""
    source = links or input_file or []
    if config_file:
        config = Config.load(config_file).update(cli_options)
    else:
        config = cli_options

    manager = Manager(config, appdata_folder)
    aio.run(scrape(manager, source))


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
    completed_after: datetime.date = datetime.date.min,
    completed_before: datetime.date = datetime.date.max,
    max_items_retry: int | None = None,
    appdata_folder: Path | None = None,
    config_file: Annotated[Path | None, Parameter(name="config")] = None,
    cli_options: Config = Config(),  # noqa: B008
):
    "Retry downloads from the database"

    if config_file:
        config = Config.load(config_file).update(cli_options)
    else:
        config = cli_options

    manager = Manager(config, appdata_folder)

    async def load_items() -> AsyncGenerator[ScrapeItem]:
        """Loads failed links from database."""
        n_retries = 0
        if choice == "failed":
            gen = manager.database.history_table.get_failed_items()
        elif choice == "all":
            gen = manager.database.history_table.get_all_items(completed_after, completed_before)
        else:
            gen = manager.database.history_table.get_all_bunkr_failed()
        try:
            async for rows in gen:
                for row in rows:
                    item = _create_item_from_row(row)
                    if _filter_by_date(item, completed_before, completed_after):
                        continue
                    yield item
                    n_retries += 1
                    if max_items_retry and n_retries > max_items_retry:
                        return
        finally:
            await gen.aclose()

    aio.run(scrape(manager, load_items))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
