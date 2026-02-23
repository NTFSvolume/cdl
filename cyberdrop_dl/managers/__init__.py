from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import field
from time import perf_counter
from typing import TYPE_CHECKING, NamedTuple

from cyberdrop_dl import __version__, appdata, config, constants
from cyberdrop_dl.database import Database
from cyberdrop_dl.managers.client_manager import ClientManager
from cyberdrop_dl.managers.config_manager import ConfigManager
from cyberdrop_dl.managers.hash_manager import HashManager
from cyberdrop_dl.managers.live_manager import LiveManager
from cyberdrop_dl.managers.log_manager import LogManager
from cyberdrop_dl.managers.path_manager import PathManager
from cyberdrop_dl.managers.storage_manager import StorageManager
from cyberdrop_dl.progress import ProgressManager
from cyberdrop_dl.utils import ffmpeg
from cyberdrop_dl.utils.logger import LogHandler, QueuedLogger
from cyberdrop_dl.utils.utilities import close_if_defined, get_system_information

if TYPE_CHECKING:
    from asyncio import TaskGroup

    from cyberdrop_dl.scrape_mapper import ScrapeMapper


class AsyncioEvents(NamedTuple):
    SHUTTING_DOWN: asyncio.Event
    RUNNING: asyncio.Event


logger = logging.getLogger(__name__)


class Manager:
    def __init__(self) -> None:
        self.hash_manager: HashManager = field(init=False)
        self.db_manager: Database = field(init=False)
        self.client_manager: ClientManager = field(init=False)
        self.storage_manager: StorageManager = field(init=False)

        self.progress_manager: ProgressManager = ProgressManager(self, portrait=False)
        self.live_manager: LiveManager = field(init=False)

        self.task_group: TaskGroup = field(init=False)
        self.scrape_mapper: ScrapeMapper = field(init=False)

        self.start_time: float = perf_counter()
        self.loggers: dict[str, QueuedLogger] = {}
        self.states: AsyncioEvents

        constants.console_handler = LogHandler(level=constants.CONSOLE_LEVEL)

        self.path_manager: PathManager = PathManager(self)
        self.path_manager.pre_startup()
        self.config_manager: ConfigManager = ConfigManager(self)
        self.config_manager.startup()

        self.path_manager.startup()
        self.log_manager: LogManager = LogManager(self)
        log_app_state()

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def async_startup(self) -> None:
        """Async startup process for the manager."""
        self.states = AsyncioEvents(asyncio.Event(), asyncio.Event())
        self.client_manager = ClientManager(self)
        await self.client_manager.startup()
        self.storage_manager = StorageManager(self)

        await self.async_db_hash_startup()

        constants.MAX_NAME_LENGTHS["FILE"] = config.get().general.max_file_name_length
        constants.MAX_NAME_LENGTHS["FOLDER"] = config.get().general.max_folder_name_length

    async def async_db_hash_startup(self) -> None:
        self.db_manager = Database(
            self.path_manager.history_db,
            config.get().runtime_options.ignore_history,
        )
        await self.db_manager.startup()
        self.hash_manager = HashManager(self)
        self.live_manager = LiveManager(self)

    async def async_db_close(self) -> None:
        "Partial shutdown for managers used for hash directory scanner"
        self.db_manager = await close_if_defined(self.db_manager)
        self.hash_manager = constants.NOT_DEFINED

    async def close(self) -> None:
        """Closes the manager."""
        self.states.RUNNING.clear()

        await self.async_db_close()

        self.client_manager = await close_if_defined(self.client_manager)
        self.storage_manager = await close_if_defined(self.storage_manager)

        while self.loggers:
            _, queued_logger = self.loggers.popitem()
            queued_logger.stop()


def log_app_state() -> None:
    auth = {}

    config_ = config.get()
    app_data = appdata.get()
    for site, auth_entries in config_.auth.model_dump().items():  # pyright: ignore[reportAny]
        auth[site] = all(auth_entries.values())  # pyright: ignore[reportAny]

    # f"Using Input File: {self.path_manager.input_file}",
    stats = dict(  # noqa: C408
        version=__version__,
        system=get_system_information(),
        ffmpeg=ffmpeg.get_ffmpeg_version(),
        ffprobe=ffmpeg.get_ffprobe_version(),
        database=app_data.db_file,
        config_file=config_.source,
        auth=auth,
        config=config_.model_dump_json(indent=2, exclude={"auth"}),
    )
    logger.debug(json.dumps(stats, indent=2, ensure_ascii=False))
