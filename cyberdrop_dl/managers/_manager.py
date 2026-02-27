from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from dataclasses import field
from typing import TYPE_CHECKING

from cyberdrop_dl import __version__, appdata, config, constants
from cyberdrop_dl.clients.http import HttpClient
from cyberdrop_dl.database import Database
from cyberdrop_dl.managers.hash_manager import HashManager
from cyberdrop_dl.managers.logs import LogManager
from cyberdrop_dl.progress import TUI
from cyberdrop_dl.storage import StorageChecker
from cyberdrop_dl.utils import close_if_defined, ffmpeg, get_system_information

if TYPE_CHECKING:
    from asyncio import TaskGroup
    from pathlib import Path

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.scrape_mapper import ScrapeMapper


@dataclasses.dataclass(slots=True)
class AsyncioEvents:
    SHUTTING_DOWN: asyncio.Event = dataclasses.field(init=False, default_factory=asyncio.Event)
    RUNNING: asyncio.Event = dataclasses.field(init=False, default_factory=asyncio.Event)


logger = logging.getLogger(__name__)


class Manager:
    def __init__(self) -> None:
        self.hash_manager: HashManager = field(init=False)
        self.db_manager: Database = field(init=False)
        self.http_client: HttpClient = field(init=False)
        self.storage_manager: StorageChecker = field(init=False)

        self.progress: TUI = TUI(refresh_rate=10)

        self.task_group: TaskGroup = asyncio.TaskGroup()
        self.scrape_mapper: ScrapeMapper = field(init=False)

        self.states: AsyncioEvents

        self.logs: LogManager = LogManager(config.get(), self.task_group)
        log_app_state()
        self._completed_downloads: set[MediaItem] = set()
        self._completed_downloads_paths: set[Path] = set()
        self._prev_downloads: set[MediaItem] = set()
        self._prev_downloads_paths: set[Path] = set()

    def add_completed(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        self._completed_downloads.add(media_item)
        self._completed_downloads_paths.add(media_item.complete_file)

    def add_prev(self, media_item: MediaItem) -> None:
        self._prev_downloads.add(media_item)
        self._prev_downloads_paths.add(media_item.complete_file)

    @property
    def completed_downloads(self) -> set[MediaItem]:
        return self._completed_downloads

    @property
    def prev_downloads(self) -> set[MediaItem]:
        return self._prev_downloads

    async def async_startup(self) -> None:
        """Async startup process for the manager."""
        self.states = AsyncioEvents()
        self.http_client = HttpClient(self.config)
        self.storage_manager = StorageChecker(self)

        await self.async_db_hash_startup()

        constants.MAX_NAME_LENGTHS["FILE"] = config.get().general.max_file_name_length
        constants.MAX_NAME_LENGTHS["FOLDER"] = config.get().general.max_folder_name_length

    async def async_db_hash_startup(self) -> None:
        self.db_manager = Database(
            appdata.get().db_file,
            config.get().runtime.ignore_history,
        )
        await self.db_manager.startup()
        self.hash_manager = HashManager(self)

    async def async_db_close(self) -> None:
        "Partial shutdown for managers used for hash directory scanner"
        self.db_manager = await close_if_defined(self.db_manager)
        self.hash_manager = constants.NOT_DEFINED

    async def close(self) -> None:
        """Closes the manager."""
        self.states.RUNNING.clear()

        await self.async_db_close()

        self.http_client = await close_if_defined(self.http_client)
        self.storage_manager = await close_if_defined(self.storage_manager)


def log_app_state() -> None:
    config_ = config.get()
    app_data = appdata.get()
    auth = {site: all(credentials.values()) for site, credentials in config_.auth.model_dump().items()}

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
