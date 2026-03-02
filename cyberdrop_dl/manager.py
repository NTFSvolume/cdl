from __future__ import annotations

import asyncio
import contextlib
import csv
import dataclasses
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from cyberdrop_dl import __version__, appdata, ffmpeg, storage
from cyberdrop_dl.clients.http import HTTPClient
from cyberdrop_dl.database import Database
from cyberdrop_dl.exceptions import get_origin
from cyberdrop_dl.hasher import Hasher
from cyberdrop_dl.scrape_mapper import ScrapeMapper
from cyberdrop_dl.tui import TUI
from cyberdrop_dl.utils import filepath, get_system_information, json

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable
    from pathlib import Path

    from yarl import URL

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import MediaItem, ScrapeItem


_CSV_DELIMITER = ","
_file_locks: defaultdict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)


@dataclasses.dataclass(slots=True)
class Events:
    SHUTTING_DOWN: asyncio.Event = dataclasses.field(init=False, default_factory=asyncio.Event)
    RUNNING: asyncio.Event = dataclasses.field(init=False, default_factory=asyncio.Event)


logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, config: Config) -> None:
        self.config: Config = config
        self.database: Database = Database(appdata.get().db_file, config.runtime.ignore_history)
        self.client: HTTPClient = HTTPClient.from_config(config)
        self.hasher: Hasher = Hasher.build(self)
        self.tui: TUI = TUI.from_config(config)
        self.task_group: asyncio.TaskGroup = asyncio.TaskGroup()
        self.scrape_mapper: ScrapeMapper = ScrapeMapper(self)
        self._states: Events | None = None
        self.logs: LogsManager = LogsManager(config, self.task_group)
        self._completed_downloads: set[MediaItem] = set()
        self._completed_downloads_paths: set[Path] = set()
        self._prev_downloads: set[MediaItem] = set()
        self._prev_downloads_paths: set[Path] = set()

    @property
    def states(self) -> Events:
        if self._states is None:
            self._states = Events()
        return self._states

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

    @contextlib.asynccontextmanager
    async def run(self) -> AsyncGenerator[ScrapeMapper]:
        """Async startup process for the manager."""

        _ = filepath.MAX_FILE_LEN.set(self.config.general.max_file_name_length)
        _ = filepath.MAX_FOLDER_LEN.set(self.config.general.max_folder_name_length)
        self.log_app_state()

        async with (
            self.task_group,
            self.client,
            storage.monitor(self.config.general.required_free_space),
        ):
            await self.client.load_cookie_files()
            yield self.scrape_mapper

    def log_app_state(self) -> None:
        config_ = self.config
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
        logger.debug(stats)


@dataclasses.dataclass(slots=True, frozen=True)
class LogsManager:
    config: Config
    task_group: asyncio.TaskGroup = dataclasses.field(repr=False)
    _has_headers: set[Path] = dataclasses.field(init=False, default_factory=set)

    async def write_jsonl(self, data: Iterable[dict[str, Any]]) -> None:
        async with _file_locks[self.config.logs.jsonl_file]:
            await asyncio.to_thread(json.dump_jsonl, data, self.config.logs.jsonl_file)

    async def _write_to_csv(self, file: Path, **kwargs: Any) -> None:
        """Write to the specified csv file. kwargs are columns for the CSV."""
        async with _file_locks[file]:
            is_first_write = file not in self._has_headers
            self._has_headers.add(file)

            def write():
                if is_first_write:
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.unlink(missing_ok=True)

                with file.open("a", encoding="utf8", newline="") as csv_file:
                    writer = csv.DictWriter(
                        csv_file, fieldnames=kwargs, delimiter=_CSV_DELIMITER, quoting=csv.QUOTE_ALL
                    )
                    if is_first_write:
                        writer.writeheader()
                    writer.writerow(kwargs)

            await asyncio.to_thread(write)

    def write_last_post_log(self, url: URL) -> None:
        _ = self.task_group.create_task(self._write_to_csv(self.config.logs.last_forum_post, url=url))

    def write_unsupported(self, url: URL, origin: ScrapeItem | URL | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(self.config.logs.unsupported_urls, url=url, origin=get_origin(origin))
        )

    def write_download_error(self, media_item: MediaItem, error_message: str) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.download_error_urls,
                url=media_item.url,
                error=error_message,
                referer=media_item.referer,
                origin=get_origin(media_item),
            )
        )

    def write_scrape_error(self, url: URL | str, error_message: str, origin: URL | Path | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.scrape_error_urls,
                url=url,
                error=error_message,
                origin=origin,
            )
        )
