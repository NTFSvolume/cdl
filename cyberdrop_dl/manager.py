from __future__ import annotations

import asyncio
import csv
import dataclasses
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cyberdrop_dl import __version__, ffmpeg
from cyberdrop_dl.cache import Cache
from cyberdrop_dl.clients.http import HTTPClient
from cyberdrop_dl.config import Config
from cyberdrop_dl.database import Database
from cyberdrop_dl.downloader import DownloadManager
from cyberdrop_dl.exceptions import get_origin
from cyberdrop_dl.hasher import Hasher
from cyberdrop_dl.scrape_mapper import ScrapeMapper
from cyberdrop_dl.tui import TUI
from cyberdrop_dl.utils import get_system_information, json

if TYPE_CHECKING:
    from collections.abc import Iterable

    from yarl import URL

    from cyberdrop_dl.data_structures.url_objects import MediaItem, ScrapeItem


_CSV_DELIMITER = ","


logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, config: Config | None = None, app_data: AppData | Path | None = None) -> None:
        config = config or Config()
        self.config: Config = config
        self.app_data: AppData = AppData((Path(app_data or "/app_data")).resolve())
        self.database: Database = Database(self.app_data.db_file, config.runtime.ignore_history)
        self.client: HTTPClient = HTTPClient.from_config(config)
        self.hasher: Hasher = Hasher.from_manager(self)
        self.tui: TUI = TUI.from_config(config)
        self.task_group: asyncio.TaskGroup = asyncio.TaskGroup()
        self.scrape_mapper: ScrapeMapper = ScrapeMapper(self)
        self.logs: LogsManager = LogsManager(config, self.task_group)
        self.downloader: DownloadManager = DownloadManager.from_manager(self)
        self.cache: Cache = Cache(self.app_data.cache_file)

    def log_app_state(self) -> None:
        auth = {site: all(credentials.values()) for site, credentials in self.config.auth.model_dump().items()}

        # f"Using Input File: {self.path_manager.input_file}",
        stats = dict(  # noqa: C408
            version=__version__,
            system=get_system_information(),
            ffmpeg=ffmpeg.get_ffmpeg_version(),
            ffprobe=ffmpeg.get_ffprobe_version(),
            database=self.app_data.db_file,
            config_file=self.config.source,
            auth=auth,
            config=self.config.model_dump_json(indent=2, exclude={"auth"}),
        )
        logger.debug(stats)


@dataclasses.dataclass(slots=True, frozen=True)
class LogsManager:
    config: Config
    task_group: asyncio.TaskGroup = dataclasses.field(repr=False)
    _has_headers: set[Path] = dataclasses.field(init=False, default_factory=set)
    _locks: defaultdict[Path, asyncio.Lock] = dataclasses.field(
        init=False, default_factory=lambda: defaultdict(asyncio.Lock)
    )

    async def write_jsonl(self, data: Iterable[dict[str, Any]]) -> None:
        async with self._locks[self.config.logs.jsonl_file]:
            await asyncio.to_thread(json.dump_jsonl, data, self.config.logs.jsonl_file)

    async def _write_to_csv(self, file: Path, **kwargs: Any) -> None:
        """Write to the specified csv file. kwargs are columns for the CSV."""
        async with self._locks[file]:
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

    def write_unsupported(self, url: URL, origin: ScrapeItem | URL | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(self.config.logs.unsupported, url=url, origin=get_origin(origin))
        )

    def write_download_error(self, media_item: MediaItem, error_message: str) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.download_errors,
                url=media_item.url,
                error=error_message,
                referer=media_item.referer,
                origin=get_origin(media_item),
            )
        )

    def write_scrape_error(self, url: URL | str, error_message: str, origin: URL | Path | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.scrape_errors,
                url=url,
                error=error_message,
                origin=origin,
            )
        )


@dataclasses.dataclass(slots=True)
class AppData:
    path: Path
    cache_file: Path = dataclasses.field(init=False)
    default_config: Path = dataclasses.field(init=False)
    db_file: Path = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.cache_file = self.path / "cache.yaml"
        self.default_config = self.path / "config.yaml"
        self.db_file = self.path / "cyberdrop.db"

    def __fspath__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return str(self.path)

    def mkdirs(self) -> None:
        for dir in (self.path,):
            dir.mkdir(parents=True, exist_ok=True)
