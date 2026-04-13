from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from cyberdrop_dl import __version__, ffmpeg, yaml
from cyberdrop_dl.cli import CLIargs
from cyberdrop_dl.config import Config
from cyberdrop_dl.database import Database
from cyberdrop_dl.hasher import Hasher
from cyberdrop_dl.managers.client_manager import ClientManager
from cyberdrop_dl.managers.live_manager import LiveManager
from cyberdrop_dl.managers.logs import LogManager
from cyberdrop_dl.managers.progress_manager import ProgressManager
from cyberdrop_dl.utils.utilities import get_system_information

if TYPE_CHECKING:
    from os import PathLike

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.scrape_mapper import ScrapeMapper


logger = logging.getLogger(__name__)


class Manager:
    def __init__(
        self,
        cli_args: CLIargs | None = None,
        appdata: AppData | None = None,
        config: Config | None = None,
    ) -> None:
        self.cache: dict[str, Any] = {}

        self.appdata: AppData = appdata or AppData.default()
        self.cli_args: CLIargs = cli_args or CLIargs()
        self.config: Config = config or Config.from_manager(self)

        self._completed_downloads: list[MediaItem] = []
        self.hasher: Hasher = Hasher(self)
        self.logs: LogManager = LogManager.from_manager(self)
        self.live_manager: LiveManager = LiveManager(self)
        self.progress_manager: ProgressManager = ProgressManager(self)

        self.scrape_mapper: ScrapeMapper
        self.database: Database
        self.client_manager: ClientManager

    def resolve_paths(self) -> None:
        self.appdata.mkdirs()
        self.config.settings.resolve_paths()
        self.logs = LogManager.from_manager(self)
        self.logs.delete_old_logs()

    async def __aenter__(self) -> Self:
        cache_file = self.appdata.cache_file
        try:
            self.cache.update(yaml.load(cache_file))
        except FileNotFoundError:
            cache_file.parent.mkdir(exist_ok=True, parents=True)
            cache_file.touch()
        return self

    async def __aexit__(self, *_) -> None:
        self.cache["version"] = __version__
        yaml.save(self.appdata.cache_file, self.cache)

    def add_completed(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        self._completed_downloads.append(media_item)

    @property
    def completed_downloads(self) -> list[MediaItem]:
        return self._completed_downloads

    async def async_startup(self) -> None:
        self._log_config_settings()
        self.async_db_hash_startup()

        self.client_manager = ClientManager(self)
        await self.client_manager.startup()

    def async_db_hash_startup(self) -> None:
        self.database = Database(
            self.appdata.db_file,
            self.config.settings.runtime_options.ignore_history,
        )

    def _log_config_settings(self) -> None:
        auth = {site: all(credentials.values()) for site, credentials in self.config.auth.model_dump().items()}
        config_settings = self.config.settings.model_copy()
        config_settings.runtime_options.deep_scrape = self.config.deep_scrape

        logger.info(f"Running cyberdrop-dl v{__version__}")

        args_info = {
            "System": get_system_information(),
            "Config File": self.config.source,
            "Input File": self.config.settings.files.input_file,
            "Download Folder": self.config.settings.files.download_folder,
            "Database File": self.appdata.db_file,
            "CLI only options": self.cli_args.model_dump(mode="json"),
            "Auth": auth,
            "Settings": config_settings.model_dump(mode="json"),
            "Global Settings": self.config.global_settings.model_dump(mode="json"),
        }
        logger.debug(args_info)
        logger.debug("ffmpeg version: %s", ffmpeg.get_ffmpeg_version())
        logger.debug("ffprobe version: %s", ffmpeg.get_ffprobe_version())

    async def close(self) -> None:
        await self.client_manager.close()


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AppData:
    path: Path
    cache_file: Path
    config_file: Path
    db_file: Path

    cache: Path
    cookies: Path
    configs: Path

    @classmethod
    def default(cls) -> Self:
        return cls.from_path(Path.cwd())

    @classmethod
    def from_path(cls, path: Path) -> Self:
        path = path.expanduser().resolve().absolute() / "AppData"
        cache = path / "Cache"
        configs = path / "Configs"
        return cls(
            path=path,
            cache=cache,
            configs=configs,
            cookies=path / "Cookies",
            config_file=configs / "Default" / "settings.yaml",
            cache_file=cache / "cache.yaml",
            db_file=cache / "cyberdrop.db",
        )

    def __truediv__(self, other: PathLike[str]):
        try:
            return self.path / other
        except TypeError:
            return NotImplemented

    def __fspath__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return str(self.path)

    def mkdirs(self) -> None:
        for dir in (self.cache, self.configs, self.cookies):
            dir.mkdir(parents=True, exist_ok=True)
