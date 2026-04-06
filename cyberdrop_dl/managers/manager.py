from __future__ import annotations

import asyncio
import dataclasses
import logging
from dataclasses import Field, field
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Self, TypeVar

from pydantic import BaseModel

from cyberdrop_dl import __version__, ffmpeg, yaml
from cyberdrop_dl.cli import ParsedArgs, parse_args
from cyberdrop_dl.database import Database
from cyberdrop_dl.hasher import Hasher
from cyberdrop_dl.managers.client_manager import ClientManager
from cyberdrop_dl.managers.config_manager import ConfigManager
from cyberdrop_dl.managers.live_manager import LiveManager
from cyberdrop_dl.managers.logs import LogManager
from cyberdrop_dl.managers.progress_manager import ProgressManager
from cyberdrop_dl.utils import filepath
from cyberdrop_dl.utils.utilities import get_system_information

if TYPE_CHECKING:
    from asyncio import TaskGroup
    from collections.abc import Sequence
    from os import PathLike

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.scraper.scrape_mapper import ScrapeMapper


logger = logging.getLogger(__name__)


class Manager:
    def __init__(self, args: Sequence[str] | None = None) -> None:
        if isinstance(args, str):
            args = [args]

        self.parsed_args: ParsedArgs = field(init=False)
        self.cache: dict[str, Any] = {}
        self.config_manager: ConfigManager = field(init=False)

        self.logs: LogManager = field(init=False)
        self.database: Database = field(init=False)
        self.client_manager: ClientManager = field(init=False)

        self.progress_manager: ProgressManager = field(init=False)
        self.live_manager: LiveManager = field(init=False)

        self._loaded_args_config: bool = False
        self._made_portable: bool = False

        self.task_group: TaskGroup = asyncio.TaskGroup()
        self.scrape_mapper: ScrapeMapper = field(init=False)

        self.start_time: float = perf_counter()
        self.downloaded_data: int = 0
        self.args = args

        self._appdata: AppData | None = None
        self._completed_downloads: list[MediaItem] = []
        self.hasher: Hasher = Hasher(self)

    @property
    def config(self):
        return self.config_manager.settings_data

    @property
    def auth_config(self):
        return self.config_manager.authentication_data

    @property
    def global_config(self):
        return self.config_manager.global_settings_data

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

    def startup(self) -> None:
        """Startup process for the manager."""

        if isinstance(self.parsed_args, Field):
            self.parsed_args = parse_args(self.args)

        self.appdata.mkdirs()

        self.config_manager = ConfigManager(self)
        self.config_manager.startup()

        self.args_consolidation()
        self.config.resolve_paths()
        self.logs = LogManager.from_manager(self)

    def add_completed(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        self._completed_downloads.append(media_item)

    @property
    def completed_downloads(self) -> list[MediaItem]:
        return self._completed_downloads

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def async_startup(self) -> None:
        """Async startup process for the manager."""

        self.args_logging()
        self.async_db_hash_startup()

        self.client_manager = ClientManager(self)
        await self.client_manager.startup()

        filepath.MAX_FILE_LEN.set(self.config_manager.global_settings_data.general.max_file_name_length)
        filepath.MAX_FOLDER_LEN.set(self.config_manager.global_settings_data.general.max_folder_name_length)

    def async_db_hash_startup(self) -> None:

        self.database = Database(
            self.appdata.db_file,
            self.config.runtime_options.ignore_history,
        )

        self.live_manager = LiveManager(self)
        self.progress_manager = ProgressManager(self)

    def process_additive_args(self) -> None:
        cli_general_options = self.parsed_args.global_settings.general
        cli_ignore_options = self.parsed_args.config_settings.ignore_options
        config_ignore_options = self.config_manager.settings_data.ignore_options
        config_general_options = self.config_manager.global_settings_data.general

        add_or_remove_lists(cli_ignore_options.skip_hosts, config_ignore_options.skip_hosts)
        add_or_remove_lists(cli_ignore_options.only_hosts, config_ignore_options.only_hosts)
        add_or_remove_lists(cli_general_options.disable_crawlers, config_general_options.disable_crawlers)

    def args_consolidation(self) -> None:
        """Consolidates runtime arguments with config values."""
        self.process_additive_args()

        conf = merge_models(self.config_manager.settings_data, self.parsed_args.config_settings)
        global_conf = merge_models(self.config_manager.global_settings_data, self.parsed_args.global_settings)
        deep_scrape = self.parsed_args.config_settings.runtime_options.deep_scrape or self.config_manager.deep_scrape

        self.config_manager.settings_data = conf
        self.config_manager.global_settings_data = global_conf
        self.config_manager.deep_scrape = deep_scrape

    def args_logging(self) -> None:
        """Logs the runtime arguments."""

        auth = {
            site: all(credentials.values())
            for site, credentials in self.config_manager.authentication_data.model_dump().items()
        }

        config_settings = self.config_manager.settings_data.model_copy()
        config_settings.runtime_options.deep_scrape = self.config_manager.deep_scrape

        logger.info(f"Running cyberdrop-dl v{__version__}")

        args_info = {
            "System": get_system_information(),
            "Config": self.config_manager.loaded_config,
            "Config File": self.config_manager.settings,
            "Input File": self.config.files.input_file,
            "Download Folder": self.config.files.download_folder,
            "Database File": self.appdata.db_file,
            "CLI only options": self.parsed_args.cli_only_args.model_dump(mode="json"),
            "Auth": auth,
            "Settings": config_settings.model_dump(mode="json"),
            "Global Settings": self.config_manager.global_settings_data.model_dump(mode="json"),
            "ffmpeg version": ffmpeg.get_ffmpeg_version(),
            "ffprobe version": ffmpeg.get_ffprobe_version(),
        }
        logger.info(args_info)

    async def close(self) -> None:

        await self.client_manager.close()

    @property
    def appdata(self) -> AppData:
        if self._appdata is None:
            if folder := self.parsed_args.cli_only_args.appdata_folder:
                path = folder / "AppData"
            else:
                path = Path("AppData")

            self._appdata = AppData.from_path(path.resolve())

        return self._appdata


def add_or_remove_lists(cli_values: list[str], config_values: list[str]) -> None:
    exclude = {"+", "-"}
    if cli_values:
        if cli_values[0] == "+":
            new_values_set = set(config_values + cli_values)
            cli_values.clear()
            cli_values.extend(sorted(new_values_set - exclude))
        elif cli_values[0] == "-":
            new_values_set = set(config_values) - set(cli_values)
            cli_values.clear()
            cli_values.extend(sorted(new_values_set - exclude))


def merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    for key, val in dict1.items():
        if isinstance(val, dict):
            if key in dict2 and isinstance(dict2[key], dict):
                merge_dicts(dict1[key], dict2[key])
        else:
            if key in dict2:
                dict1[key] = dict2[key]

    for key, val in dict2.items():
        if key not in dict1:
            dict1[key] = val

    return dict1


M = TypeVar("M", bound=BaseModel)


def merge_models(default: M, new: M) -> M:
    default_dict = default.model_dump()
    new_dict = new.model_dump(exclude_unset=True)

    updated_dict = merge_dicts(default_dict, new_dict)
    return default.model_validate(updated_dict)


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AppData:
    path: Path
    cache_file: Path
    db_file: Path
    config_file: Path

    cache: Path
    cookies: Path
    configs: Path

    @classmethod
    def from_path(cls, path: Path) -> Self:
        assert path.is_absolute()
        cache = path / "Cache"
        return cls(
            path=path,
            cache=cache,
            configs=path / "Configs",
            cookies=path / "Cookies",
            config_file=path / "config.yaml",
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
