from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import field
from time import perf_counter
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar

from pydantic import BaseModel
from typing_extensions import override

from cyberdrop_dl import __version__, aio, constants, ffmpeg, yaml
from cyberdrop_dl.cli import parse_args
from cyberdrop_dl.database import Database
from cyberdrop_dl.managers.client_manager import ClientManager
from cyberdrop_dl.managers.config_manager import ConfigManager
from cyberdrop_dl.managers.hash_manager import HashManager
from cyberdrop_dl.managers.live_manager import LiveManager
from cyberdrop_dl.managers.log_manager import LogManager
from cyberdrop_dl.managers.path_manager import PathManager
from cyberdrop_dl.managers.progress_manager import ProgressManager
from cyberdrop_dl.utils.logger import LogHandler, QueuedLogger, log
from cyberdrop_dl.utils.utilities import get_system_information

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from cyberdrop_dl.cli.model import ParsedArgs
    from cyberdrop_dl.scraper.scrape_mapper import ScrapeMapper


class AsyncioEvents(NamedTuple):
    SHUTTING_DOWN: asyncio.Event
    RUNNING: asyncio.Event


class Manager(aio.AsyncContextManagerMixin):
    def __init__(self, args: Sequence[str] | None = None) -> None:
        if isinstance(args, str):
            args = [args]

        self.hash_manager: HashManager

        self.db_manager: Database
        self.client_manager: ClientManager

        self.progress_manager: ProgressManager = field(init=False)
        self.live_manager: LiveManager = field(init=False)

        self.task_group: asyncio.TaskGroup
        self.scrape_mapper: ScrapeMapper

        self.start_time: float = perf_counter()

        self.loggers: dict[str, QueuedLogger] = {}
        self.states: AsyncioEvents

        constants.console_handler = LogHandler(level=constants.CONSOLE_LEVEL)

        self.parsed_args: ParsedArgs = parse_args(args)
        self.path_manager: PathManager = PathManager(self)
        self.path_manager.pre_startup()
        self.cache_manager: dict[str, Any] = yaml.load(self.path_manager.cache_folder / "cache.yaml")
        self.config_manager: ConfigManager = ConfigManager(self)
        self.config_manager.startup()

        self.args_consolidation()
        self.path_manager.startup()
        self.log_manager = LogManager(self)

    @property
    def config(self):
        return self.config_manager.settings_data

    @property
    def auth_config(self):
        return self.config_manager.authentication_data

    @property
    def global_config(self):
        return self.config_manager.global_settings_data

    @override
    async def _async_ctx_(self) -> None:
        self.states = AsyncioEvents(asyncio.Event(), asyncio.Event())
        self.args_logging()

        exit_stack = self._stack
        self.client_manager = await exit_stack.enter_async_context(ClientManager(self))

        _ = await exit_stack.enter_async_context(self.connect_to_db())

        constants.MAX_NAME_LENGTHS["FILE"] = self.config_manager.global_settings_data.general.max_file_name_length
        constants.MAX_NAME_LENGTHS["FOLDER"] = self.config_manager.global_settings_data.general.max_folder_name_length

        def save_cache() -> None:
            self.cache_manager["version"] = __version__
            yaml.save(self.path_manager.cache_folder / "cache.yaml", self.cache_manager)

        def close_loggers() -> None:
            while self.loggers:
                _, queued_logger = self.loggers.popitem()
                queued_logger.stop()

        exit_stack.callback(save_cache)
        exit_stack.callback(close_loggers)

    @contextlib.asynccontextmanager
    async def connect_to_db(self) -> AsyncGenerator[None]:
        self.db_manager = Database(
            self.path_manager.history_db,
            self.config.runtime_options.ignore_history,
        )

        self.hash_manager = HashManager(self)
        self.live_manager = LiveManager(self)
        self.progress_manager = ProgressManager(self)
        self.progress_manager.startup()
        async with self.db_manager:
            try:
                yield
            finally:
                del self.db_manager

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
        auth_provided = {}

        for site, auth_entries in self.config_manager.authentication_data.model_dump().items():
            auth_provided[site] = all(auth_entries.values())

        config_settings = self.config_manager.settings_data.model_copy()
        config_settings.runtime_options.deep_scrape = self.config_manager.deep_scrape
        config_settings = config_settings.model_dump_json(indent=4)
        global_settings = self.config_manager.global_settings_data.model_dump_json(indent=4)
        cli_only_args = self.parsed_args.cli_only_args.model_dump_json(indent=4)
        system_info = get_system_information()

        args_info = (
            "Starting Cyberdrop-DL Process",
            f"Running Version: {__version__}",
            f"System Info: {system_info}",
            f"Using Config: {self.config_manager.loaded_config}",
            f"Using Config File: {self.config_manager.settings}",
            f"Using Input File: {self.path_manager.input_file}",
            f"Using Download Folder: {self.path_manager.download_folder}",
            f"Using Database File: {self.path_manager.history_db}",
            f"Using CLI only options: {cli_only_args}",
            f"Using Authentication: \n{json.dumps(auth_provided, indent=4, sort_keys=True)}",
            f"Using Settings: \n{config_settings}",
            f"Using Global Settings: \n{global_settings}",
            f"Using ffmpeg version: {ffmpeg.get_ffmpeg_version()}",
            f"Using ffprobe version: {ffmpeg.get_ffprobe_version()}",
        )
        log("\n".join(args_info))


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
