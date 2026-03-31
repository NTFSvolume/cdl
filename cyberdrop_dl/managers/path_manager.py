from __future__ import annotations

import os
from dataclasses import field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from cyberdrop_dl import env
from cyberdrop_dl.utils.utilities import purge_dir_tree

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager


def cwd() -> Path:
    path = Path.cwd().resolve()
    if env.RUNNING_IN_IDE and path.name == "cyberdrop_dl":
        return path.parent
    return path


class PathManager:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager

        self.download_folder: Path = field(init=False)
        self.sorted_folder: Path = field(init=False)
        self.scan_folder: Path | None = None

        self.log_folder: Path = field(init=False)

        self.input_file: Path = field(init=False)

        self.main_log: Path = field(init=False)

        self.pages_folder: Path = field(init=False)

    def startup(self) -> None:
        settings_data = self.manager.config_manager.settings_data
        current_config = self.manager.config_manager.loaded_config

        here = cwd()

        def replace(path: Path) -> Path:
            path_w_config = str(path).replace("{config}", current_config)
            if os.name == "nt":
                return (here / path_w_config).resolve()
            normalized_path_str = path_w_config.replace("\\", "/")
            return (here / normalized_path_str).resolve()

        self.download_folder = replace(settings_data.files.download_folder)
        self.sorted_folder = replace(settings_data.sorting.sort_folder)
        self.log_folder = replace(settings_data.logs.log_folder)
        self.input_file = replace(settings_data.files.input_file)

        if settings_data.sorting.scan_folder:
            self.scan_folder = replace(settings_data.sorting.scan_folder)

        self.log_folder.mkdir(parents=True, exist_ok=True)

        now = datetime.now()
        self.manager.config_manager.settings_data.logs._set_output_filenames(now)
        self.pages_folder = self.main_log.parent / "cdl_responses"
        self.manager.config_manager.settings_data.logs._delete_old_logs_and_folders(now)
        self.manager.config_manager.settings_data.logs.mkdirs()
        purge_dir_tree(self.log_folder)

        if not self.input_file.is_file():
            self.input_file.touch(exist_ok=True)
