from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from cyberdrop_dl import env

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

        now = datetime.now()
        settings_data.logs._set_output_filenames(now)
        settings_data.logs._delete_old_logs_and_folders(now)
        settings_data.logs.mkdirs()
