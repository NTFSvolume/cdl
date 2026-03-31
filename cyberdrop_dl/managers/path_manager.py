from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager


class PathManager:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager

    def startup(self) -> None:
        self.manager.config_manager.settings_data.resolve_paths()
