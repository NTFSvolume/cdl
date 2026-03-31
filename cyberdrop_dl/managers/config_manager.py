from __future__ import annotations

from dataclasses import field
from time import sleep
from typing import TYPE_CHECKING

from cyberdrop_dl import yaml
from cyberdrop_dl.config import AuthSettings, ConfigSettings, GlobalSettings
from cyberdrop_dl.exceptions import InvalidYamlError
from cyberdrop_dl.managers.logs import LogManager
from cyberdrop_dl.utils.apprise import get_apprise_urls

if TYPE_CHECKING:
    from pathlib import Path

    from pydantic import BaseModel

    from cyberdrop_dl.managers.manager import Manager
    from cyberdrop_dl.utils.apprise import AppriseURL


class ConfigManager:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        self.loaded_config: str = ""

        self.authentication_settings: Path = field(init=False)
        self.settings: Path = field(init=False)
        self.global_settings: Path = field(init=False)
        self.deep_scrape: bool = False
        self.apprise_urls: list[AppriseURL] = []

        self.authentication_data: AuthSettings = field(init=False)
        self.settings_data: ConfigSettings = field(init=False)
        self.global_settings_data: GlobalSettings = field(init=False)
        self.pydantic_config: str | None = None
        self.apprise_file: Path

    def startup(self) -> None:
        """Startup process for the config manager."""
        self.loaded_config = self.get_default_config()
        self.settings = self.manager.path_manager.config_folder / self.loaded_config / "settings.yaml"
        self.global_settings = self.manager.path_manager.config_folder / "global_settings.yaml"
        self.authentication_settings = self.manager.path_manager.config_folder / "authentication.yaml"
        auth_override = self.manager.path_manager.config_folder / self.loaded_config / "authentication.yaml"

        if auth_override.is_file():
            self.authentication_settings = auth_override

        self.settings.parent.mkdir(parents=True, exist_ok=True)
        self.pydantic_config = self.manager.cache_manager.get("pydantic_config")
        self.load_configs()

    def get_default_config(self) -> str:
        return self.manager.cache_manager.get("default_config") or "Default"

    def load_configs(self) -> None:
        """Loads all the configs."""
        self._load_authentication_config()
        self._load_global_settings_config()
        self._load_settings_config()
        self.apprise_file = self.manager.path_manager.config_folder / self.loaded_config / "apprise.txt"
        self.apprise_urls = get_apprise_urls(file=self.apprise_file)

    @staticmethod
    def get_model_fields(model: BaseModel, *, exclude_unset: bool = True) -> set[str]:
        fields = set()
        default_dict: dict = model.model_dump(exclude_unset=exclude_unset)
        for submodel_name, submodel in default_dict.items():
            for field_name in submodel:
                fields.add(f"{submodel_name}.{field_name}")
        return fields

    def _load_authentication_config(self) -> None:
        """Verifies the authentication config file and creates it if it doesn't exist."""
        needs_update = _is_in_file("socialmediagirls_username:", self.authentication_settings)
        posible_fields = self.get_model_fields(AuthSettings(), exclude_unset=False)
        if self.authentication_settings.is_file():
            self.authentication_data = AuthSettings.model_validate(yaml.load(self.authentication_settings))
            set_fields = self.get_model_fields(self.authentication_data)
            if posible_fields == set_fields and not needs_update and self.pydantic_config:
                return

        else:
            self.authentication_data = AuthSettings()

        yaml.save(self.authentication_settings, self.authentication_data)

    def _load_settings_config(self) -> None:
        """Verifies the settings config file and creates it if it doesn't exist."""
        needs_update = _is_in_file("download_error_urls_filename:", self.settings)
        posible_fields = self.get_model_fields(ConfigSettings(), exclude_unset=False)
        if self.manager.parsed_args.cli_only_args.config_file:
            self.settings = self.manager.parsed_args.cli_only_args.config_file
            self.loaded_config = "CLI-Arg Specified"

        if self.settings.is_file():
            self.settings_data = ConfigSettings.model_validate(yaml.load(self.settings))
            set_fields = self.get_model_fields(self.settings_data)
            self.deep_scrape = self.settings_data.runtime_options.deep_scrape
            self.settings_data.runtime_options.deep_scrape = False
            if posible_fields == set_fields and not needs_update and self.pydantic_config:
                return
        else:
            self.settings_data = ConfigSettings()
            self.settings_data.files.input_file = (
                self.manager.path_manager.appdata / "Configs" / self.loaded_config / "URLs.txt"
            )
            downloads = self.manager.path_manager.cwd / "Downloads"
            self.settings_data.sorting.sort_folder = downloads / "Cyberdrop-DL Sorted Downloads"
            self.settings_data.files.download_folder = downloads / "Cyberdrop-DL Downloads"
            self.settings_data.logs.log_folder = (
                self.manager.path_manager.appdata / "Configs" / self.loaded_config / "Logs"
            )

        yaml.save(self.settings, self.settings_data)

    def _load_global_settings_config(self) -> None:
        """Verifies the global settings config file and creates it if it doesn't exist."""
        needs_update = _is_in_file("Dupe_Cleanup_Options:", self.global_settings)
        posible_fields = self.get_model_fields(GlobalSettings(), exclude_unset=False)
        if self.global_settings.is_file():
            self.global_settings_data = GlobalSettings.model_validate(yaml.load(self.global_settings))
            set_fields = self.get_model_fields(self.global_settings_data)
            if posible_fields == set_fields and not needs_update and self.pydantic_config:
                return
        else:
            self.global_settings_data = GlobalSettings()

        yaml.save(self.global_settings, self.global_settings_data)

    def reload_config(self) -> None:
        self.startup()
        self.manager.path_manager.startup()
        sleep(1)
        self.manager.logs = LogManager.from_manager(self.manager)
        sleep(1)


def _is_in_file(search_value: str, file: Path) -> bool:
    if not file.is_file():
        return False
    try:
        return search_value.casefold() in file.read_text(encoding="utf8").casefold()
    except Exception as e:
        raise InvalidYamlError(file, e) from e
