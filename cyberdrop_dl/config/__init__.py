from __future__ import annotations

from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, Self, TypeVar

from cyclopts import Parameter
from pydantic import BaseModel, Field

from cyberdrop_dl import yaml
from cyberdrop_dl.config.merge import merge_models
from cyberdrop_dl.models import AppriseURL  # noqa: TC001
from cyberdrop_dl.utils.apprise import read_apprise_urls

from ._global import GlobalSettings
from .auth import AuthSettings
from .settings import ConfigSettings

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import AppData, Manager

    _BaseModelT = TypeVar("_BaseModelT", bound=BaseModel)


@Parameter(name="*")
class Config(BaseModel):
    source: Path | None = None

    auth: AuthSettings = Field(default_factory=AuthSettings)
    settings: ConfigSettings = Field(default_factory=ConfigSettings)
    global_settings: GlobalSettings = Field(default_factory=GlobalSettings)

    deep_scrape: bool = False
    apprise_urls: tuple[AppriseURL, ...] = ()

    @classmethod
    def create(cls, appdata: AppData, config_file: Path | None = None) -> Self:
        apprise_file = appdata.configs / "apprise.txt"
        global_settings = appdata.configs / "global_settings.yaml"
        auth_file = appdata.configs / "authentication.yaml"
        config_file = config_file or appdata.config_file

        return cls(
            source=config_file,
            auth=_load_config_file(auth_file, AuthSettings),
            settings=_load_config_file(config_file, ConfigSettings),
            global_settings=_load_config_file(global_settings, GlobalSettings),
            apprise_urls=read_apprise_urls(apprise_file),
        )

    @classmethod
    def from_manager(cls, manager: Manager) -> Self:
        return cls.create(manager.appdata, manager.cli_args.config_file)

    def update(self, other: Self) -> Self:
        return merge_models(self, other)


def _load_config_file(file: Path, model: type[_BaseModelT]) -> _BaseModelT:
    try:
        content = yaml.load(file)
    except FileNotFoundError:
        default = model()
        yaml.save(file, default)
        return default
    else:
        return model.model_validate(content)


__all__ = ["AuthSettings", "Config", "ConfigSettings", "GlobalSettings"]
