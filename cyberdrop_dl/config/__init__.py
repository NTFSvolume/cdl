from __future__ import annotations

from contextvars import ContextVar, Token
from pathlib import Path
from typing import Self

from pydantic import BaseModel

from cyberdrop_dl.config.auth import AuthSettings
from cyberdrop_dl.config.settings import ConfigSettings
from cyberdrop_dl.models import Settings, get_model_fields

_config: ContextVar[Config] = ContextVar("_config")
_appdata: ContextVar[AppData] = ContextVar("_appdata")


class AppData:
    def __init__(self, path: Path) -> None:
        self.path: Path = path
        self.cookies_dir: Path = path / "cookies"
        self.cache_file: Path = path / "cache.yaml"
        self.default_config: Path = path / "config.yaml"
        self.db_file: Path = path / "cyberdrop.db"

    def __fspath__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({vars(self)!r})"

    def mkdirs(self) -> None:
        for dir in (self.cookies_dir,):
            dir.mkdir(parents=True, exist_ok=True)


class Config(Settings):
    auth: AuthSettings = AuthSettings()
    settings: ConfigSettings = ConfigSettings()

    _token: Token[Config] | None = None
    _resolved: bool = False

    def __enter__(self) -> Self:
        self._token = _config.set(self)
        return self

    def __exit__(self, *_) -> None:
        assert self._token is not None
        _config.reset(self._token)

    def save(self, file: Path) -> None:
        from cyberdrop_dl.utils import yaml

        yaml.save(file, self)

    def resolve_paths(self) -> None:
        if not self._resolved:
            self._resolve_paths(self)
        self._resolved = True

    @classmethod
    def _resolve_paths(cls, model: BaseModel) -> None:
        for name, value in vars(model).items():
            if isinstance(value, Path):
                setattr(model, name, value.resolve())

            elif isinstance(value, BaseModel):
                cls._resolve_paths(value)


def load(file: Path) -> Config:
    from cyberdrop_dl.utils import yaml

    default = Config()
    if not file.is_file():
        config = default
        overwrite = True

    else:
        all_fields = get_model_fields(default, exclude_unset=False)
        config = Config.model_validate(yaml.load(file))
        set_fields = get_model_fields(config)
        overwrite = all_fields != set_fields

    if overwrite:
        config.save(file)

    return config


def get() -> Config:
    return _config.get()
