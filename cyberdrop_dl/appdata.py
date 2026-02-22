from __future__ import annotations

import dataclasses
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

_appdata: ContextVar[AppData] = ContextVar("_appdata")


@dataclasses.dataclass(slots=True)
class AppData:
    path: Path
    cookies_dir: Path = dataclasses.field(init=False)
    cache_file: Path = dataclasses.field(init=False)
    default_config: Path = dataclasses.field(init=False)
    db_file: Path = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.cookies_dir = self.path / "cookies"
        self.cache_file = self.path / "cache.yaml"
        self.default_config = self.path / "config.yaml"
        self.db_file = self.path / "cyberdrop.db"

    def __fspath__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return str(self.path)

    def mkdirs(self) -> None:
        for dir in (self.cookies_dir,):
            dir.mkdir(parents=True, exist_ok=True)


def get() -> AppData:
    return _appdata.get()
