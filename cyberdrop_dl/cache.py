from __future__ import annotations

import dataclasses
from collections.abc import Iterator, MutableMapping
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, Self

from cyberdrop_dl import __version__
from cyberdrop_dl.utils import yaml

if TYPE_CHECKING:
    from pathlib import Path

_cache: ContextVar[Cache] = ContextVar("_cache")


@dataclasses.dataclass(slots=True)
class Cache(MutableMapping[str, Any]):
    file: Path
    _cache: dict[str, Any] = dataclasses.field(init=False)
    _token: Token[Cache] | None = None

    def __post_init__(self) -> None:
        self._cache = yaml.load(self.file)

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._cache)

    def __len__(self) -> int:
        return len(self._cache)

    def __delitem__(self, key: str) -> None:
        try:
            _ = self._cache.pop(key)
        except KeyError:
            pass
        else:
            self._save()

    def __setitem__(self, key: str, value: Any, /) -> None:
        self._cache[key] = value
        self._save()

    def __enter__(self) -> Self:
        self._token = _cache.set(self)
        return self

    def __exit__(self, *_) -> None:
        assert self._token is not None
        self._token = _cache.reset(self._token)
        self.close()

    def _save(self) -> None:
        if self._token is None:
            yaml.save(self.file, self._cache)

    def close(self) -> None:
        self["version"] = __version__


def get():
    return _cache.get()
