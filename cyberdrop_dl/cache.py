from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self

from cyberdrop_dl import __version__, yaml

if TYPE_CHECKING:
    from pathlib import Path


@dataclasses.dataclass(slots=True)
class Cache(Mapping[str, Any]):
    file: Path
    _cache: dict[str, Any] = dataclasses.field(init=False)
    _in_ctx: bool = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._cache = yaml.load(self.file, create=True)

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._cache)

    def __len__(self) -> int:
        return len(self._cache)

    def __delitem__(self, key: str) -> None:
        _ = self._cache.pop(key)
        self._save()

    def __setitem__(self, key: str, value: Any, /) -> None:
        self._cache[key] = value
        self._save()

    def __enter__(self) -> Self:
        self._in_ctx = True
        return self

    def __exit__(self, *_) -> None:
        self._in_ctx = False
        self["version"] = __version__

    def _save(self) -> None:
        if not self._in_ctx:
            yaml.dump(self.file, self._cache)

    def clear(self) -> None:
        self._cache.clear()
        self._save()
