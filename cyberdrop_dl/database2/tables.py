# ruff: noqa: C408
from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING, ClassVar, TypeAlias

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping


Properties: TypeAlias = tuple[str, ...]


@dataclasses.dataclass(slots=True)
class Table:
    name: ClassVar[str]
    columns: ClassVar[Mapping[str, Properties]]
    column_names: ClassVar[set[str]]
    primary_keys: ClassVar[set[str]]
    foreign: ClassVar[Mapping[str, Properties]] = {}

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r}, columns={self.columns!r}, foreign={self.foreign!r})"

    def __init_subclass__(cls, *, name: str | None = None, **_) -> None:
        cls.name = name or cls.__name__.casefold()
        cls.columns = {k: tuple(map(str.upper, v)) for k, v in cls.columns.items()}
        cls.column_names = set(cls.columns.keys())
        cls.primary_keys = {k for k, v in cls.columns.items() if "PRIMARY KEY" in v}

    def check_columns(self, other: Iterable[str]) -> None:
        assert self.column_names.issuperset(other), f"Invalid keys for table {self.name}. {tuple(other)}"


class History(Table, name="media"):
    columns: ClassVar[Mapping[str, Properties]] = dict(
        domain=("TEXT", "PRIMARY KEY", "NOT NULL"),
        url_path=("TEXT", "PRIMARY KEY", "NOT NULL"),
        referer=("TEXT", "NOT NULL"),
        album_id=("TEXT",),
        download_path=("TEXT", "NOT NULL"),
        download_filename=("TEXT", "NOT NULL"),
        original_filename=("TEXT", "PRIMARY KEY", "NOT NULL"),
        file_size=("INT",),
        duration=("FLOAT",),
        completed=("INTEGER", "NOT NULL", "DEFAULT 0"),
        created_at=("TIMESTAMP", "NOT NULL", "DEFAULT CURRENT_TIMESTAMP"),
        completed_at=("TIMESTAMP",),
    )


class Files(Table):
    columns: ClassVar[Mapping[str, Properties]] = dict(
        folder=("TEXT", "NOT NULL", "PRIMARY KEY"),
        download_filename=("TEXT", "NOT NULL", "PRIMARY KEY"),
        original_filename=("TEXT", "NOT NULL"),
        file_size=("INT",),
        referer=("TEXT", "NOT NULL"),
        date=("TIMESTAMP",),
    )


class Hash(Table):
    columns: ClassVar[Mapping[str, Properties]] = dict(
        folder=("TEXT", "NOT NULL", "PRIMARY KEY"),
        download_filename=("TEXT", "NOT NULL", "PRIMARY KEY"),
        hash_type=("TEXT", "NOT NULL", "PRIMARY KEY"),
        hash=("TEXT", "NOT NULL"),
    )
    foreign: ClassVar[Mapping[str, Properties]] = dict(
        files=("folder", "download_filename"),
    )


class Schema(Table, name="schema_version"):
    columns: ClassVar[Mapping[str, Properties]] = dict(
        version=("VARCHAR(50)", "PRIMARY KEY", "UNIQUE", "NOT NULL"),
        applied_on=("TIMESTAMP", "NOT NULL", "DEFAULT CURRENT_TIMESTAMP"),
    )


@dataclasses.dataclass(slots=True, frozen=True)
class Tables:
    history: History = dataclasses.field(default_factory=History)
    files: Files = dataclasses.field(default_factory=Files)
    hash: Hash = dataclasses.field(default_factory=Hash)
    schema: Schema = dataclasses.field(default_factory=Schema)

    def __iter__(self) -> Iterator[Table]:
        return iter(dataclasses.astuple(self))
