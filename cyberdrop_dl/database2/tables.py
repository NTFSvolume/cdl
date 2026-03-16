import dataclasses
import datetime
import logging
from collections.abc import Generator, Iterable
from typing import Any, ClassVar, Self, get_args

import aiosqlite

logger = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class Reference:
    table: str
    column: str
    on_delete: str

    def __str__(self) -> str:
        return f"REFERENCES {self.table}({self.column}) ON DELETE {self.on_delete}"


PK = {"PK": True}
AUTOINCREMENT = {"AUTOINCREMENT": True}


def REFERENCE(table: str, column: str, on_delete: str = "CASCADE") -> dict[str, Reference]:  # noqa: N802
    return {"REFERENCE": Reference(table, column, on_delete)}


_type_map: dict[type[Any], str] = {
    int: "INTEGER",
    float: "FLOAT",
    str: "TEXT",
    datetime.datetime: "DATETIME",
}


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


@dataclasses.dataclass(slots=True)
class Table:
    __table_name__: ClassVar[str]
    COLUMNS: ClassVar[set[str]]
    UNIQUE: ClassVar[tuple[str, ...]] = ()

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.__table_name__!r}, columns={self.COLUMNS!r})"

    def __init_subclass__(cls) -> None:
        cls.__table_name__ = getattr(cls, "__table_name__", None) or cls.__name__.lower()
        cls.COLUMNS = {f.name for f in dataclasses.fields(cls)}

    def check_columns(self, other: Iterable[str]) -> None:
        assert self.COLUMNS.issuperset(other), f"Invalid keys for table {self.__table_name__}. {tuple(other)}"

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> Self:
        return cls(**{name: row[name] for name in cls.COLUMNS})

    @classmethod
    def to_sql_schema(cls) -> str:
        joined_columns = ",\n".join(cls._parse_columns())
        sql = f"CREATE TABLE IF NOT EXISTS {cls.__table_name__} (\n{joined_columns}"
        if cls.UNIQUE:
            sql += f",\nUNIQUE({', '.join(cls.UNIQUE)})"
        return sql + "\n);"

    @classmethod
    def _parse_columns(cls) -> Generator[str]:
        for field in dataclasses.fields(cls):
            # This only work if we do not use __future__ annotations
            if isinstance(field.type, type):
                python_type = field.type
            else:
                python_type, *_ = get_args(field.type)

            sql_type = _type_map[python_type]
            column = f"{field.name} {sql_type}"

            if field.metadata.get("PK"):
                column += " PRIMARY KEY"

            elif field.default is not None:
                column += " NOT NULL"

            if field.metadata.get("AUTOINCREMENT"):
                column += " AUTOINCREMENT"

            if reference := field.metadata.get("REFERENCE"):
                column += f" {reference}"

            if field.default_factory is _now:
                column += " DEFAULT (datetime('now'))"

            yield column


@dataclasses.dataclass(slots=True)
class Media(Table):
    id: int = dataclasses.field(metadata=PK | AUTOINCREMENT)
    domain: str
    url_path: str
    referer: str
    name: str
    album_id: str | None = None
    size: int | None = None
    duration: float | None = None
    created_at: datetime.datetime = dataclasses.field(default_factory=_now)

    UNIQUE: ClassVar[tuple[str, ...]] = "domain", "url_path"


@dataclasses.dataclass(slots=True)
class Downloads(Table):
    id: int = dataclasses.field(metadata=PK | AUTOINCREMENT)
    media_id: int = dataclasses.field(metadata=REFERENCE("media", "id", "CASCADE"))
    folder: str
    file_name: str
    original_file_name: str
    created_at: datetime.datetime = dataclasses.field(default_factory=_now)
    completed_at: datetime.datetime | None = None


@dataclasses.dataclass(slots=True)
class Files(Table):
    """Table of files that exists on disk"""

    id: int = dataclasses.field(metadata=PK | AUTOINCREMENT)
    folder: str
    name: str
    size: int
    modtime: datetime.datetime | None = None

    UNIQUE: ClassVar[tuple[str, ...]] = "folder", "name"


@dataclasses.dataclass(slots=True)
class Hash(Table):
    file_id: int = dataclasses.field(metadata=REFERENCE("files", "id", "CASCADE"))
    algorithm: str
    hash: str

    UNIQUE: ClassVar[tuple[str, ...]] = "file_id", "algorithm", "hash"


@dataclasses.dataclass(slots=True)
class Schema(Table):
    __table_name__: ClassVar[str] = "schema_version"
    version: str = dataclasses.field(metadata=PK)
    applied_on: datetime.datetime = dataclasses.field(default_factory=_now)

    UNIQUE: ClassVar[tuple[str, ...]] = ("version",)


TABLES = (Media, Downloads, Files, Hash, Schema)

if __name__ == "__main__":
    for table in TABLES:
        print("")  # noqa: T201
        print(table.to_sql_schema())  # noqa: T201
