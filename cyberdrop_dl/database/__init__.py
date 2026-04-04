from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Self

import aiosqlite

from .tables import HashTable, HistoryTable, SchemaVersionTable

if TYPE_CHECKING:
    from pathlib import Path


@dataclasses.dataclass(slots=True)
class Database:
    _db_path: Path
    ignore_history: bool

    history_table: HistoryTable = dataclasses.field(init=False)
    hash_table: HashTable = dataclasses.field(init=False)
    _schema_versions: SchemaVersionTable = dataclasses.field(init=False)
    _db_conn: aiosqlite.Connection = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.history_table = HistoryTable(self)
        self.hash_table = HashTable(self)
        self._schema_versions = SchemaVersionTable(self)

    async def __aenter__(self) -> Self:
        self._db_conn = await aiosqlite.connect(self._db_path, timeout=20)
        self._db_conn.row_factory = aiosqlite.Row
        await self._pre_allocate()
        await self.history_table.startup()
        await self.hash_table.startup()
        await self._schema_versions.startup()
        return self

    async def __aexit__(self, *_) -> None:
        await self._db_conn.close()

    async def _pre_allocate(self) -> None:
        """We pre-allocate 100MB of space to the SQL file just in case the user runs out of disk space."""

        pre_allocate_script = (
            "CREATE TABLE IF NOT EXISTS t(x);"
            "INSERT INTO t VALUES(zeroblob(100*1024*1024));"  # 100 MB
            "DROP TABLE t;"
        )

        free_pages_query = "PRAGMA freelist_count;"
        cursor = await self._db_conn.execute(free_pages_query)
        free_space = await cursor.fetchone()

        if free_space and free_space[0] <= 1024:
            await self._db_conn.executescript(pre_allocate_script)
            await self._db_conn.commit()


__all__ = ["Database"]
