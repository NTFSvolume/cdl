from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, TypeAlias, cast

import aiosqlite
from packaging.version import Version

from cyberdrop_dl.database2 import query
from cyberdrop_dl.database2.tables import Table, Tables

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    import datetime
    from collections.abc import AsyncGenerator, Iterable
    from sqlite3 import Row

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem


Properties: TypeAlias = tuple[str, ...]


_current_db: ContextVar[Database] = ContextVar("_db")
_FETCH_MANY_SIZE: int = 1000
CURRENT_APP_SCHEMA_VERSION = "8.10.0"
MIN_REQUIRED_VERSION = "8.10.0"

create_hash_index = """
CREATE INDEX IF NOT EXISTS idx_hash_type_hash ON hash (hash_type, hash);
"""


@dataclasses.dataclass(slots=True)
class Database:
    db_path: Path
    ignore_history: bool

    conn: aiosqlite.Connection = dataclasses.field(init=False)
    tables: Tables = dataclasses.field(default_factory=Tables)

    async def connect(self) -> None:
        exists = self.db_path.exists()
        self.conn = await aiosqlite.connect(self.db_path, timeout=20)
        self.conn.row_factory = aiosqlite.Row

        if exists:
            await self._check()

        await self._pre_allocate()
        for table in self.tables:
            await self._create(table)

        await self._update()

    async def fetchone(self, query: str, parameters: Iterable[Any] | None = None) -> Row | None:
        cursor = await self.conn.execute(query, parameters)
        return await cursor.fetchone()

    async def fetchall(self, query: str, parameters: Iterable[Any] | None = None) -> list[Row]:
        return await self.conn.execute_fetchall(query, parameters)  # pyright: ignore[reportReturnType]

    async def commit(self, query: str, parameters: Iterable[Any] | None = None) -> None:
        _ = await self.conn.execute(query, parameters)
        await self.conn.commit()

    async def close(self) -> None:
        await self.conn.close()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def _pre_allocate(self) -> None:
        """We pre-allocate 100MB of space to the SQL file just in case the user runs out of disk space."""

        free_space = await self.fetchone("PRAGMA freelist_count;")
        assert free_space is not None

        if free_space[0] > 1024:
            return

        pre_allocate_script = (
            "CREATE TABLE IF NOT EXISTS t(x);"
            "INSERT INTO t VALUES(zeroblob(100*1024*1024));"  # 100 MB
            "DROP TABLE t;"
        )
        _ = await self.conn.executescript(pre_allocate_script)
        await self.conn.commit()

    async def _get_version(self) -> Version | None:
        sql, _ = query.select(self.tables.schema, "schema_version", limit=1)
        if result := await self.fetchone(sql + " ORDER BY ROWID DESC"):
            return Version(result["version"])

    async def _check(self) -> None:
        logger.info(f"Expected database schema version: {CURRENT_APP_SCHEMA_VERSION}")
        version = await self._get_version()
        logger.info(f"Database reports installed version: {version}")
        if version is None or version < Version(MIN_REQUIRED_VERSION):
            raise RuntimeError("Unsupported database version")

    async def _update(self) -> None:
        version = await self._get_version()
        if version is not None and version >= Version(CURRENT_APP_SCHEMA_VERSION):
            return

        # TODO: on v9, raise SystemExit if db version is None or older than 8.0.0
        logger.info(f"Updating database version to {CURRENT_APP_SCHEMA_VERSION}")
        sql, params = query.insert(self.tables.schema, version=CURRENT_APP_SCHEMA_VERSION)
        await self.commit(sql, params)

    async def _create(self, table: Table) -> None:
        await self.commit(query.create(table, **table.foreign))


@contextlib.asynccontextmanager
async def connect(db_path: Path, ignore_history: bool) -> AsyncGenerator[Database]:
    async with Database(db_path, ignore_history) as db:
        token = _current_db.set(db)
        try:
            yield db
        finally:
            _current_db.reset(token)


async def check_complete(domain: str, url: AbsoluteHttpURL, referer: AbsoluteHttpURL, db_path: str) -> bool:
    """Checks whether an individual file has completed given its domain and url path."""
    db = _current_db.get()
    if db.ignore_history:
        return False

    async def get_referer_and_completed() -> tuple[str, bool]:
        sql, params = query.select(db.tables.history, "referer", "completed", domain=domain, url_path=db_path)
        if row := await db.fetchone(sql, params):
            return row["referer"], bool(row["completed"])
        return "", False

    current_referer, completed = await get_referer_and_completed()
    if completed and url != referer and str(referer) != current_referer:
        # Update the referer if it has changed so that check_complete_by_referer can work
        logger.info(f"Updating referer of {url} from {current_referer} to {referer}")
        sql, params = query.update(db.tables.history, referer=referer, domain=domain, url_path=db_path)
        await db.commit(sql, params)

    return completed


async def check_album(domain: str, album_id: str) -> dict[str, bool]:
    """Checks whether an album has completed given its domain and album id."""
    db = _current_db.get()
    if db.ignore_history:
        return {}

    sql, params = query.select(db.tables.history, "url_path", "completed", domain=domain, album_id=album_id)
    rows = await db.conn.execute_fetchall(sql, params)
    return {row["url_path"]: bool(row["completed"]) for row in rows}


async def set_album_id(domain: str, media_item: MediaItem) -> None:
    """Sets an album_id in the database."""
    db = _current_db.get()
    sql, params = query.update(
        db.tables.history,
        album_id=media_item.album_id,
        domain=domain,
        url_path=media_item.db_path,
    )
    await db.commit(sql, params)


async def check_complete_by_referer(domain: str | None, referer: AbsoluteHttpURL) -> bool:
    """Checks whether an individual file has completed given its domain and url path."""
    db = _current_db.get()
    if db.ignore_history:
        return False

    if domain is None:
        sql, params = query.exists(db.tables.history, completed=1, referer=referer)

    else:
        sql, params = query.exists(db.tables.history, completed=1, referer=referer, domain=domain)

    return bool(await db.fetchone(sql, params))


async def insert_incompleted(domain: str, media_item: MediaItem) -> None:
    """Inserts an uncompleted file into the database."""

    db = _current_db.get()
    download_filename = media_item.download_filename or ""
    sql, params = query.insert_or_ignore(
        db.tables.history,
        domain=domain,
        url_path=media_item.db_path,
        referer=media_item.referer,
        album_id=media_item.album_id,
        download_path=media_item.download_folder,
        download_filename=download_filename,
        original_filename=media_item.original_filename,
    )

    await db.commit(sql, params)


async def mark_complete(domain: str, media_item: MediaItem) -> None:
    """Mark a download as completed in the database."""
    db = _current_db.get()
    sql, params = query.update(
        db.tables.history,
        completed=1,
        completed_at="CURRENT_TIMESTAMP",
        domain=domain,
        url_path=media_item.db_path,
    )
    await db.commit(sql, params)


async def add_filesize(domain: str, media_item: MediaItem) -> None:
    """Adds the file size to the db."""
    db = _current_db.get()

    sql, params = query.update(
        db.tables.history,
        file_size=await asyncio.to_thread(lambda *_: media_item.complete_file.stat().st_size),
        domain=domain,
        url_path=media_item.db_path,
    )
    await db.commit(sql, params)


async def add_duration(domain: str, media_item: MediaItem) -> None:
    """Adds the duration to the db."""
    db = _current_db.get()
    sql, params = query.update(
        db.tables.history,
        duration=media_item.duration,
        domain=domain,
        url_path=media_item.db_path,
    )
    await db.commit(sql, params)


async def get_duration(domain: str, media_item: MediaItem) -> float | None:
    """Returns the duration from the database."""
    if media_item.is_segment:
        return

    db = _current_db.get()
    sql, params = query.select(
        db.tables.history,
        "duration",
        domain=domain,
        url_path=media_item.db_path,
        limit=1,
    )
    if row := await db.fetchone(sql, params):
        return row["duration"]


async def add_download_filename(domain: str, media_item: MediaItem) -> None:
    """Add the download_filename to the db."""
    db = _current_db.get()
    url_path = media_item.db_path
    query = "UPDATE media SET download_filename=? WHERE domain = ? and url_path = ? and download_filename = ''"
    await db.conn.execute(query, (media_item.download_filename, domain, url_path))
    await db.conn.commit()


async def check_filename_exists(filename: str) -> bool:
    """Checks whether a downloaded filename exists in the database."""
    db = _current_db.get()
    sql, params = query.exists(db.tables.history, download_filename=filename)
    return bool(await db.fetchone(sql, params))


async def get_downloaded_filename(domain: str, media_item: MediaItem) -> str | None:
    """Returns the downloaded filename from the database."""

    if media_item.is_segment:
        return media_item.filename

    db = _current_db.get()
    sql, params = query.select(
        db.tables.history,
        "download_filename",
        domain=domain,
        url_path=media_item.db_path,
        limit=1,
    )
    if row := await db.fetchone(sql, params):
        return row["download_filename"]


async def get_failed_items() -> AsyncGenerator[list[Row]]:
    """Returns a list of failed items."""
    db = _current_db.get()
    sql, params = query.select(db.tables.history, "referer", "download_path", "completed_at", "created_at", completed=0)
    cursor = await db.conn.execute(sql, params)
    while rows := await cursor.fetchmany(_FETCH_MANY_SIZE):
        yield cast("list[Row]", rows)


async def get_all_items(after: datetime.date, before: datetime.date) -> AsyncGenerator[list[Row]]:
    """Returns a list of all items."""
    query_ = """
    SELECT referer,download_path,completed_at,created_at
    FROM media WHERE COALESCE(completed_at, '1970-01-01') BETWEEN ? AND ?
    ORDER BY completed_at DESC;
    """
    db = _current_db.get()
    cursor = await db.conn.execute(query_, (after.isoformat(), before.isoformat()))
    while rows := await cursor.fetchmany(_FETCH_MANY_SIZE):
        yield cast("list[Row]", rows)


async def get_all_bunkr_failed() -> AsyncGenerator[list[Row]]:
    async for rows in get_all_bunkr_failed_via_hash():
        yield rows
    async for rows in get_all_bunkr_failed_via_size():
        yield rows


async def get_all_bunkr_failed_via_size() -> AsyncGenerator[list[Row]]:
    db = _current_db.get()
    sql, params = query.select(
        db.tables.history,
        "referer",
        "download_path",
        "completed_at",
        "created_at",
        file_size=322_509,
        domain="bunkr",
    )

    cursor = await db.conn.execute(sql, params)
    while rows := await cursor.fetchmany(_FETCH_MANY_SIZE):
        yield cast("list[Row]", rows)


async def get_all_bunkr_failed_via_hash() -> AsyncGenerator[list[Row]]:
    query = """
    SELECT m.referer,download_path,completed_at,created_at
    FROM hash h INNER JOIN media m ON h.download_filename= m.download_filename
    WHERE h.hash = 'eb669b6362e031fa2b0f1215480c4e30';
    """

    db = _current_db.get()
    cursor = await db.conn.execute(query)
    while rows := await cursor.fetchmany(_FETCH_MANY_SIZE):
        yield cast("list[Row]", rows)


async def get_file_hash_exists(path: Path | str, hash_type: str) -> str | None:
    query = "SELECT hash FROM hash WHERE folder=? AND download_filename=? AND hash_type=? AND hash IS NOT NULL"
    db = _current_db.get()

    path = Path(path)
    if not path.is_absolute():
        path = path.absolute()
    folder = str(path.parent)
    filename = path.name

    # Check if the file exists with matching folder, filename, and size
    if row := await db.fetchone(query, (folder, filename, hash_type)):
        return row[0]


async def get_files_with_hash_matches(hash_value: str, size: int, hash_type: str | None = None) -> list[aiosqlite.Row]:
    """Retrieves a list of (folder, filename) tuples based on a given hash.

    Args:
        hash_value: The hash value to search for.
        size: file size

    Returns:
        A list of (folder, filename) tuples, or an empty list if no matches found.
    """
    db = _current_db.get()
    if hash_type:
        query = """
        SELECT files.folder, files.download_filename,files.date
        FROM hash JOIN files ON hash.folder = files.folder AND hash.download_filename = files.download_filename
        WHERE hash.hash = ? AND files.file_size = ? AND hash.hash_type = ?;
        """

    else:
        query = """
        SELECT files.folder, files.download_filename FROM hash JOIN files
        ON hash.folder = files.folder AND hash.download_filename = files.download_filename
        WHERE hash.hash = ? AND files.file_size = ? AND hash.hash_type = ?;
        """

    return await db.fetchall(query, (hash_value, size, hash_type))


async def check_hash_exists(hash_type: str, hash_value: str) -> bool:
    db = _current_db.get()
    if db.ignore_history:
        return False

    query = "SELECT 1 FROM hash WHERE hash.hash_type = ? AND hash.hash = ? LIMIT 1"
    return bool(await db.fetchone(query, (hash_type, hash_value)))


async def insert_or_update_hash_db(
    hash_value: str,
    hash_type: Literal["md5", "sha256"],
    file: Path | str,
    original_filename: str | None,
    referer: AbsoluteHttpURL | None,
) -> bool:
    """Inserts or updates a record in the specified SQLite database.

    Args:
        hash_value: The calculated hash of the file.
        file: The file path
        original_filename: The name original name of the file.
        referer: referer URL
        hash_type: The hash type (e.g., md5, sha256)

    Returns:
        True if all the record was inserted or updated successfully, False otherwise.
    """

    hash = await insert_or_update_hashes(hash_value, hash_type, file)
    file_ = await insert_or_update_file(original_filename, referer, file)
    return file_ and hash


async def insert_or_update_hashes(hash_value: str, hash_type: str, file: Path | str) -> bool:
    query = """
    INSERT INTO hash (hash, hash_type, folder, download_filename)
    VALUES (?, ?, ?, ?) ON CONFLICT(download_filename, folder, hash_type) DO UPDATE SET hash = ?;
    """
    db = _current_db.get()
    try:
        full_path = Path(file)
        if not full_path.is_absolute():
            full_path = full_path.absolute()
        download_filename = full_path.name
        folder = str(full_path.parent)
        await db.commit(query, (hash_value, hash_type, folder, download_filename, hash_value))

    except Exception as e:
        logger.exception(f"Error inserting/updating record: {e}")
        return False
    else:
        return True


async def insert_or_update_file(
    original_filename: str | None, referer: AbsoluteHttpURL | str | None, file: Path | str
) -> bool:
    query = """
    INSERT INTO files (folder, original_filename, download_filename, file_size, referer, date)
    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(download_filename, folder)
    DO UPDATE SET original_filename = ?, file_size = ?, referer = ?, date = ?;
    """
    referer_ = str(referer) if referer else None
    db = _current_db.get()
    try:
        full_path = Path(file)
        if not full_path.is_absolute():
            full_path = full_path.absolute()
        download_filename = full_path.name
        folder = str(full_path.parent)
        stat = full_path.stat()
        file_size = stat.st_size
        file_date = int(stat.st_mtime)
        await db.commit(
            query,
            (
                folder,
                original_filename,
                download_filename,
                file_size,
                referer_,
                file_date,
                original_filename,
                file_size,
                referer_,
                file_date,
            ),
        )
    except Exception as e:
        logger.exception(f"Error inserting/updating record: {e}", 40, exc_info=e)
        return False
    return True


async def get_all_unique_hashes(hash_type: str | None = None) -> list[str]:
    """Retrieves a list of hashes

    Args:
        hash_value: The hash value to search for.
        hash_type: The type of hash[optional]

    Returns:
        A list of (folder, filename) tuples, or an empty list if no matches found.
    """
    db = _current_db.get()
    if hash_type:
        query, params = "SELECT DISTINCT hash FROM hash WHERE hash_type =?", (hash_type,)

    else:
        query, params = "SELECT DISTINCT hash FROM hash", ()
    try:
        rows = await db.fetchall(query, params)
        return [row[0] for row in rows]
    except Exception as e:
        logger.exception(f"Error retrieving folder and filename: {e}")
        return []
