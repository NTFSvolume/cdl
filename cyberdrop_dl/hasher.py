from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, NamedTuple, NewType

import xxhash
from send2trash import send2trash

from cyberdrop_dl import aio, constants
from cyberdrop_dl.constants import HashAlgorithm, Hashing

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from pathlib import Path

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.database import Database
    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.tui import TUI

_HASHERS: Final = {
    HashAlgorithm.md5: hashlib.md5,
    HashAlgorithm.xxh128: xxhash.xxh128,
    HashAlgorithm.sha256: hashlib.sha256,
}
_1MB: Final = 1024 * 1024
_10MB: Final = _1MB * 10
HashValue = NewType("HashValue", str)


class HashResult(NamedTuple):
    hash: HashValue
    file_size: int
    mtime: int


XXH128Result = NewType("XXH128Result", HashResult)

HashResults = dict[HashAlgorithm, HashResult]


logger = logging.getLogger(__name__)


async def hash_directory(manager: Manager, path: Path) -> None:
    # TODO: make db a context manager
    await manager.async_db_hash_startup()
    await Hasher(manager.tui, manager.config, manager.db_manager).hash_folder(path)
    manager.tui.print_dedupe_stats()
    await manager.async_db_close()


def compute_hash(file: Path, algorithm: HashAlgorithm) -> HashValue:
    assert file.is_absolute()
    chunk_size = _10MB if file.suffix.lower() in constants.FileExt.VIDEO else _1MB
    with file.open("rb") as fp:
        hash = _HASHERS[algorithm]()
        buffer = bytearray(chunk_size)  # Reusable buffer to reduce allocations
        mem_view = memoryview(buffer)
        while size := fp.readinto(buffer):
            hash.update(mem_view[:size])

    return HashValue(hash.hexdigest())


@dataclasses.dataclass(slots=True)
class Hasher:
    """Manage hashes and db insertion.

    The hasher will have a peak RAM consumption of (concurrency * 10MB) while hashing
    and use a max of (concurrency) number of threads (if available)"""

    tui: TUI
    config: Config
    database: Database
    concurrency: int = 20

    _xxh128_hashes: dict[Path, XXH128Result] = dataclasses.field(init=False, default_factory=dict)
    _sem: asyncio.BoundedSemaphore = dataclasses.field(init=False)
    _hashes: tuple[HashAlgorithm, ...] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._sem = asyncio.BoundedSemaphore(self.concurrency)
        self._hashes = HashAlgorithm.xxh128, *self.config.dedupe.additional_hashes

    @property
    def results(self) -> MappingProxyType[Path, XXH128Result]:
        return MappingProxyType(self._xxh128_hashes)

    async def hash_folder(self, path: Path) -> None:
        if not await aio.is_dir(path):
            raise NotADirectoryError(path)

        with self.tui(screen="hashing"), self.tui.hashing(path):
            async with asyncio.TaskGroup() as tg:
                async for file in aio.rglob(path, "*"):
                    await self._sem.acquire()
                    tg.create_task(self._hash_file(file))

    async def _hash_file(self, file: Path) -> HashResults | None:
        try:
            with self.tui.hashing.new_hook(file):
                results = await asyncio.gather(*(self._get_hash_or_compute(file, algo) for algo in self._hashes))

        except Exception as e:
            # Files may have been deleted/moved after we downloaded them
            logger.exception(f"Unable to hash file = '{file}'({e})")
        else:
            return dict(zip(self._hashes, results, strict=True))
        finally:
            self._sem.release()

    async def _hash_item(self, media_item: MediaItem) -> None:
        if media_item.is_segment or media_item.complete_file.suffix in constants.TempExt:
            return

        results = await self._hash_file(media_item.complete_file)
        if not results:
            return

        xxh128_result = XXH128Result(results[HashAlgorithm.xxh128])
        media_item.hash = xxh128_result.hash
        self._xxh128_hashes[media_item.complete_file] = xxh128_result
        # TODO: save results to the database

    async def _get_hash_or_compute(self, file: Path, hash_algo: HashAlgorithm) -> HashResult:
        """Generates hash of a file."""
        stat = await aio.stat(file)
        f_size = stat.st_size
        f_mtime = int(stat.st_mtime)
        db_lookup = await self.database.hash_table.get_file_hash_exists(file, hash_algo)

        match db_lookup:
            case [hash, db_size, db_mtime] if db_size == f_size:
                if db_mtime is None:
                    # TODO: pre v9 db row. We need to delete them
                    pass
                else:
                    self.tui.hashing.add_prev_hashed()
                    return HashResult(HashValue(hash), f_size, f_mtime)
            case _:
                pass

        hash = await asyncio.to_thread(compute_hash, file, hash_algo)
        self.tui.hashing.add_hashed(hash_algo)
        return HashResult(HashValue(hash), f_size, f_mtime)

    async def in_place_hash(self, media_item: MediaItem) -> None:
        if self.config.dedupe.hashing is not Hashing.IN_PLACE:
            return
        await self._sem.acquire()
        await self._hash_item(media_item)

    async def post_download_hash(self, downloads: Iterable[MediaItem]) -> None:
        if self.config.dedupe.hashing is not Hashing.POST_DOWNLOAD:
            return

        with self.tui(screen="hashing"):
            async with asyncio.TaskGroup() as tg:
                for item in downloads:
                    await self._sem.acquire()
                    tg.create_task(self._hash_item(item))

    async def dedupe(self) -> None:
        if self.config.runtime.ignore_history or not self.config.dedupe.auto_dedupe:
            return
        with self.tui(screen="hashing"):  # TODO: Add a new screen for "removing_hashing"
            czkawka = Czkawka(
                send_to_trash_bin=self.config.dedupe.send_deleted_to_trash,
                concurrency=self.concurrency,
                on_delete=self.tui.hashing.add_removed,
            )
            await czkawka.run(self.results)


@dataclasses.dataclass(slots=True, kw_only=True)
class Czkawka:
    """Deletes dedupes based on hash results"""

    # Why this name? Czkawka it's a popular dedupe sofware and this class works in a similar way to find matches
    # https://github.com/qarmin/czkawka.

    send_to_trash_bin: bool
    concurrency: int = 20
    on_delete: Callable[[], Any] = lambda: None
    _sem: asyncio.BoundedSemaphore = dataclasses.field(init=False)
    _suffix: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._sem = asyncio.BoundedSemaphore(self.concurrency)
        self._suffix = "Sent to trash" if self.send_to_trash_bin else "Permanently deleted"

    async def run(self, results: Mapping[Path, XXH128Result]) -> None:
        """delete duplicate files"""
        async with asyncio.TaskGroup() as tg:
            for file, result in results.items():
                # TODO: We should query all files with a matching result from the db and only keep the oldest one
                await self._sem.acquire()
                _ = tg.create_task(self._delete_and_log(file, result.hash))

    async def _delete_and_log(self, file: Path, xxh128_value: HashValue) -> None:
        hash_string = f"xxh128:{xxh128_value}"
        try:
            deleted = await _delete_file(file, self.send_to_trash_bin)
        except OSError as e:
            logger.exception(f"Unable to remove '{file}' ({hash_string}): {e}")
        else:
            if not deleted:
                return
            msg = (
                f"Removed new download '{file}' [{self._suffix}]. "
                f"File hash matches with a previous download ({hash_string})"
            )
            logger.info(msg)
            self.on_delete()
        finally:
            self._sem.release()


async def _delete_file(path: Path, to_trash: bool = True) -> bool:
    """Deletes a file and return `True` on success, `False` is the file was not found.

    Any other exception is propagated"""

    if to_trash:
        coro = asyncio.to_thread(send2trash, path)
    else:
        coro = aio.unlink(path)

    try:
        await coro
    except FileNotFoundError:
        return False
    except OSError as e:
        # send2trash raises everything as a bare OSError. We should only ignore FileNotFound and raise everything else
        if "file not found" not in str(e).casefold():
            raise
        return False
    else:
        return True
