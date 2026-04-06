from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal

import xxhash
from send2trash import send2trash

from cyberdrop_dl import aio
from cyberdrop_dl.constants import Hashing, TempExt

if TYPE_CHECKING:
    from yarl import URL

    from cyberdrop_dl.config.config_model import DupeCleanup
    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers.manager import Manager


_HASHERS: Final = {
    "md5": hashlib.md5,
    "xxh128": xxhash.xxh128,
    "sha256": hashlib.sha256,
}
_CHUNK_SIZE: Final = 1024 * 1024  # 1MB


logger = logging.getLogger(__name__)


def _compute_hash(file: Path, algorithm: Literal["xxh128", "md5", "sha256"]) -> str:
    with file.open("rb") as fp:
        hash = _HASHERS[algorithm]()
        buffer = bytearray(_CHUNK_SIZE)
        mem_view = memoryview(buffer)
        while size := fp.readinto(buffer):
            hash.update(mem_view[:size])

    return hash.hexdigest()


async def hash_directory_scanner(manager: Manager, path: Path) -> None:
    manager.async_db_hash_startup()
    async with manager.database:
        await manager.hasher.hash_directory(path)
        manager.progress_manager.print_dedupe_stats()
        manager.progress_manager.hash_progress.reset()


@dataclasses.dataclass(slots=True)
class Hasher:
    manager: Manager
    hashed_media_items: list[MediaItem] = dataclasses.field(init=False, default_factory=list)
    hashes_dict: dict[str, dict[int, set[Path]]] = dataclasses.field(
        init=False, default_factory=lambda: defaultdict(lambda: defaultdict(set))
    )
    _sem: asyncio.BoundedSemaphore = dataclasses.field(init=False, default_factory=lambda: asyncio.BoundedSemaphore(20))
    _cwd: Path = dataclasses.field(init=False, default_factory=Path.cwd)

    @property
    def config(self) -> DupeCleanup:
        return self.manager.config.dupe_cleanup_options

    async def hash_file(self, filename: Path | str, hash_type: Literal["xxh128", "md5", "sha256"]) -> str:
        file_path = self._cwd / filename
        return await asyncio.to_thread(_compute_hash, file_path, hash_type)

    @property
    def _to_trash(self) -> bool:
        return self.config.send_deleted_to_trash

    @property
    def _deleted_file_suffix(self) -> Literal["Sent to trash", "Permanently deleted"]:
        return "Sent to trash" if self._to_trash else "Permanently deleted"

    async def hash_directory(self, path: Path) -> None:
        path = Path(path)
        with (
            self.manager.live_manager.get_hash_live(stop=True),
            self.manager.progress_manager.hash_progress.currently_hashing_dir(path),
        ):
            if not await aio.is_dir(path):
                raise NotADirectoryError(None, path)

            async for file in aio.rglob(path, "*"):
                _ = await self.update_db_and_retrive_hash(file)

    async def hash_item(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        hash = await self.update_db_and_retrive_hash(media_item.path, media_item.original_filename, media_item.referer)
        await self.save_hash_data(media_item, hash)

    async def hash_item_during_download(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return

        if self.config.hashing != Hashing.IN_PLACE:
            return

        try:
            assert media_item.original_filename
            hash = await self.update_db_and_retrive_hash(
                media_item.path, media_item.original_filename, media_item.referer
            )
            await self.save_hash_data(media_item, hash)
        except Exception as e:
            logger.exception(f"After hash processing failed: '{media_item.path}' with error {e}")

    async def update_db_and_retrive_hash(
        self,
        file: Path | str,
        original_filename: str | None = None,
        referer: URL | None = None,
    ) -> str | None:
        file = Path(file)

        if file.suffix in TempExt:
            return

        if not await aio.get_size(file):
            return

        hash = await self._update_db_and_retrive_hash(file, original_filename, referer, hash_type="xxh128")
        if self.config.add_md5_hash:
            await self._update_db_and_retrive_hash(file, original_filename, referer, hash_type="md5")
        if self.config.add_sha256_hash:
            await self._update_db_and_retrive_hash(file, original_filename, referer, hash_type="sha256")

        return hash

    async def _update_db_and_retrive_hash(
        self,
        file: Path,
        original_filename: str | None,
        referer: URL | None,
        hash_type: Literal["xxh128", "md5", "sha256"],
    ) -> str | None:
        """Generates hash of a file."""
        self.manager.progress_manager.hash_progress.update_currently_hashing(file)
        hash = await self.manager.database.hash.get_file_hash_exists(file, hash_type)
        try:
            if not hash:
                hash = await self.hash_file(file, hash_type)
                await self.manager.database.hash.insert_or_update_hash_db(
                    hash,
                    hash_type,
                    file,
                    original_filename,
                    referer,
                )
                self.manager.progress_manager.hash_progress.add_new_completed_hash(hash_type)
            else:
                self.manager.progress_manager.hash_progress.add_prev_hash()
                await self.manager.database.hash.insert_or_update_hash_db(
                    hash,
                    hash_type,
                    file,
                    original_filename,
                    referer,
                )
        except Exception as e:
            logger.exception(f"Error hashing '{file}' : {e}")
        else:
            return hash

    async def save_hash_data(self, media_item: MediaItem, hash: str | None) -> None:
        if not hash:
            return

        absolute_path = await aio.resolve(media_item.path)
        size = await aio.get_size(media_item.path)
        assert size
        self.hashed_media_items.append(media_item)
        if hash:
            media_item.hash = hash
        self.hashes_dict[hash][size].add(absolute_path)

    async def cleanup_dupes_after_download(self) -> None:
        if self.config.hashing == Hashing.OFF:
            return
        if not self.config.auto_dedupe:
            return
        if self.manager.config.runtime_options.ignore_history:
            return
        with self.manager.live_manager.get_hash_live(stop=True):
            file_hashes_dict = await self.get_file_hashes_dict()
        with self.manager.live_manager.get_remove_file_via_hash_live(stop=True):
            await self.final_dupe_cleanup(file_hashes_dict)

    async def final_dupe_cleanup(self, final_dict: dict[str, dict[int, set[Path]]]) -> None:
        """cleanup files based on dedupe setting"""

        get_matches = self.manager.database.hash.get_files_with_hash_matches
        async with asyncio.TaskGroup() as tg:

            async def delete_dupes(hash_value: str, size: int) -> None:
                db_matches = await get_matches(hash_value, size, "xxh128")
                for row in db_matches[1:]:
                    file = Path(row["folder"], row["download_filename"])
                    await self._sem.acquire()
                    tg.create_task(self._delete_and_log(file, hash_value))

            for hash_value, size_dict in final_dict.items():
                for size in size_dict:
                    tg.create_task(delete_dupes(hash_value, size))

    async def _delete_and_log(self, file: Path, xxh128_value: str) -> None:
        hash_string = f"xxh128:{xxh128_value}"
        try:
            deleted = await _delete_file(file, self._to_trash)
        except OSError as e:
            logger.exception(f"Unable to remove '{file}' ({hash_string}): {e}")

        else:
            if not deleted:
                return

            msg = (
                f"Removed new download '{file}' [{self._deleted_file_suffix}]. "
                f"File hash matches with a previous download ({hash_string})"
            )
            logger.info(msg)
            self.manager.progress_manager.hash_progress.add_removed_file()

        finally:
            self._sem.release()

    async def get_file_hashes_dict(self) -> dict[str, dict[int, set[Path]]]:
        downloads = self.manager.completed_downloads

        async def exists(item: MediaItem) -> MediaItem | None:
            if await aio.is_file(item.path):
                return item

        results = await asyncio.gather(*(exists(item) for item in downloads))
        for media_item in results:
            if media_item is None:
                continue
            try:
                await self.hash_item(media_item)
            except Exception:
                logger.exception(msg=f"Unable to hash '{media_item.path}'")
        return self.hashes_dict


async def _delete_file(path: Path, to_trash: bool = True) -> bool:
    """Deletes a file and return `True` on success, `False` is the file was not found.

    Any other exception is propagated"""

    if to_trash:
        coro = asyncio.to_thread(send2trash, path)
    else:
        coro = aio.unlink(path)

    try:
        await coro
        return True
    except FileNotFoundError:
        pass
    except OSError as e:
        # send2trash raises everything as a bare OSError. We should only ignore FileNotFound and raise everything else
        msg = str(e)
        if "File not found" not in msg:
            raise

    return False
