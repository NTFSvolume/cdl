from __future__ import annotations

import asyncio
import dataclasses
import functools
import logging
from collections import defaultdict
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self

import psutil
from pydantic import ByteSize

from cyberdrop_dl.exceptions import InsufficientFreeSpaceError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    from psutil._ntuples import sdiskpart

    from cyberdrop_dl.data_structures.url_objects import MediaItem


logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, slots=True, order=True)
class DiskPartition:
    mountpoint: Path
    device: Path = dataclasses.field(compare=False)
    fstype: str = dataclasses.field(compare=False)
    opts: str = dataclasses.field(compare=False)

    @staticmethod
    def from_psutil(diskpart: sdiskpart) -> DiskPartition:
        # Resolve converts any mapped drive to UNC paths (windows)
        return DiskPartition(
            Path(diskpart.mountpoint).resolve(),
            Path(diskpart.device).resolve(),
            diskpart.fstype,
            diskpart.opts,
        )


@dataclasses.dataclass(frozen=True, slots=True, order=True)
class DiskPartitionStats:
    partition: DiskPartition
    free_space: ByteSize

    def __str__(self) -> str:
        free_space = self.free_space.human_readable(decimal=True)
        stats_as_dict = dataclasses.asdict(self.partition) | {"free_space": free_space}
        return ", ".join(f"'{k}': '{v}'" for k, v in stats_as_dict.items())


_storage: ContextVar[StorageChecker] = ContextVar("_storage")
_PARTITIONS: list[DiskPartition] = []
_UNAVAILABLE: set[Path] = set()
_LOCKS: dict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)
_CHECK_PERIOD: Final = 2  # how often the check_free_space_loop will run (in seconds)
_LOG_PERIOD: Final = 10  # log storage details every <x> loops, AKA log every 20 (2x10) seconds,


@dataclasses.dataclass(slots=True)
class StorageChecker:
    """Runs an infinite loop to keep an updated value of the available space on all storage devices."""

    required_free_space: int
    _free_space_map: dict[Path, int] = dataclasses.field(init=False, default_factory=dict)
    _loop: asyncio.Task[None] | None = None

    def __str__(self) -> str:
        info = "\n".join(f"    {stats!s}" for stats in self._partition_stats())
        return f"Storage status:\n {info}"

    def _partition_stats(self) -> Generator[DiskPartitionStats]:
        for partition in partitions():
            free_space = self._free_space_map.get(partition.mountpoint)
            if free_space is not None:
                yield DiskPartitionStats(partition, ByteSize(free_space))

    async def _has_sufficient_space(self, folder: Path) -> bool:
        await _check_nt_network_drive(folder)
        mount = _get_mount_point(folder)
        if not mount:
            return False

        if mount not in self._free_space_map:
            async with _LOCKS[mount]:
                if mount not in self._free_space_map:
                    # Manually query this mount now. Next time it will be part of the loop

                    self._free_space_map[mount] = await get_free_space(mount)
                    logger.info(f"A new mountpoint ('{mount!s}') will be used for '{folder}'")
                    logger.info(self)

                    if self._loop is None:
                        self._loop = asyncio.create_task(self._check_free_space_loop())

        free_space = self._free_space_map[mount]
        return free_space == -1 or free_space > self.required_free_space

    async def _check_free_space_loop(self) -> None:
        """Infinite loop to get free space of all used mounts and update internal dict"""

        last_check = -1
        assert len(self._free_space_map) >= 1
        while True:
            last_check += 1
            mountpoints = sorted(mount for mount, free_space in self._free_space_map.items() if free_space != -1)
            if mountpoints:
                results = await asyncio.gather(*(get_free_space(mount) for mount in mountpoints))
                self._free_space_map.update(zip(mountpoints, results, strict=True))

            if last_check % _LOG_PERIOD == 0:
                logger.debug(self)

            await asyncio.sleep(_CHECK_PERIOD)

    async def check_free_space(self, media_item: MediaItem) -> None:
        """Checks if there is enough free space to download this item."""

        if not await self._has_sufficient_space(media_item.download_folder):
            raise InsufficientFreeSpaceError(origin=media_item)

    async def close(self) -> None:
        self._free_space_map.clear()
        if self._loop is None:
            return
        try:
            _ = self._loop.cancel()
            await self._loop
        except asyncio.CancelledError:
            pass

    async def __aenter__(self) -> Self:
        _ = _storage.set(self)
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


@functools.lru_cache
def _get_mount_point(folder: Path) -> Path | None:
    # Cached for performance.
    # It's not an expensive operation nor IO blocking, but it's very common for multiple files to share the same download folder
    # ex: HLS downloads could have over a thousand segments. All of them will go to the same folder
    if partition := find_partition(folder):
        return partition.mountpoint

    # Mount point for this path does not exists
    # This will only happen on Windows, ex: an USB drive (`D:`) that is not currently available (AKA disconnected)
    # On Unix there's always at least 1 mountpoint, root (`/`)
    msg = f"No available mountpoint found for '{folder}'"
    msg += f"\n -> drive = '{_drive_as_path(folder.drive)}' , last_parent = '{folder.parents[-1]}'"
    logger.error(msg)


def _drive_as_path(drive: str) -> Path:
    is_mapped_drive = ":" in drive and len(drive) == 2
    return Path(f"{drive}/" if is_mapped_drive else drive)


def _get_disk_partitions() -> Generator[DiskPartition]:
    for p in psutil.disk_partitions(all=True):
        try:
            yield DiskPartition.from_psutil(p)
        except OSError as e:
            logger.error(
                f"Unable to get information about {p.mountpoint}. All files with that mountpoint as target will be skipped: {e!r}"
            )


def find_partition(path: Path) -> DiskPartition | None:
    if not path.is_absolute():
        raise ValueError(f"{path!r} is not absolute")

    possible_partitions = (p for p in partitions() if path.is_relative_to(p.mountpoint))

    # Get the closest mountpoint to `folder`
    # mount_a = /home/user/  -> points to an internal SSD
    # mount_b = /home/user/USB -> points to an external USB drive
    # If `folder`` is `/home/user/USB/videos`, the correct mountpoint is mount_b
    if partition := max(possible_partitions, key=lambda p: len(p.mountpoint.parts), default=None):
        return partition


def is_fuse_fs(path: Path) -> bool:
    if partition := find_partition(path):
        return "fuse" in partition.fstype
    return False


async def _check_nt_network_drive(folder: Path) -> None:
    """Checks is the drive of this folder is a Windows network drive (UNC or unknown mapped drive) and exists."""
    # See: https://github.com/jbsparrow/CyberDropDownloader/issues/860
    if not psutil.WINDOWS:
        return

    # We can discard mapped drives because they would have been converted to UNC path at startup
    # calling resolve on a mapped network drive returns its UNC path
    # it would only still be a mapped drive is the network address is not available
    is_mapped_drive = ":" in folder.drive and len(folder.drive) == 2
    is_unc_path = folder.drive.startswith("\\\\")
    if is_mapped_drive or not is_unc_path:
        return

    folder_drive = _drive_as_path(folder.drive)

    if folder_drive in _UNAVAILABLE:
        return

    mounts = mountpoints()
    if folder_drive in mounts:
        return

    async with _LOCKS[folder_drive]:
        if folder_drive in _UNAVAILABLE or folder_drive in mounts:
            return

        logger.debug(f"Checking new possible network_drive: '{folder_drive}' for folder '{folder}'")

        try:
            is_dir = await asyncio.to_thread(folder_drive.is_dir)
        except OSError:
            is_dir = False

        if is_dir:
            _PARTITIONS.append(DiskPartition(folder_drive, folder_drive, "network_drive", ""))
            _get_mount_point.cache_clear()

        else:
            _UNAVAILABLE.add(folder_drive)


async def get_free_space(path: Path) -> int:
    unsupported = None
    free_space = 0

    try:
        result = await asyncio.to_thread(psutil.disk_usage, str(path))
        free_space = result.free
    except OSError as e:
        if "operation not supported" not in str(e).casefold():
            raise

        unsupported = e

    if unsupported or (free_space == 0 and is_fuse_fs(path)):
        logger.error(
            f"Unable to get free space from mount point ('{path!s}')'. Skipping free space check",
            exc_info=unsupported,
        )
        free_space = -1

    return free_space


def create_free_space_checker(media_item: MediaItem, *, frecuency: int = 5) -> Callable[[], Awaitable[None]]:
    current_chunk = 0
    check = _storage.get().check_free_space

    async def checker() -> None:
        nonlocal current_chunk
        if current_chunk % frecuency == 0:
            await check(media_item)
        current_chunk += 1

    return checker


def partitions() -> tuple[DiskPartition, ...]:
    if not _PARTITIONS:
        _PARTITIONS.extend(_get_disk_partitions())
    return tuple(_PARTITIONS)


def mountpoints() -> tuple[Path, ...]:
    return tuple(p.mountpoint for p in partitions())


def clear_cache() -> None:
    _PARTITIONS.clear()
    _UNAVAILABLE.clear()
    _LOCKS.clear()
    _get_mount_point.cache_clear()
