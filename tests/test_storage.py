from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from cyberdrop_dl import storage
from cyberdrop_dl.storage import StorageChecker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def storag() -> AsyncGenerator[StorageChecker]:
    async with StorageChecker(required_free_space=512_000_000) as m:
        yield m


async def test_unsupported_fs_should_not_return_zero() -> None:
    cwd = await asyncio.to_thread(Path().resolve)
    free_space = await storage.get_free_space(cwd)
    assert free_space > 0
    with mock.patch("psutil.disk_usage", side_effect=OSError(None, "operation not supported")):
        free_space = await storage.get_free_space(cwd)
        assert free_space == -1

    with mock.patch("psutil.disk_usage", side_effect=OSError(None, "another error")):
        with pytest.raises(OSError):
            _ = await storage.get_free_space(cwd)


async def test_fuse_filesystem_should_not_return_zero() -> None:
    cwd = await asyncio.to_thread(Path().resolve)
    partition = storage.find_partition(cwd)
    assert partition
    assert not storage.is_fuse_fs(cwd)
    storage._PARTITIONS = [dataclasses.replace(partition, fstype="fuse")]  # pyright: ignore[reportPrivateUsage]
    assert storage.is_fuse_fs(cwd)

    free_space = await storage.get_free_space(cwd)
    assert free_space > 0

    class NullUsage:
        free = 0

    with mock.patch("psutil.disk_usage", return_value=NullUsage()):
        free_space = await storage.get_free_space(cwd)
        assert free_space == -1


def test_storage_only_work_with_abs_paths() -> None:
    cwd = Path()
    with pytest.raises(ValueError):
        _ = storage.find_partition(cwd)

    assert storage.find_partition(cwd.resolve())


async def test_find_partition_finds_the_correct_partition() -> None:
    def part(path: str) -> storage.DiskPartition:
        return storage.DiskPartition(Path(path), Path(path), "", "")

    root, home, usb, external_ssd = partitions = [
        part("/"),
        part("/home"),
        part("/mnt/USB"),
        part("/home/external_SSD"),
    ]

    storage._PARTITIONS = partitions  # pyright: ignore[reportPrivateUsage]

    assert storage.find_partition(Path("/swap_file")) is root
    assert storage.find_partition(Path("/home/user/.bash_rc")) is home
    assert storage.find_partition(Path("/home/external_SSD/song.mp3")) is external_ssd
    assert storage.find_partition(Path("mnt/USB")) is None
    assert storage.find_partition(Path("/mnt/USB")) is usb
    assert storage.find_partition(Path("/mnt")) is root
