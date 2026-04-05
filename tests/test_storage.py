from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from unittest import mock

import pytest

from cyberdrop_dl import aio
from cyberdrop_dl.storage import _psutil as storage


def create_partition(path: str):
    return storage.DiskPartition(Path(path), Path(path), "", "")


def find_partition(path: str):
    return storage._find_partition(Path(path))


async def test_unsupported_fs_should_not_return_zero() -> None:
    cwd = await aio.resolve(Path())
    free_space = await storage._get_free_space(cwd)
    assert free_space > 0
    with mock.patch("psutil.disk_usage", side_effect=OSError(None, "operation not supported")):
        free_space = await storage._get_free_space(cwd)
        assert free_space == -1

    with mock.patch("psutil.disk_usage", side_effect=OSError(None, "another error")):
        with pytest.raises(OSError):
            _ = await storage._get_free_space(cwd)


async def test_fuse_filesystem_should_not_return_zero() -> None:
    cwd = await aio.resolve(Path())
    partition = storage._find_partition(cwd)
    assert partition
    assert not storage._is_fuse_fs(cwd)
    storage._PARTITIONS = [dataclasses.replace(partition, fstype="fuse")]  # pyright: ignore[reportPrivateUsage]
    assert storage._is_fuse_fs(cwd)

    free_space = await storage._get_free_space(cwd)
    assert free_space > 0

    class NullUsage:
        free = 0

    with mock.patch("psutil.disk_usage", return_value=NullUsage()):
        free_space = await storage._get_free_space(cwd)
        assert free_space == -1


def test_storage_only_work_with_abs_paths() -> None:
    cwd = Path()
    with pytest.raises(ValueError):
        _ = storage._find_partition(cwd)

    assert storage._find_partition(cwd.resolve())


@pytest.mark.skipif(os.name == "nt", reason="Test paths are only posix")
async def test_find_partition_finds_the_correct_partition() -> None:

    root, home, usb, external_ssd = partitions = [
        create_partition(path) for path in ("/", "/home", "/mnt/USB", "/home/external_SSD")
    ]

    storage._PARTITIONS = partitions  # pyright: ignore[reportPrivateUsage]

    assert find_partition("/swap_file") is root
    assert find_partition("/home/user/.bash_rc") is home
    assert find_partition("/home/external_SSD/song.mp3") is external_ssd
    assert find_partition("/mnt/USB") is usb
    assert find_partition("/mnt") is root


@pytest.mark.skipif(os.name != "nt", reason="Test paths are only for windows")
async def test_find_partition_finds_the_correct_partition_windows() -> None:
    c_drive, d_drive = partitions = [create_partition(path) for path in ("C:/", "D:/")]

    storage._PARTITIONS = partitions  # pyright: ignore[reportPrivateUsage]

    assert find_partition("C:/pagefile") is c_drive
    assert find_partition("C:/") is c_drive
    assert find_partition("D:/music/song.mp3") is d_drive
    assert find_partition("Z:/") is None
