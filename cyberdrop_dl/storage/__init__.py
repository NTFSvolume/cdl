from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
from contextvars import ContextVar
from typing import TYPE_CHECKING

from cyberdrop_dl.exceptions import InsufficientFreeSpaceError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from pathlib import Path

    from cyberdrop_dl.data_structures import MediaItem

_required_free_space: ContextVar[int] = ContextVar("_required_free_space")

try:
    from ._psutil import has_sufficient_space
    from ._psutil import start_loop as _psutil_loop
except ImportError:
    _psutil_loop = None

    async def has_sufficient_space(folder: Path, /, required_free_space: int) -> bool:
        usage = await asyncio.to_thread(shutil.disk_usage, folder)
        return usage.free > required_free_space


def create_free_space_checker(media_item: MediaItem, *, frecuency: int = 5) -> Callable[[], Awaitable[None]]:
    current_chunk = 0

    async def checker() -> None:
        nonlocal current_chunk
        if current_chunk % frecuency == 0:
            await check(media_item)
        current_chunk += 1

    return checker


async def check(media_item: MediaItem) -> None:
    """Checks if there is enough free space to download this item."""

    if not await has_sufficient_space(media_item.download_folder, _required_free_space.get()):
        raise InsufficientFreeSpaceError(media_item)


@contextlib.asynccontextmanager
async def monitor(required_free_space: int) -> AsyncGenerator[None]:
    if _psutil_loop is None:
        logger.warning("psutil is not available on this system. Falling back to eager checks for free space")
        yield
        return

    loop = asyncio.create_task(_psutil_loop(), name="storage monitor")
    token = _required_free_space.set(required_free_space)
    await asyncio.sleep(0)
    try:
        yield
    finally:
        _required_free_space.reset(token)
        try:
            _ = loop.cancel("On monitor exit")
            await loop
        except asyncio.CancelledError:
            pass
