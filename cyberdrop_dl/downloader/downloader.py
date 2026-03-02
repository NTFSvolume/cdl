from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from aiohttp import ClientConnectorError, ClientError, ClientResponseError

from cyberdrop_dl import aio
from cyberdrop_dl.exceptions import (
    DownloadError,
    DurationError,
    InvalidContentTypeError,
    RestrictedDateRangeError,
    RestrictedFiletypeError,
    SkipDownloadError,
)
from cyberdrop_dl.utils import error_handling_wrapper

_VIDEO_HLS_BATCH_SIZE = 10
_AUDIO_HLS_BATCH_SIZE = 50


logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from cyberdrop_dl import config
    from cyberdrop_dl.clients.download_client import StreamDownloader
    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.manager import Manager

    P = ParamSpec("P")
    R = TypeVar("R")


logger = logging.getLogger(__name__)


def retry(
    func: Callable[[Downloader, MediaItem], Coroutine[None, None, R]],
) -> Callable[[Downloader, MediaItem], Coroutine[None, None, R]]:
    @functools.wraps(func)
    async def wrapper(self: Downloader, media_item: MediaItem) -> R:
        while True:
            try:
                return await func(self, media_item)
            except DownloadError as e:
                if not e.retry:
                    raise

                media_item.attempts += 1
                logger.error(f"Download failed: {media_item.url} with error: {e!s}")
                if media_item.attempts >= self.config.rate_limits.download_attempts:
                    raise

                retry_msg = f"Retrying download: {media_item.url}, attempt: {media_item.attempts + 1}"
                logger.info(retry_msg)

    return wrapper


_file_lock_vault: aio.WeakAsyncLocks[str] = aio.WeakAsyncLocks()
_NULL_CONTEXT: contextlib.nullcontext[None] = contextlib.nullcontext()


class Downloader:
    """High level class to handle download retries, limiters and post-download chores"""

    def __init__(
        self,
        config: config.Config,
        manager: Manager,
        client: StreamDownloader,
        slots: int,
    ) -> None:
        self.manager: Manager = manager
        self.config: Config = config
        self.client: StreamDownloader = client
        self.processed_items: set[str] = set()
        self.waiting_items: int = 0
        self._current_attempt_filesize: dict[str, int] = {}
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(slots)
        self._server_locks: aio.WeakAsyncLocks[str] = aio.WeakAsyncLocks[str]()
        self._server_locked_domains: set[str] = set()
        self._hardcoded_limits: dict[str, int] = {}
        self._site_sems: dict[str, asyncio.Semaphore] = {}
        self._global_limiter: asyncio.Semaphore = asyncio.Semaphore(self.config.rate_limits.max_simultaneous_downloads)

    def _domain_limiter(self, domain: str) -> asyncio.Semaphore:
        if sem := self._site_sems.get(domain):
            return sem

        limit = self.config.rate_limits.max_simultaneous_downloads_per_domain
        if hardcoded_limit := self._hardcoded_limits.get(domain):
            limit = min(limit, hardcoded_limit)

        self._site_sems[domain] = sem = asyncio.Semaphore(limit)
        return sem

    def _server_lock(self, domain: str, server: str) -> asyncio.Lock | contextlib.nullcontext[None]:
        if domain not in self._server_locked_domains:
            return _NULL_CONTEXT

        return self._server_locks[server]

    async def mark_incomplete(self, media_item: MediaItem) -> None:
        """Marks the media item as incomplete in the database."""
        if media_item.is_segment:
            return
        await self.manager.database.history_table.insert_incompleted(media_item.domain, media_item)

    @contextlib.asynccontextmanager
    async def _limiter(self, media_item: MediaItem):
        if media_item.is_segment:
            yield
            return

        self.waiting_items += 1
        await self.mark_incomplete(media_item)

        server = media_item.real_url.host

        async with (
            self._server_lock(media_item.domain, server),
            self._domain_limiter(media_item.domain),
            self._global_limiter,
            _file_lock_vault[media_item.filename],
        ):
            logger.debug(f"Lock for {media_item.filename!r} acquired")
            self.processed_items.add(media_item.db_path)
            self.waiting_items -= 1
            try:
                yield
            finally:
                logger.debug(f"Lock for {media_item.filename!r} released")

    async def run(self, media_item: MediaItem) -> bool:
        if media_item.url.path in self.processed_items and not self.config.runtime.ignore_history:
            return False

        async with self._limiter(media_item):
            if not media_item.is_segment:
                logger.info(f"Download starting: {media_item.url}")

            return bool(await self._download(media_item))

    async def _check_file_can_download(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return

        if not self.manager.client.check_allowed_filetype(media_item):
            raise RestrictedFiletypeError(origin=media_item)
        if not await self.manager.client.check_file_duration(media_item):
            raise DurationError(origin=media_item)
        if not self.manager.client.check_allowed_date_range(media_item):
            raise RestrictedDateRangeError(origin=media_item)

    @error_handling_wrapper
    @retry
    async def _download(self, media_item: MediaItem) -> bool | None:
        try:
            if not media_item.is_segment:
                media_item.duration = await self.manager.database.history_table.get_duration(
                    media_item.domain, media_item
                )
                await self._check_file_can_download(media_item)

            return await self.client.download(media_item)

        except SkipDownloadError as e:
            if not media_item.is_segment:
                logger.info(f"Download skipped {media_item.url}: {e}")
                self.manager.tui.files.add_skipped()

        except (DownloadError, ClientResponseError, InvalidContentTypeError):
            raise

        except (
            ConnectionResetError,
            FileNotFoundError,
            PermissionError,
            TimeoutError,
            ClientError,
            ClientConnectorError,
        ) as e:
            ui_message = getattr(e, "status", type(e).__name__)
            message = str(e)
            raise DownloadError(ui_message, message, retry=True) from e
