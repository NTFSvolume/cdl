from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, ParamSpec, TypeVar

from aiohttp import ClientConnectorError, ClientError, ClientResponseError

from cyberdrop_dl import config
from cyberdrop_dl.exceptions import (
    DownloadError,
    DurationError,
    ErrorLogMessage,
    InvalidContentTypeError,
    RestrictedDateRangeError,
    RestrictedFiletypeError,
    SkipDownloadError,
)
from cyberdrop_dl.utils import aio
from cyberdrop_dl.utils.dates import set_creation_time
from cyberdrop_dl.utils.logger import log, log_debug
from cyberdrop_dl.utils.utilities import error_handling_wrapper

_VIDEO_HLS_BATCH_SIZE = 10
_AUDIO_HLS_BATCH_SIZE = 50


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from cyberdrop_dl.clients.download_client import DownloadClient
    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers import Manager


P = ParamSpec("P")
R = TypeVar("R")


class SegmentDownloadResult(NamedTuple):
    item: MediaItem
    downloaded: bool


KNOWN_BAD_URLS = {
    "https://i.imgur.com/removed.png": 404,
    "https://saint2.su/assets/notfound.gif": 404,
    "https://bnkr.b-cdn.net/maintenance-vid.mp4": 503,
    "https://bnkr.b-cdn.net/maintenance.mp4": 503,
    "https://c.bunkr-cache.se/maintenance-vid.mp4": 503,
    "https://c.bunkr-cache.se/maintenance.jpg": 503,
}


def retry(func: Callable[P, Coroutine[None, None, R]]) -> Callable[P, Coroutine[None, None, R]]:
    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        self: Downloader = args[0]
        media_item: MediaItem = args[1]
        while True:
            try:
                return await func(*args, **kwargs)
            except DownloadError as e:
                if not e.retry:
                    raise

                media_item.current_attempt += 1

                log(f"{self.log_prefix} failed: {media_item.url} with error: {e!s}", 40)
                if media_item.current_attempt >= self.max_attempts:
                    raise

                retry_msg = f"Retrying {self.log_prefix.lower()}: {media_item.url} , retry attempt: {media_item.current_attempt + 1}"
                log(retry_msg, 20)

    return wrapper


GENERIC_CRAWLERS = ".", "no_crawler"


class Downloader:
    def __init__(
        self,
        config: config.Config,
        manager: Manager,
        client: DownloadClient,
        slots: int,
    ) -> None:
        self.manager: Manager = manager

        self.config = config
        self.client: DownloadClient = client

        self.log_prefix = "Download"
        self.processed_items: set[str] = set()
        self.waiting_items = 0
        self._current_attempt_filesize: dict[str, int] = {}
        self._file_lock_vault: aio.WeakAsyncLocks[str] = aio.WeakAsyncLocks()
        self._ignore_history: bool = self.config.runtime_options.ignore_history
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(slots)

    @property
    def max_attempts(self):
        if self.config.download_options.disable_download_attempt_limit:
            return 1
        return self.config.rate_limiting_options.download_attempts

    @contextlib.asynccontextmanager
    async def _limiter(self, media_item: MediaItem):
        media_item.current_attempt = 0
        if media_item.is_segment:
            yield
            return

        self.waiting_items += 1
        await self.client.mark_incomplete(media_item)

        server = (media_item.debrid_link or media_item.url).host
        server_limit, domain_limit, global_limit = (
            self.client.server_limiter(media_item.domain, server),
            self._semaphore,
            self.manager.client_manager.global_download_slots,
        )

        async with server_limit, domain_limit, global_limit:
            self.processed_items.add(media_item.db_path)
            self.waiting_items -= 1
            yield

    async def run(self, media_item: MediaItem) -> bool:
        if media_item.url.path in self.processed_items and not self._ignore_history:
            return False

        async with self._limiter(media_item):
            if not media_item.is_segment:
                log(f"{self.log_prefix} starting: {media_item.url}", 20)

            async with self._file_lock_vault[media_item.filename]:
                log_debug(f"Lock for {media_item.filename!r} acquired", 20)
                try:
                    return bool(await self.download(media_item))
                finally:
                    log_debug(f"Lock for {media_item.filename!r} released", 20)

    async def finalize_download(self, media_item: MediaItem, downloaded: bool) -> None:
        if downloaded:
            await asyncio.to_thread(Path.chmod, media_item.complete_file, 0o666)
            await _set_file_datetime(media_item, media_item.complete_file)

        self.manager.progress_manager.files.add_completed()
        log(f"Download finished: {media_item.url}", 20)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def check_file_can_download(self, media_item: MediaItem) -> None:
        await self.manager.storage_manager.check_free_space(media_item)
        if not self.manager.client_manager.check_allowed_filetype(media_item):
            raise RestrictedFiletypeError(origin=media_item)
        if not await self.manager.client_manager.check_file_duration(media_item):
            raise DurationError(origin=media_item)
        if not self.manager.client_manager.check_allowed_date_range(media_item):
            raise RestrictedDateRangeError(origin=media_item)

    @error_handling_wrapper
    @retry
    async def download(self, media_item: MediaItem) -> bool | None:
        try:
            if not media_item.is_segment:
                media_item.duration = await self.manager.db_manager.history_table.get_duration(
                    media_item.domain, media_item
                )
                await self.check_file_can_download(media_item)

            downloaded = await self.client.download_file(media_item.domain, media_item)
            if downloaded:
                await asyncio.to_thread(Path.chmod, media_item.complete_file, 0o666)
                if not media_item.is_segment:
                    await _set_file_datetime(media_item, media_item.complete_file)
                    self.manager.progress_manager.files.add_completed()
                    log(f"Download finished: {media_item.url}", 20)

            return downloaded

        except SkipDownloadError as e:
            if not media_item.is_segment:
                log(f"Download skip {media_item.url}: {e}", 10)
                self.manager.progress_manager.files.add_skipped()

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

    def write_download_error(
        self,
        media_item: MediaItem,
        error_log_msg: ErrorLogMessage,
        exc_info: Exception | None = None,
    ) -> None:
        full_message = f"{self.log_prefix} Failed: {media_item.url} ({error_log_msg.main_log_msg}) \n -> Referer: {media_item.referer}"
        log(full_message, 40, exc_info=exc_info)
        self.manager.logs.write_download_error_log(media_item, error_log_msg.csv_log_msg)
        self.manager.progress_manager.download_errors.add_failure(error_log_msg.ui_failure)
        self.manager.progress_manager.files.add_failed()


async def _set_file_datetime(media_item: MediaItem, complete_file: Path) -> None:
    if media_item.is_segment:
        return

    if config.get().download_options.disable_file_timestamps:
        return

    if not media_item.timestamp:
        log(f"Unable to parse upload date for {media_item.url}, using current datetime as file datetime", 30)
        return

    # 1. try setting creation date
    try:
        await set_creation_time(media_item.complete_file, media_item.timestamp)

    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, ValueError):
        pass

    # 2. try setting modification and access date
    try:
        await asyncio.to_thread(os.utime, complete_file, (media_item.timestamp, media_item.timestamp))
    except OSError:
        pass
