from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiofiles
import aiohttp
from aiolimiter import AsyncLimiter

from cyberdrop_dl import config, constants, storage
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import DownloadError, InvalidContentTypeError, SlowDownloadError
from cyberdrop_dl.utils import aio, dates

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping
    from typing import Any

    import aiohttp

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.managers.http import HttpClient
    from cyberdrop_dl.tui.common import ProgressHook


class DownloadSpeedLimiter(AsyncLimiter):
    __slots__ = ()

    def __init__(self, max_rate: int) -> None:
        super().__init__(max_rate, time_period=1)

    async def acquire(self, amount: float = 1) -> None:
        if self.max_rate <= 0:
            return
        await super().acquire(amount)


_CONTENT_TYPES_OVERRIDES: dict[str, str] = {"text/vnd.trolltech.linguist": "video/MP2T"}
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds
_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}


logger = logging.getLogger(__name__)


class StreamDownloader:
    """Low level class to that performs the actual download + database updates"""

    def __init__(self, manager: Manager, http_client: HttpClient, config: config.Config) -> None:
        self.manager = manager
        self.http_client = http_client
        self.config = config
        self._slow_download_threshold = config.runtime.slow_download_speed
        self._supports_ranges: bool = True
        self.chunk_size: int = 1024 * 1024 * 10  # 10MB
        if config.rate_limits.download_speed_limit:
            self.chunk_size = min(self.chunk_size, config.rate_limits.download_speed_limit)

        self._speed_limiter: DownloadSpeedLimiter = DownloadSpeedLimiter(config.rate_limits.download_speed_limit)

    async def download(self, media_item: MediaItem) -> bool:
        """Starts a file."""
        if self.config.download.skip_download_mark_completed and not media_item.is_segment:
            logger.info(f"Download skipped {media_item.url} due to mark completed option", 10)
            self.manager.progress.files.add_skipped()
            await self.mark_completed(media_item.domain, media_item)
            return False

        downloaded = await self._download(media_item)

        if downloaded:
            _ = await asyncio.to_thread(shutil.move, media_item.partial_file, media_item.complete_file)
            if not media_item.is_segment:
                has_valid_duration = await self.http_client.check_file_duration(media_item)
                await self.manager.db_manager.history_table.add_filesize(media_item.domain, media_item)
                if not has_valid_duration:
                    await asyncio.to_thread(media_item.complete_file.unlink)
                    logger.warning(f"Download deleted {media_item.url} due to runtime restrictions")
                    await self.mark_incomplete(media_item)
                    self.manager.progress.files.add_skipped()
                    return False

            await self._finalize_download(media_item)

        return downloaded

    async def _download(self, media_item: MediaItem) -> bool:
        resume_point = 0
        if self._supports_ranges and (size := await aio.get_size(media_item.partial_file)):
            resume_point = size
            media_item.headers["Range"] = f"bytes={size}-"

        await asyncio.sleep(self.config.rate_limits.total_delay)
        download_url = media_item.debrid_url or media_item.url
        async with self.__request_context(download_url, media_item.domain, media_item.headers) as resp:
            return await self._process_response(media_item, resume_point, resp)

    async def _process_response(
        self,
        media_item: MediaItem,
        resume_point: int,
        resp: aiohttp.ClientResponse | AbstractResponse,
    ) -> bool:
        if resp.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
            await asyncio.to_thread(media_item.partial_file.unlink)

        _ = await self.http_client.check_http_status(resp, download=True)

        if not media_item.is_segment:
            _ = _check_content_type(media_item.ext, resp.headers)

        media_item.filesize = int(resp.headers.get("Content-Length", "0")) or None
        if resp.status != HTTPStatus.PARTIAL_CONTENT:
            await asyncio.to_thread(media_item.partial_file.unlink, missing_ok=True)

        if (
            not media_item.is_segment
            and not media_item.timestamp
            and (last_modified := _get_last_modified(resp.headers))
        ):
            logger.warning(
                f"Unable to parse upload date for {media_item.url}, using `Last-Modified` header as file datetime"
            )
            media_item.timestamp = last_modified

        hook = self.manager.progress.downloads(media_item.filename, media_item.filesize)
        if resume_point:
            hook.advance(resume_point)

        await self._read_stream(media_item, self._get_resp_reader(resp), hook)
        return True

    def _get_resp_reader(
        self, resp: aiohttp.ClientResponse | AbstractResponse
    ) -> AbstractResponse | aiohttp.StreamReader:
        if isinstance(resp, AbstractResponse):
            return resp
        return resp.content

    @contextlib.asynccontextmanager
    async def __request_context(
        self, url: AbsoluteHttpURL, domain: str, headers: dict[str, str]
    ) -> AsyncGenerator[AbstractResponse | aiohttp.ClientResponse]:
        if domain in _USE_IMPERSONATION:
            resp = await self.http_client.curl_session.get(str(url), stream=True, headers=headers)
            try:
                yield AbstractResponse.from_resp(resp)
            finally:
                await resp.aclose()
            return

        async with self.http_client.dl_session.get(url, headers=headers) as resp:
            yield resp

    async def _read_stream(
        self,
        media_item: MediaItem,
        content: aiohttp.StreamReader | AbstractResponse,
        progress_hook: ProgressHook,
    ) -> None:
        """Appends content to a file."""

        check_free_space = storage.create_free_space_checker(media_item)

        await check_free_space()
        await self._pre_download_check(media_item)

        empty = True
        with progress_hook:
            check_speed = self._create_speed_checker(progress_hook)

            async with aiofiles.open(media_item.partial_file, mode="ab") as f:
                async for chunk in content.iter_chunked(self.chunk_size):
                    await check_free_space()
                    n_bytes = len(chunk)
                    await self._speed_limiter.acquire(n_bytes)
                    _ = await f.write(chunk)
                    if empty:
                        empty = not bool(n_bytes)
                    progress_hook.advance(n_bytes)
                    check_speed()

        if empty:
            await aio.unlink(media_item.partial_file, missing_ok=True)
            raise DownloadError(HTTPStatus.INTERNAL_SERVER_ERROR, "File is empty")

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[Any, Any, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            if not media_item.partial_file.is_file():
                media_item.partial_file.touch()

        return asyncio.to_thread(prepare)

    def _create_speed_checker(self, hook: ProgressHook) -> Callable[[], None]:
        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read
            if not self._slow_download_threshold:
                return

            if hook.speed() > self._slow_download_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.perf_counter()
            elif time.perf_counter() - last_slow_speed_read > _SLOW_DOWNLOAD_PERIOD:
                raise SlowDownloadError

        return check_download_speed

    async def _finalize_download(self, media_item: MediaItem) -> None:
        await asyncio.to_thread(Path.chmod, media_item.complete_file, 0o666)
        if media_item.is_segment:
            return

        media_item.downloaded = True
        await self.manager.hash_manager.hash_client.hash_item_during_download(media_item)
        self.manager.add_completed(media_item)
        await self.mark_completed(media_item.domain, media_item)
        await _set_file_datetime(media_item, media_item.complete_file)
        logger.info(f"Download finished: {media_item.url}")

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def mark_incomplete(self, media_item: MediaItem) -> None:
        """Marks the media item as incomplete in the database."""
        if media_item.is_segment:
            return
        await self.manager.db_manager.history_table.insert_incompleted(media_item.domain, media_item)

    async def mark_completed(self, domain: str, media_item: MediaItem) -> None:
        self.manager.progress.files.add_completed()
        await self.manager.db_manager.history_table.mark_complete(domain, media_item)


def _check_filesize_limits(media: MediaItem) -> bool:
    """Checks if the file size is within the limits."""
    limits = config.get().file_size_limits.ranges
    assert media.filesize is not None
    if media.ext in constants.FileFormats.IMAGE:
        return media.filesize in limits.image
    if media.ext in constants.FileFormats.VIDEO:
        return media.filesize in limits.video
    return media.filesize in limits.other


def _check_content_type(ext: str, headers: Mapping[str, str]) -> None:
    content_type: str = headers.get("Content-Type", "")
    if not content_type:
        return

    override_key = next((name for name in _CONTENT_TYPES_OVERRIDES if name in content_type), "<NO_OVERRIDE>")
    content_type = (_CONTENT_TYPES_OVERRIDES.get(override_key) or content_type).lower()
    if ("html" in content_type or "text" in content_type) and ext.lower() not in constants.FileFormats.TEXT:
        msg = f"Received '{content_type}', was expecting other"
        raise InvalidContentTypeError(message=msg)


def _get_last_modified(headers: Mapping[str, str]) -> int | None:
    if date_str := headers.get("Last-Modified"):
        return dates.parse_http(date_str)


async def _set_file_datetime(media_item: MediaItem, complete_file: Path) -> None:
    if media_item.is_segment:
        return

    if config.get().download.disable_file_timestamps:
        return

    if not media_item.timestamp:
        logger.warning(f"Unable to parse upload date for {media_item.url}, using current datetime as file datetime")
        return

    # 1. try setting creation date
    try:
        await dates.set_creation_time(media_item.complete_file, media_item.timestamp)

    except (OSError, ValueError):
        pass

    # 2. try setting modification and access date
    try:
        await asyncio.to_thread(os.utime, complete_file, (media_item.timestamp, media_item.timestamp))
    except OSError:
        pass
