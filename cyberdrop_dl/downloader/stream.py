from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Protocol

import aiofiles
from aiolimiter import AsyncLimiter

from cyberdrop_dl import aio, config, constants, storage
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import DownloadError, InvalidContentTypeError, SlowDownloadError
from cyberdrop_dl.tui.common import ProgressHook
from cyberdrop_dl.utils import dates

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Mapping

    import aiohttp

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.tui.common import ProgressHook


class ReadableStream(Protocol):
    def iter_chunked(self, size: int, /) -> AsyncIterator[bytes]: ...


class SpeedLimiter(AsyncLimiter):
    __slots__ = ()

    async def acquire(self, amount: float = 1) -> None:
        if self.max_rate <= 0:
            return
        await super().acquire(amount)


_CONTENT_TYPES_OVERRIDES: dict[str, str] = {
    "text/vnd.trolltech.linguist": "video/MP2T",
}
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds
_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}


logger = logging.getLogger(__name__)


class StreamDownloader:
    """Low level class to that performs the actual download + database updates"""

    SUPPORT_RANGES: ClassVar[bool] = True
    speed_limiter: SpeedLimiter

    def __init__(self, manager: Manager) -> None:
        config = manager.config
        self.manager: Manager = manager
        self.config: Config = manager.config
        self.slow_download_threshold: int = config.runtime.slow_download_speed
        self.chunk_size: int = config.rate_limits.chunk_size
        self.speed_limiter = SpeedLimiter(config.rate_limits.download_speed_limit, time_period=1)

    async def download(self, media_item: MediaItem) -> bool:
        """Starts a file download.

        Returns `True` if the file was downloaded successfully. `False` if the file was downloaded but deleted by config options"""
        if not media_item.is_segment and self.config.download.skip_download_mark_completed:
            logger.info(f"Download skipped {media_item.url} due to mark completed option")
            self.manager.tui.files.add_skipped()
            await self.manager.db_manager.history_table.mark_complete(media_item.domain, media_item)
            return False

        # We need to make the request first to get the file size and create the progress hook for the UI
        # But the hook has to outlive the request itself so we can keep using it later for hashing;
        # hashing while the request is still active would tie up a socket

        async with self._request_download(media_item) as (stream, progress_hook):
            try:
                await self._download_stream(media_item, stream, progress_hook)
            except Exception:
                progress_hook.done()
                raise

        # Move the file to its final destination, hash it and them update database
        with progress_hook:
            _ = await asyncio.to_thread(shutil.move, media_item.partial_file, media_item.complete_file)
            try:
                await self._check_file_duration(media_item)
            except ValueError:
                return False
            else:
                await self._finalize_download(media_item)
                return True

    async def _check_file_duration(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        has_valid_duration = await self.manager.http_client.check_file_duration(media_item)
        await self.manager.db_manager.history_table.add_filesize(media_item.domain, media_item)
        if not has_valid_duration:
            await asyncio.to_thread(media_item.complete_file.unlink)
            logger.warning(f"Download deleted {media_item.url} due to runtime restrictions")
            self.manager.tui.files.add_skipped()
            raise ValueError

    @contextlib.asynccontextmanager
    async def _request_download(self, media_item: MediaItem) -> AsyncGenerator[tuple[ReadableStream, ProgressHook]]:
        if self.SUPPORT_RANGES and (size := await aio.get_size(media_item.partial_file)):
            resume_point = size
            media_item.headers["Range"] = f"bytes={size}-"
        else:
            resume_point = 0

        await asyncio.sleep(self.config.rate_limits.total_download_delay)
        async with self.__request_context(media_item.real_url, media_item.domain, media_item.headers) as resp:
            await self._check_resp(media_item, resp)
            media_item.filesize = resume_point + int(resp.headers.get("Content-Length", 0))
            if media_item.is_segment:
                progress_hook = self.manager.tui.downloads.current_hook.remove_done_callback()
            else:
                progress_hook = self.manager.tui.downloads.new_hook(media_item.filename, media_item.filesize)
            if resume_point:
                progress_hook.advance(resume_point)
            yield self._get_stream_reader_(resp), progress_hook

    async def _check_resp(self, media_item: MediaItem, resp: aiohttp.ClientResponse | AbstractResponse) -> None:
        if resp.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
            await asyncio.to_thread(media_item.partial_file.unlink)

        await self.manager.http_client.check_http_status(resp, is_download=True)
        if not media_item.is_segment:
            _check_content_type(media_item.ext, resp.headers)

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

    @staticmethod
    def _get_stream_reader_(resp: aiohttp.ClientResponse | AbstractResponse) -> AbstractResponse | aiohttp.StreamReader:
        if isinstance(resp, AbstractResponse):
            return resp
        return resp.content

    @contextlib.asynccontextmanager
    async def __request_context(
        self, url: AbsoluteHttpURL, domain: str, headers: dict[str, str]
    ) -> AsyncGenerator[AbstractResponse | aiohttp.ClientResponse]:
        if domain in _USE_IMPERSONATION:
            resp = await self.manager.http_client.curl_session.get(str(url), stream=True, headers=headers)
            try:
                yield AbstractResponse.from_resp(resp)
            finally:
                await resp.aclose()
            return

        async with self.manager.http_client.aiohttp_session.get(url, headers=headers) as resp:
            yield resp

    async def _download_stream(
        self, media_item: MediaItem, stream: ReadableStream, progress_hook: ProgressHook
    ) -> None:
        """Appends content to a file."""

        check_free_space = storage.create_free_space_checker(media_item)

        await check_free_space()
        await self._pre_download_check(media_item)

        empty = True
        check_speed = self._create_speed_checker(progress_hook)
        async with aiofiles.open(media_item.partial_file, mode="ab") as f:
            async for chunk in stream.iter_chunked(self.chunk_size):
                await check_free_space()
                n_bytes = len(chunk)
                await self.speed_limiter.acquire(n_bytes)
                _ = await f.write(chunk)
                if empty:
                    empty = not bool(n_bytes)
                progress_hook.advance(n_bytes)
                check_speed()

        if empty:
            await aio.unlink(media_item.partial_file, missing_ok=True)
            raise DownloadError(HTTPStatus.INTERNAL_SERVER_ERROR, "File is empty")

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[None, None, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            if not media_item.partial_file.is_file():
                media_item.partial_file.touch()

        return asyncio.to_thread(prepare)

    def _create_speed_checker(self, hook: ProgressHook) -> Callable[[], None]:
        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read
            if not self.slow_download_threshold:
                return

            if hook.speed() > self.slow_download_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.perf_counter()
            elif time.perf_counter() - last_slow_speed_read > _SLOW_DOWNLOAD_PERIOD:
                raise SlowDownloadError

        return check_download_speed

    async def _finalize_download(self, media_item: MediaItem) -> None:
        media_item.downloaded = True
        await asyncio.to_thread(Path.chmod, media_item.complete_file, 0o666)
        if media_item.is_segment:
            return
        await self.manager.hash_manager.hash_client.run(media_item)
        self.manager.add_completed(media_item)
        self.manager.tui.files.add_completed()
        await self.manager.db_manager.history_table.mark_complete(media_item.domain, media_item)
        if not self.config.download.disable_file_timestamps:
            await _set_file_datetime(media_item)
        logger.info(f"Download finished: {media_item.url}")


def _check_filesize_limits(item: MediaItem) -> bool:
    """Checks if the file size is within the limits."""
    limits = config.get().file_size_limits.ranges
    assert item.filesize is not None
    if item.ext in constants.FileFormats.IMAGE:
        return item.filesize in limits.image
    if item.ext in constants.FileFormats.VIDEO:
        return item.filesize in limits.video
    return item.filesize in limits.other


def _check_content_type(ext: str, headers: Mapping[str, str]) -> None:
    content_type = headers.get("Content-Type")
    if not content_type:
        return

    if override_key := next((name for name in _CONTENT_TYPES_OVERRIDES if name in content_type), None):
        content_type = _CONTENT_TYPES_OVERRIDES[override_key]
    else:
        content_type = content_type.lower()

    if ("html" in content_type or "text" in content_type) and ext.lower() not in constants.FileFormats.TEXT:
        msg = f"Received '{content_type}', was expecting other"
        raise InvalidContentTypeError(message=msg)


def _get_last_modified(headers: Mapping[str, str]) -> int | None:
    if date_str := headers.get("Last-Modified"):
        return dates.to_timestamp(dates.parse_http(date_str))


async def _set_file_datetime(item: MediaItem) -> None:
    if not item.timestamp:
        logger.warning(f"Unable to parse upload date for {item.url}, using current datetime as file datetime")
        return

    # 1. try setting creation date
    try:
        await dates.set_creation_time(item.complete_file, item.timestamp)

    except (OSError, ValueError):
        pass

    # 2. try setting modification and access date
    try:
        await asyncio.to_thread(os.utime, item.complete_file, (item.timestamp, item.timestamp))
    except OSError:
        pass
