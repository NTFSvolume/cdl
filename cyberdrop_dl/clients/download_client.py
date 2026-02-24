from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiofiles

from cyberdrop_dl import config, constants
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.exceptions import DownloadError, InvalidContentTypeError, SlowDownloadError
from cyberdrop_dl.utils import aio, dates
from cyberdrop_dl.utils.logger import log
from cyberdrop_dl.utils.utilities import get_size_or_none

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping
    from typing import Any

    import aiohttp

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.managers.client_manager import HttpClient
    from cyberdrop_dl.progress._common import ProgressHook


_CONTENT_TYPES_OVERRIDES: dict[str, str] = {"text/vnd.trolltech.linguist": "video/MP2T"}
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds
_FREE_SPACE_CHECK_PERIOD: int = 5  # Check every 5 chunks
_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}


logger = logging.getLogger(__name__)


class DownloadClient:
    """Low level class to that performs the actual download + database updates"""

    def __init__(self, manager: Manager, http_client: HttpClient) -> None:
        self.manager = manager
        self.http_client = http_client
        self.download_speed_threshold = config.get().runtime_options.slow_download_speed
        self._supports_ranges: bool = True

    async def _download(self, media_item: MediaItem) -> bool:
        resume_point = 0
        if self._supports_ranges and (size := await asyncio.to_thread(get_size_or_none, media_item.partial_file)):
            resume_point = size
            media_item.headers["Range"] = f"bytes={size}-"

        await asyncio.sleep(config.get().rate_limits.total_delay)
        download_url = media_item.debrid_link or media_item.url
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
            _ = _get_content_type(media_item.ext, resp.headers)

        media_item.filesize = int(resp.headers.get("Content-Length", "0")) or None
        if resp.status != HTTPStatus.PARTIAL_CONTENT:
            await asyncio.to_thread(media_item.partial_file.unlink, missing_ok=True)

        if (
            not media_item.is_segment
            and not media_item.timestamp
            and (last_modified := _get_last_modified(resp.headers))
        ):
            msg = f"Unable to parse upload date for {media_item.url}, using `Last-Modified` header as file datetime"
            log(msg, 30)
            media_item.timestamp = last_modified

        size = (media_item.filesize + resume_point) if media_item.filesize is not None else None

        if not media_item.is_segment:
            self.manager.progress_manager.downloads.new_hook(media_item.filename, size)

        await self._append_content(media_item, self._get_resp_reader(resp))
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
            resp = await self.http_client._curl_session.get(str(url), stream=True, headers=headers)
            try:
                yield AbstractResponse.from_resp(resp)
            finally:
                await resp.aclose()
            return

        async with self.http_client._download_session.get(url, headers=headers) as resp:
            yield resp

    async def _append_content(self, media_item: MediaItem, content: aiohttp.StreamReader | AbstractResponse) -> None:
        """Appends content to a file."""

        check_free_space = self.make_free_space_checker(media_item)
        await check_free_space()
        await self._pre_download_check(media_item)

        with self.manager.progress_manager.downloads.current_hook as hook:
            check_download_speed = self.make_speed_checker(hook)

            async with aiofiles.open(media_item.partial_file, mode="ab") as f:
                async for chunk in content.iter_chunked(self.http_client.speed_limiter.chunk_size):
                    await check_free_space()
                    chunk_size = len(chunk)
                    await self.http_client.speed_limiter.acquire(chunk_size)
                    await f.write(chunk)
                    hook.advance(chunk_size)
                    check_download_speed()

        await self._post_download_check(media_item)

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[Any, Any, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            if not media_item.partial_file.is_file():
                media_item.partial_file.touch()

        return asyncio.to_thread(prepare)

    async def _post_download_check(self, media_item: MediaItem, *_) -> None:
        if not await aio.get_size(media_item.partial_file):
            await aio.unlink(media_item.partial_file, missing_ok=True)
            raise DownloadError(HTTPStatus.INTERNAL_SERVER_ERROR, message="File is empty")

    def make_free_space_checker(self, media_item: MediaItem) -> Callable[[], Coroutine[Any, Any, None]]:
        current_chunk = 0

        async def check_free_space() -> None:
            nonlocal current_chunk
            if current_chunk % _FREE_SPACE_CHECK_PERIOD == 0:
                await self.manager.storage_manager.check_free_space(media_item)
            current_chunk += 1

        return check_free_space

    def make_speed_checker(self, hook: ProgressHook) -> Callable[[], None]:
        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read
            if not self.download_speed_threshold:
                return

            if hook.speed() > self.download_speed_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.perf_counter()
            elif time.perf_counter() - last_slow_speed_read > _SLOW_DOWNLOAD_PERIOD:
                raise SlowDownloadError

        return check_download_speed

    async def download_file(self, media_item: MediaItem) -> bool:
        """Starts a file."""
        if config.get().download_options.skip_download_mark_completed and not media_item.is_segment:
            log(f"Download skipped {media_item.url} due to mark completed option", 10)
            self.manager.progress_manager.files.add_skipped()
            await self.mark_completed(media_item.domain, media_item)
            return False

        downloaded = await self._download(media_item)

        if downloaded:
            _ = await asyncio.to_thread(media_item.partial_file.rename, media_item.complete_file)
            if not media_item.is_segment:
                has_valid_duration = await self.http_client.check_file_duration(media_item)
                await self.manager.db_manager.history_table.add_duration(media_item.domain, media_item)
                await self.manager.db_manager.history_table.add_filesize(media_item.domain, media_item)
                if not has_valid_duration:
                    await asyncio.to_thread(media_item.complete_file.unlink)
                    logger.warning(f"Download deleted {media_item.url} due to runtime restrictions")
                    await self.mark_incomplete(media_item)
                    self.manager.progress_manager.files.add_skipped()
                    return False

            await self._finalize_download(media_item)

        return downloaded

    async def _finalize_download(self, media_item: MediaItem) -> None:
        await asyncio.to_thread(Path.chmod, media_item.complete_file, 0o666)
        if media_item.is_segment:
            return

        media_item.downloaded = True
        await self.manager.hash_manager.hash_client.hash_item_during_download(media_item)
        self.manager.add_completed(media_item)
        await self.mark_completed(media_item.domain, media_item)
        await _set_file_datetime(media_item, media_item.complete_file)
        log(f"Download finished: {media_item.url}")

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def mark_incomplete(self, media_item: MediaItem) -> None:
        """Marks the media item as incomplete in the database."""
        if media_item.is_segment:
            return
        await self.manager.db_manager.history_table.insert_incompleted(media_item.domain, media_item)

    async def mark_completed(self, domain: str, media_item: MediaItem) -> None:
        self.manager.progress_manager.files.add_completed()
        await self.manager.db_manager.history_table.mark_complete(domain, media_item)


def _check_filesize_limits(media: MediaItem) -> bool:
    """Checks if the file size is within the limits."""
    file_size_limits = config.get().file_size_limits
    max_video_filesize = file_size_limits.maximum_video_size or float("inf")
    min_video_filesize = file_size_limits.minimum_video_size
    max_image_filesize = file_size_limits.maximum_image_size or float("inf")
    min_image_filesize = file_size_limits.minimum_image_size
    max_other_filesize = file_size_limits.maximum_other_size or float("inf")
    min_other_filesize = file_size_limits.minimum_other_size

    assert media.filesize is not None
    if media.ext in constants.FileFormats.IMAGE:
        proceed = min_image_filesize < media.filesize < max_image_filesize
    elif media.ext in constants.FileFormats.VIDEO:
        proceed = min_video_filesize < media.filesize < max_video_filesize
    else:
        proceed = min_other_filesize < media.filesize < max_other_filesize

    return proceed


def _get_content_type(ext: str, headers: Mapping[str, str]) -> str | None:
    content_type: str = headers.get("Content-Type", "")
    content_length = headers.get("Content-Length")
    if not content_type and not content_length:
        msg = "No content type in response headers"
        raise InvalidContentTypeError(message=msg)

    if not content_type:
        return None

    override_key = next((name for name in _CONTENT_TYPES_OVERRIDES if name in content_type), "<NO_OVERRIDE>")
    override: str | None = _CONTENT_TYPES_OVERRIDES.get(override_key)
    content_type = override or content_type
    content_type = content_type.lower()

    if _is_html_or_text(content_type) and ext.lower() not in constants.FileFormats.TEXT:
        msg = f"Received '{content_type}', was expecting other"
        raise InvalidContentTypeError(message=msg)

    return content_type


def _get_last_modified(headers: Mapping[str, str]) -> int | None:
    if date_str := headers.get("Last-Modified"):
        return dates.parse_http(date_str)


def _is_html_or_text(content_type: str) -> bool:
    return any(s in content_type for s in ("html", "text"))


async def _set_file_datetime(media_item: MediaItem, complete_file: Path) -> None:
    if media_item.is_segment:
        return

    if config.get().download_options.disable_file_timestamps:
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
