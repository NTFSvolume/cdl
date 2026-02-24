from __future__ import annotations

import asyncio
import contextlib
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiofiles

from cyberdrop_dl import config, constants
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.exceptions import DownloadError, InvalidContentTypeError, SlowDownloadError
from cyberdrop_dl.utils import aio, dates
from cyberdrop_dl.utils.aio import WeakAsyncLocks
from cyberdrop_dl.utils.logger import log
from cyberdrop_dl.utils.utilities import get_size_or_none

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping
    from pathlib import Path
    from typing import Any

    import aiohttp

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.managers import Manager
    from cyberdrop_dl.managers.client_manager import ClientManager
    from cyberdrop_dl.progress._common import ProgressHook


_CONTENT_TYPES_OVERRIDES: dict[str, str] = {"text/vnd.trolltech.linguist": "video/MP2T"}
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds

_FREE_SPACE_CHECK_PERIOD: int = 5  # Check every 5 chunks
_NULL_CONTEXT: contextlib.nullcontext[None] = contextlib.nullcontext()
_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}


class DownloadClient:
    """AIOHTTP operations for downloading."""

    def __init__(self, manager: Manager, client_manager: ClientManager) -> None:
        self.manager = manager
        self.client_manager = client_manager
        self.download_speed_threshold = config.get().runtime_options.slow_download_speed
        self._server_locks = WeakAsyncLocks[str]()
        self.server_locked_domains: set[str] = set()
        self._supports_ranges: bool = True

    def server_limiter(self, domain: str, server: str) -> asyncio.Lock | contextlib.nullcontext[None]:
        if domain not in self.server_locked_domains:
            return _NULL_CONTEXT

        return self._server_locks[server]

    @contextlib.asynccontextmanager
    async def _track_errors(self, domain: str):
        with self.client_manager.request_context(domain):
            await self.client_manager.manager.states.RUNNING.wait()
            yield

    async def _download(self, domain: str, media_item: MediaItem) -> bool:
        resume_point = 0
        if self._supports_ranges and (size := await asyncio.to_thread(get_size_or_none, media_item.partial_file)):
            resume_point = size
            media_item.headers["Range"] = f"bytes={size}-"

        await asyncio.sleep(config.get().rate_limiting_options.total_delay)

        def process_response(resp: aiohttp.ClientResponse | AbstractResponse):
            return self._process_response(media_item, domain, resume_point, resp)

        download_url = media_item.debrid_link or media_item.url
        async with self.__request_context(download_url, media_item.domain, media_item.headers) as resp:
            return await process_response(resp)

    async def _process_response(
        self,
        media_item: MediaItem,
        domain: str,
        resume_point: int,
        resp: aiohttp.ClientResponse | AbstractResponse,
    ) -> bool:
        if resp.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
            await asyncio.to_thread(media_item.partial_file.unlink)

        _ = await self.client_manager.check_http_status(resp, download=True)

        if not media_item.is_segment:
            _ = _get_content_type(media_item.ext, resp.headers)

        media_item.filesize = int(resp.headers.get("Content-Length", "0")) or None
        if not media_item.complete_file:
            proceed, skip = await self.get_final_file_info(media_item, domain)
            self.client_manager.check_content_length(resp.headers)
            if skip:
                self.manager.progress_manager.files.add_skipped()
                return False
            if not proceed:
                if media_item.is_segment:
                    return True
                log(f"Skipping {media_item.url} as it has already been downloaded", 10)
                self.manager.progress_manager.files.add_previously_completed(False)
                await self.process_completed(media_item, domain)
                await self.handle_media_item_completion(media_item, downloaded=False)

                return False

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
            resp = await self.client_manager._curl_session.get(str(url), stream=True, headers=headers)
            try:
                yield AbstractResponse.from_resp(resp)
            finally:
                await resp.aclose()
            return

        async with self.client_manager._download_session.get(url, headers=headers) as resp:
            yield resp

    async def _append_content(self, media_item: MediaItem, content: aiohttp.StreamReader | AbstractResponse) -> None:
        """Appends content to a file."""

        check_free_space = self.make_free_space_checker(media_item)
        await check_free_space()
        await self._pre_download_check(media_item)

        with self.manager.progress_manager.downloads.current_hook as hook:
            check_download_speed = self.make_speed_checker(hook)

            async with aiofiles.open(media_item.partial_file, mode="ab") as f:
                async for chunk in content.iter_chunked(self.client_manager.speed_limiter.chunk_size):
                    await check_free_space()
                    chunk_size = len(chunk)
                    await self.client_manager.speed_limiter.acquire(chunk_size)
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

    async def download_file(self, domain: str, media_item: MediaItem) -> bool:
        """Starts a file."""
        if config.get().download_options.skip_download_mark_completed and not media_item.is_segment:
            log(f"Download Removed {media_item.url} due to mark completed option", 10)
            self.manager.progress_manager.files.add_skipped()
            # set completed path
            await self.process_completed(media_item, domain)
            return False

        async with self._track_errors(domain):
            downloaded = await self._download(domain, media_item)

        if downloaded:
            _ = await asyncio.to_thread(media_item.partial_file.rename, media_item.complete_file)
            if not media_item.is_segment:
                proceed = await self.client_manager.check_file_duration(media_item)
                await self.manager.db_manager.history_table.add_duration(domain, media_item)
                if not proceed:
                    log(f"Download Skip {media_item.url} due to runtime restrictions", 10)
                    await asyncio.to_thread(media_item.complete_file.unlink)
                    await self.mark_incomplete(media_item, media_item.domain)
                    self.manager.progress_manager.files.add_skipped()
                    return False
                await self.process_completed(media_item, domain)
                await self.handle_media_item_completion(media_item, downloaded=True)
        return downloaded

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def mark_incomplete(self, media_item: MediaItem) -> None:
        """Marks the media item as incomplete in the database."""
        if media_item.is_segment:
            return
        await self.manager.db_manager.history_table.insert_incompleted(media_item.domain, media_item)

    async def process_completed(self, media_item: MediaItem, domain: str) -> None:
        await self.mark_completed(domain, media_item)
        await self.add_file_size(domain, media_item)

    async def mark_completed(self, domain: str, media_item: MediaItem) -> None:
        await self.manager.db_manager.history_table.mark_complete(domain, media_item)

    async def add_file_size(self, domain: str, media_item: MediaItem) -> None:
        if await asyncio.to_thread(media_item.complete_file.is_file):
            await self.manager.db_manager.history_table.add_filesize(domain, media_item)

    async def handle_media_item_completion(self, media_item: MediaItem, downloaded: bool = False) -> None:
        """Sends to hash client to handle hashing and marks as completed/current download."""
        try:
            media_item.downloaded = downloaded
            await self.manager.hash_manager.hash_client.hash_item_during_download(media_item)
            self.manager.add_completed(media_item)
        except Exception:
            log(f"Error handling media item completion of: {media_item.complete_file}", 10, exc_info=True)


def get_file_location(media_item: MediaItem) -> Path:
    return media_item.download_folder / media_item.filename


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
