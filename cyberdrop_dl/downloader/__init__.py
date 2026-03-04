from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import time
from collections.abc import Mapping
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, Protocol, TypeVar, final

import aiofiles
from aiohttp import ClientConnectorError, ClientError, ClientResponseError, hdrs
from aiolimiter import AsyncLimiter

from cyberdrop_dl import aio, constants, ffmpeg, storage
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
from cyberdrop_dl.exceptions import (
    DownloadError,
    DurationError,
    InvalidContentTypeError,
    RestrictedDateRangeError,
    RestrictedFiletypeError,
    SkipDownloadError,
    SlowDownloadError,
)
from cyberdrop_dl.tui.common import ProgressHook
from cyberdrop_dl.utils import dates, error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Mapping

    from cyberdrop_dl.clients.http import HTTPClient
    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.database import Database
    from cyberdrop_dl.manager import Manager
    from cyberdrop_dl.tui import TUI
    from cyberdrop_dl.tui.common import ProgressHook

_P = ParamSpec("_P")
_R = TypeVar("_R")

logger = logging.getLogger(__name__)


def _retry(
    func: Callable[[Downloader, MediaItem], Coroutine[None, None, _R]],
) -> Callable[[Downloader, MediaItem], Coroutine[None, None, _R]]:
    @functools.wraps(func)
    async def wrapper(self: Downloader, media_item: MediaItem) -> _R:
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


_file_locks: aio.WeakAsyncLocks[str] = aio.WeakAsyncLocks()
_NULL_CONTEXT: contextlib.nullcontext[None] = contextlib.nullcontext()
_CONTENT_TYPES_OVERRIDES: dict[str, str] = {
    "text/vnd.trolltech.linguist": "video/MP2T",
}
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds
_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}
_VIDEO_HLS_BATCH_SIZE = 10
_AUDIO_HLS_BATCH_SIZE = 50


class ReadableStream(Protocol):
    def iter_chunked(self, size: int, /) -> AsyncIterator[bytes]: ...


class SpeedLimiter(AsyncLimiter):
    __slots__ = ()

    async def acquire(self, amount: float = 1) -> None:
        if self.max_rate <= 0:
            return
        await super().acquire(amount)


class StreamDownloader:
    """Low level class to that performs the actual download"""

    SUPPORT_RANGES: ClassVar[bool] = True

    def __init__(self, manager: Manager) -> None:
        self.manager: Manager = manager
        self.config: Config = manager.config
        self.client: HTTPClient = manager.client
        self.database: Database = manager.database
        self.tui: TUI = manager.tui
        self.slow_download_threshold: int = manager.config.runtime.slow_download_speed
        self.chunk_size: int = manager.config.rate_limits.chunk_size
        self.speed_limiter: SpeedLimiter = SpeedLimiter(manager.config.rate_limits.download_speed_limit, time_period=1)

    @final
    async def download(self, media_item: MediaItem) -> bool:
        """Starts a file download.

        Returns `True` if the file was downloaded successfully. `False` if the file was downloaded but deleted by config options"""
        if not media_item.is_segment and self.config.download.skip_download_mark_completed:
            logger.info(f"Download skipped {media_item.url} due to mark completed option")
            self.tui.files.add_skipped()
            await self.database.history_table.mark_complete(media_item.domain, media_item)
            return False

        # We need to make the request first to get the file size and create the progress hook for the UI
        # But the hook has to outlive the request itself so we can keep using it later for hashing;
        # hashing while the request is still active would tie up a socket

        async with self.__request_download(media_item) as (stream, progress_hook):
            try:
                await self.__read_stream(media_item, stream, progress_hook)
            except Exception:
                progress_hook.done()
                raise

        # Move the file to its final destination, hash it and them update database
        # TODO: move all the databse updates out of the downloader. Use ctx manager
        with progress_hook:
            _ = await asyncio.gather(
                aio.move(media_item.partial_file, media_item.complete_file),
                self.database.history_table.add_filesize(media_item.domain, media_item),
            )
            try:
                await self._post_download_check(media_item)
            except ValueError:
                return False
            else:
                await self.__finalize_download(media_item)
                return True

    @contextlib.asynccontextmanager
    async def __request_download(self, media_item: MediaItem) -> AsyncGenerator[tuple[ReadableStream, ProgressHook]]:
        if self.SUPPORT_RANGES and (size := await aio.get_size(media_item.partial_file)):
            resume_point = size
            media_item.headers[hdrs.RANGE] = f"bytes={size}-"
        else:
            resume_point = 0

        async with self.__request_context(media_item.real_url, media_item.domain, media_item.headers) as resp:
            await self.__check_resp(media_item, resp)
            media_item.filesize = resume_point + int(resp.headers.get(hdrs.CONTENT_LENGTH, 0))
            if media_item.is_segment:
                progress_hook = self.tui.downloads.new_hls_seg_task()
            else:
                progress_hook = self.tui.downloads.new_task(media_item.filename, media_item.filesize)
            if resume_point:
                progress_hook.advance(resume_point)

            yield resp, progress_hook

    async def __check_resp(self, media_item: MediaItem, resp: AbstractResponse[Any]) -> None:
        if resp.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
            await aio.unlink(media_item.partial_file)

        await self.client.check_http_status(resp, is_download=True)
        if not media_item.is_segment:
            _check_content_type(media_item.ext, resp.headers)

        if resp.status != HTTPStatus.PARTIAL_CONTENT:
            await aio.unlink(media_item.partial_file, missing_ok=True)

        if (
            not media_item.is_segment
            and not media_item.timestamp
            and (last_modified := _get_last_modified(resp.headers))
        ):
            logger.warning(
                f"Unable to parse upload date for {media_item.url}, using `Last-Modified` header as file datetime"
            )
            media_item.timestamp = last_modified

        _check_content_length(resp.headers)

    @contextlib.asynccontextmanager
    async def __request_context(
        self, url: AbsoluteHttpURL, domain: str, headers: dict[str, str]
    ) -> AsyncGenerator[AbstractResponse[Any]]:
        await asyncio.sleep(self.config.rate_limits.total_download_delay)
        async with self.client.rate_limiter.acquire(domain):
            if domain in _USE_IMPERSONATION:
                resp = await self.client.curl_session.get(str(url), stream=True, headers=headers)
                try:
                    yield AbstractResponse.from_resp(resp)
                finally:
                    await resp.aclose()
                return

            async with self.client.aiohttp_session.get(url, headers=headers) as resp:
                yield AbstractResponse.from_resp(resp)

    async def __read_stream(self, media_item: MediaItem, stream: ReadableStream, progress_hook: ProgressHook) -> None:
        check_free_space = storage.create_free_space_checker(media_item)

        await check_free_space()
        await self._pre_download_check(media_item)

        empty = True
        check_speed = self.__create_speed_checker(progress_hook)
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

    async def _post_download_check(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return

        if media_item.duration is None:
            media_item.duration = await _probe_duration(media_item)

        if _filter_by_duration(media_item, self.config):
            msg = f"Download deleted {media_item.url} due to duration restrictions ({media_item.duration})"
        elif _filter_by_filesize(media_item, self.config):
            msg = f"Download deleted {media_item.url} due to filesize restrictions ({media_item.filesize})"
        else:
            return

        await aio.unlink(media_item.complete_file)
        logger.warning(msg)
        self.tui.files.add_skipped()
        raise ValueError

    def __create_speed_checker(self, hook: ProgressHook) -> Callable[[], None]:
        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read
            if not self.slow_download_threshold:
                return

            if hook.speed > self.slow_download_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.monotonic()
            elif time.monotonic() - last_slow_speed_read > _SLOW_DOWNLOAD_PERIOD:
                raise SlowDownloadError

        return check_download_speed

    async def __finalize_download(self, media_item: MediaItem) -> None:
        media_item.downloaded = True
        await aio.chmod(media_item.complete_file, 0o666)
        if media_item.is_segment:
            return

        _ = await asyncio.gather(
            self.manager.hasher.in_place_hash(media_item),
            self.database.history_table.mark_complete(media_item.domain, media_item),
        )
        self.manager.add_completed(media_item)
        self.tui.files.add_completed()
        if not self.config.download.disable_file_timestamps:
            await _set_file_datetime(media_item)
        logger.info(f"Download finished: {media_item.url}")


class Downloader:
    """High level class to handle download retries, slots limiter, and skip by config options"""

    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        self.client: HTTPClient = manager.client
        self.database: Database = manager.database
        self.config: Config = manager.config
        self.tui: TUI = manager.tui
        self.stream: StreamDownloader = StreamDownloader(manager)
        self.processed_items: set[str] = set()

    @final
    async def run(self, media_item: MediaItem) -> bool:
        if media_item.url.path in self.processed_items and not self.config.runtime.ignore_history:
            return False

        async with self.__limiter(media_item):
            if not media_item.is_segment:
                logger.info(f"Download starting: {media_item.url}")

            return bool(await self._download(media_item))

    @contextlib.asynccontextmanager
    async def __limiter(self, media_item: MediaItem):
        if media_item.is_segment:
            yield
            return

        await self.database.history_table.insert_incompleted(media_item.domain, media_item)
        server = media_item.real_url.host
        async with (
            self.client.download_limiter.acquire(media_item.domain, server),
            _file_locks[media_item.filename],
        ):
            logger.debug(f"Lock for {media_item.filename!r} acquired")
            self.processed_items.add(media_item.db_path)
            try:
                yield
            finally:
                logger.debug(f"Lock for {media_item.filename!r} released")

    async def _check_skip_by_config(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return

        if media_item.duration is None:
            media_item.duration = await self.database.history_table.get_duration(media_item.domain, media_item)
        if media_item.duration is None:
            media_item.duration = await _probe_duration(media_item)

        if _filter_by_extension(media_item, self.config):
            raise RestrictedFiletypeError(origin=media_item)
        if _filter_by_duration(media_item, self.config):
            raise DurationError(origin=media_item)
        if _filter_by_date(media_item, self.config):
            raise RestrictedDateRangeError(origin=media_item)
        if _filter_by_filesize(media_item, self.config):
            msg = f"File size({media_item.filesize}s) out of config range"
            raise SkipDownloadError("Filesize Not Allowed", message=msg, origin=media_item)

    @error_handling_wrapper
    @_retry
    async def _download(self, media_item: MediaItem) -> bool | None:
        try:
            await self._check_skip_by_config(media_item)
            return await self.stream.download(media_item)

        except SkipDownloadError as e:
            logger.info(f"Download skipped {media_item.url}: {e}")
            self.tui.files.add_skipped()

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


def _filter_by_extension(media_item: MediaItem, config: Config) -> bool:
    ignore_options = config.ignore
    ext = media_item.ext

    if ignore_options.exclude_images and ext in constants.FileExt.IMAGE:
        return False
    if ignore_options.exclude_videos and ext in constants.FileExt.VIDEO:
        return False
    if ignore_options.exclude_audio and ext in constants.FileExt.AUDIO:
        return False

    return ext in constants.FileExt.MEDIA or not ignore_options.exclude_other


def _filter_by_date(media_item: MediaItem, config: Config) -> bool:
    if not media_item.datetime:
        return False

    date = media_item.datetime.date()
    options = config.ignore
    return bool(
        (options.exclude_before and date < options.exclude_before)
        or (options.exclude_after and date > options.exclude_after)
    )


# TODO: This needs a better check to include the actual domain used
def _check_content_length(headers: Mapping[str, str]) -> None:
    content_length, content_type = headers[hdrs.CONTENT_LENGTH], headers.get(hdrs.CONTENT_TYPE)
    if content_type is None:
        return

    match [content_length, content_type]:
        case ["322509", "video/mp4"]:
            raise DownloadError("Bunkr Maintenance", message="Bunkr under maintenance")
        case ["73003", "video/mp4"]:
            raise DownloadError(410, "Video removed")  # efukt
        case _:
            return


async def _probe_duration(media_item: MediaItem) -> float | None:
    is_video = media_item.ext in constants.FileExt.VIDEO
    is_audio = media_item.ext in constants.FileExt.AUDIO
    if not (is_video or is_audio):
        return

    if media_item.downloaded:
        properties = await ffmpeg.probe(media_item.complete_file)
    else:
        properties = await ffmpeg.probe(media_item.url, headers=media_item.headers)

    if properties.format.duration:
        return properties.format.duration
    if is_video and properties.video:
        return properties.video.duration
    if is_audio and properties.audio:
        return properties.audio.duration


def _filter_by_duration(media_item: MediaItem, config: Config) -> bool:
    """Checks the file runtime against the config runtime limits."""

    if media_item.duration is None:
        return False

    limits = config.media_duration_limits.ranges
    if media_item.ext in constants.FileExt.VIDEO:
        return media_item.duration not in limits.video
    if media_item.ext in constants.FileExt.AUDIO:
        return media_item.duration not in limits.audio
    return False


def _filter_by_filesize(item: MediaItem, config: Config) -> bool:
    """Checks if the file size is within the limits."""

    if item.filesize is None:
        return False

    limits = config.file_size_limits.ranges
    if item.ext in constants.FileExt.IMAGE:
        return item.filesize not in limits.image
    if item.ext in constants.FileExt.VIDEO:
        return item.filesize not in limits.video
    return item.filesize not in limits.other


def _check_content_type(ext: str, headers: Mapping[str, str]) -> None:
    content_type = headers.get(hdrs.CONTENT_TYPE)
    if not content_type:
        return

    if override_key := next((name for name in _CONTENT_TYPES_OVERRIDES if name in content_type), None):
        content_type = _CONTENT_TYPES_OVERRIDES[override_key]
    else:
        content_type = content_type.lower()

    if ("html" in content_type or "text" in content_type) and ext not in constants.FileExt.TEXT:
        msg = f"Received '{content_type}', was expecting other"
        raise InvalidContentTypeError(message=msg)


def _get_last_modified(headers: Mapping[str, str]) -> int | None:
    if date_str := headers.get(hdrs.LAST_MODIFIED):
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
