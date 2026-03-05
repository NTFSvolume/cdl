from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import logging
import os
import time
from abc import ABC, abstractmethod
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, final

from aiohttp import ClientConnectorError, ClientError, ClientResponseError
from aiolimiter import AsyncLimiter
from typing_extensions import TypeVar

from cyberdrop_dl import aio, constants, ffmpeg
from cyberdrop_dl.exceptions import (
    DownloadError,
    DurationError,
    InvalidContentTypeError,
    RestrictedDateRangeError,
    RestrictedFiletypeError,
    SkipDownloadError,
    SlowDownloadError,
)
from cyberdrop_dl.utils import dates, error_handling_wrapper

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from cyberdrop_dl.clients.http import HTTPClient
    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import DownloadProtocol, MediaItem
    from cyberdrop_dl.database import Database
    from cyberdrop_dl.manager import Manager
    from cyberdrop_dl.tui import TUI, ProgressHook

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


logger = logging.getLogger(__name__)


_LOCKS: aio.WeakAsyncLocks[str] = aio.WeakAsyncLocks()
_NULL_CONTEXT: contextlib.nullcontext[None] = contextlib.nullcontext()
_SLOW_DOWNLOAD_PERIOD: int = 10  # seconds
_PROTOCOL_MAP: dict[DownloadProtocol, type[FileDownloader]] = {}


def _retry(
    func: Callable[[DownloadManager, MediaItem], Coroutine[None, None, _R]],
) -> Callable[[DownloadManager, MediaItem], Coroutine[None, None, _R]]:
    @functools.wraps(func)
    async def wrapper(self: DownloadManager, media_item: MediaItem) -> _R:
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


@dataclasses.dataclass(slots=True)
class FileDownloader(ABC):
    """Low level class to that performs the actual download"""

    PROTOCOL: ClassVar[DownloadProtocol]
    dl_manager: DownloadManager

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        existing_proto = _PROTOCOL_MAP.get(cls.PROTOCOL)
        assert existing_proto is None, (
            f"[{cls.__name__}] A downloader for protocol {cls.PROTOCOL} already exists {existing_proto}"
        )
        _PROTOCOL_MAP[cls.PROTOCOL] = cls

    @abstractmethod
    async def run(self, media_item: MediaItem) -> bool: ...


class SpeedLimiter(AsyncLimiter):
    __slots__ = ()

    async def acquire(self, amount: float = 1) -> None:
        if self.max_rate <= 0:
            return
        await super().acquire(amount)


class DownloadManager:
    """High level class to handle download retries, slots limiter, and skip by config options"""

    def __init__(self, manager: Manager) -> None:
        self.manager: Manager = manager
        self.client: HTTPClient = manager.client
        self.database: Database = manager.database
        self.config: Config = manager.config
        self.tui: TUI = manager.tui
        self.processed_items: set[str] = set()
        self.speed_limiter: SpeedLimiter = SpeedLimiter(
            manager.config.rate_limits.download_speed_limit,
            time_period=1,
        )
        self._completed_downloads: list[MediaItem] = []
        self.slow_download_threshold: int = manager.config.runtime.slow_download_speed

    @final
    async def add_completed(self, media_item: MediaItem) -> None:
        if media_item.is_segment:
            return
        if not self.config.download.disable_file_timestamps:
            await _set_file_datetime(media_item)
        self._completed_downloads.append(media_item)
        self.tui.files.add_completed()

    @final
    async def run(self, media_item: MediaItem) -> bool:
        if media_item.url.path in self.processed_items and not self.config.runtime.ignore_history:
            return False

        async with self.__limiter(media_item):
            if not media_item.is_segment:
                logger.info(f"Download starting: {media_item.url}")

            return bool(await self.__download(media_item))

    @contextlib.asynccontextmanager
    async def __limiter(self, media_item: MediaItem):
        if media_item.is_segment:
            yield
            return

        await self.database.history_table.insert_incompleted(media_item.domain, media_item)
        server = media_item.real_url.host
        async with (
            self.client.download_limiter.acquire(media_item.domain, server),
            _LOCKS[media_item.filename],
        ):
            logger.debug(f"Lock for {media_item.filename!r} acquired")
            self.processed_items.add(media_item.db_path)
            try:
                yield
            finally:
                logger.debug(f"Lock for {media_item.filename!r} released")

    async def __check_skip_by_config(self, media_item: MediaItem) -> None:
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

    async def post_download_check(self, media_item: MediaItem) -> None:
        if (await aio.get_size(media_item.partial_file)) == 0:
            await aio.unlink(media_item.partial_file)
            raise DownloadError(HTTPStatus.INTERNAL_SERVER_ERROR, "File is empty")

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

    @error_handling_wrapper
    @_retry
    async def __download(self, media_item: MediaItem) -> bool | None:
        try:
            await self.__check_skip_by_config(media_item)
            downloader = _PROTOCOL_MAP[media_item.protocol](self)
            return await downloader.run(media_item)

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

    @final
    def create_speed_checker(self, hook: ProgressHook) -> Callable[[], None]:
        if not self.slow_download_threshold:
            return lambda: None

        last_slow_speed_read = None

        def check_download_speed() -> None:
            nonlocal last_slow_speed_read

            if hook.speed > self.slow_download_threshold:
                last_slow_speed_read = None
            elif not last_slow_speed_read:
                last_slow_speed_read = time.monotonic()
            elif time.monotonic() - last_slow_speed_read > _SLOW_DOWNLOAD_PERIOD:
                raise SlowDownloadError

        return check_download_speed


def _filter_by_extension(media_item: MediaItem, config: Config) -> bool:
    options = config.ignore
    ext = media_item.ext

    return (
        (options.exclude_images and ext in constants.FileExt.IMAGE)
        or (options.exclude_videos and ext in constants.FileExt.VIDEO)
        or (options.exclude_audio and ext in constants.FileExt.AUDIO)
        or options.exclude_other
    )


def _filter_by_date(media_item: MediaItem, config: Config) -> bool:
    if not media_item.datetime:
        return False

    date = media_item.datetime.date()
    options = config.ignore
    return bool(
        (options.exclude_before and date < options.exclude_before)
        or (options.exclude_after and date > options.exclude_after)
    )


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
