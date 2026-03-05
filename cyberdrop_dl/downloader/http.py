from __future__ import annotations

import asyncio
import contextlib
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar

import aiofiles
from aiohttp import hdrs
from typing_extensions import override

from cyberdrop_dl import aio, constants, storage
from cyberdrop_dl.clients.response import AbstractResponse
from cyberdrop_dl.data_structures.url_objects import DownloadProtocol
from cyberdrop_dl.downloader import DownloadManager, FileDownloader
from cyberdrop_dl.exceptions import DownloadError, InvalidContentTypeError
from cyberdrop_dl.utils import dates

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Coroutine, Mapping

    from cyberdrop_dl.clients.http import HTTPClient
    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem
    from cyberdrop_dl.database import Database
    from cyberdrop_dl.hasher import Hasher
    from cyberdrop_dl.tui import TUI, ProgressHook


logger = logging.getLogger(__name__)


_USE_IMPERSONATION: set[str] = {"vsco", "celebforum"}
_VIDEO_HLS_BATCH_SIZE = 10
_AUDIO_HLS_BATCH_SIZE = 50
_CONTENT_TYPES_OVERRIDES: dict[str, str] = {
    "text/vnd.trolltech.linguist": "video/MP2T",
}


class HTTPFileDownloader(FileDownloader):
    PROTOCOL: ClassVar[DownloadProtocol] = DownloadProtocol.HTTP
    SUPPORTS_RANGES: ClassVar[bool] = True

    def __init__(self, manager: DownloadManager) -> None:
        super().__init__(manager)
        self.config: Config = manager.config
        self.client: HTTPClient = manager.client
        self.database: Database = manager.database
        self.tui: TUI = manager.tui
        self.chunk_size: int = manager.config.rate_limits.chunk_size
        self.hasher: Hasher = manager.manager.hasher

    @override
    async def run(self, media_item: MediaItem) -> bool:
        """Starts a file download.

        Returns `True` if the file was successfully downloaded.
        `False` if the file was downloaded but deleted by config options

        Exceptions are propagated"""
        if not media_item.is_segment and self.config.download.skip_download_mark_completed:
            logger.info(f"Download skipped {media_item.url} due to mark completed option")
            self.tui.files.add_skipped()
            await self.database.history_table.mark_complete(media_item.domain, media_item)
            return False

        # We need to make the request first to get the file size and create the progress hook for the UI
        # But the hook has to outlive the request itself so we can keep using it later for hashing;
        # hashing while the request is still active would tie up a socket

        async with self.__request_download(media_item) as (stream, progress_hook):
            await self._pre_download_check(media_item)
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
            media_item.downloaded = True
            try:
                await self.dl_manager.post_download_check(media_item)
            except ValueError:
                media_item.downloaded = False
            else:
                await self.__finalize_download(media_item)

            return media_item.downloaded

    @contextlib.asynccontextmanager
    async def __request_download(
        self, media_item: MediaItem
    ) -> AsyncGenerator[tuple[AbstractResponse[Any], ProgressHook]]:
        if self.SUPPORTS_RANGES and (size := await aio.get_size(media_item.partial_file)):
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

    async def __read_stream(
        self,
        media_item: MediaItem,
        resp: AbstractResponse[Any],
        progress_hook: ProgressHook,
    ) -> None:
        check_free_space = storage.create_free_space_checker(media_item)
        check_speed = self.dl_manager.create_speed_checker(progress_hook)

        await check_free_space()

        async with aiofiles.open(media_item.partial_file, mode="ab") as f:
            async for chunk in resp.iter_chunked(self.chunk_size):
                await check_free_space()
                n_bytes = len(chunk)
                await self.dl_manager.speed_limiter.acquire(n_bytes)
                await f.write(chunk)
                progress_hook.advance(n_bytes)
                check_speed()

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[None, None, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            if not media_item.partial_file.is_file():
                media_item.partial_file.touch()

        return asyncio.to_thread(prepare)

    async def __finalize_download(self, media_item: MediaItem) -> None:
        await aio.chmod(media_item.complete_file, 0o666)
        if media_item.is_segment:
            return

        _ = await asyncio.gather(
            self.hasher.in_place_hash(media_item),
            self.database.history_table.mark_complete(media_item.domain, media_item),
        )
        await self.dl_manager.add_completed(media_item)
        logger.info(f"Download finished: {media_item.url}")


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
