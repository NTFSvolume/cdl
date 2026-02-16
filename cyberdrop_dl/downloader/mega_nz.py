from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Final, NamedTuple

import aiofiles
from mega.chunker import MegaChunker, get_chunks
from mega.errors import _CODE_TO_DESCRIPTIONS

from cyberdrop_dl.clients.download_client import DownloadClient
from cyberdrop_dl.downloader.downloader import Downloader
from cyberdrop_dl.exceptions import CDLBaseError, create_error_msg

if TYPE_CHECKING:
    from collections.abc import Coroutine

    import aiohttp
    from mega.data_structures import Crypto
    from yarl import URL

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers.manager import Manager


class MegaNzError(CDLBaseError):
    def __init__(self, error: str | int, **kwargs) -> None:
        super().__init__(f"MegaNZ Error ({error})", **kwargs)


class RequestError(MegaNzError):
    """Error in API request."""

    def __init__(self, msg: str | int) -> None:
        self.code = code = msg if isinstance(msg, int) else None

        if code is None:
            super().__init__(error=msg, message=msg)
            return

        name, message = _CODE_TO_DESCRIPTIONS[code]
        ui_failure = f"{name}[{code}]"
        super().__init__(ui_failure, message=message)
        if http_code := _HTTP_ERRORS_OVERRIDES.get(code):
            self.ui_failure = create_error_msg(http_code)


_BANDWIDTH_LIMIT_EXCEEDED: Final = 509

_HTTP_ERRORS_OVERRIDES: Final = {
    -9: HTTPStatus.GONE,
    -16: HTTPStatus.FORBIDDEN,
    -24: _BANDWIDTH_LIMIT_EXCEEDED,
    -401: _BANDWIDTH_LIMIT_EXCEEDED,
}


class DecryptData(NamedTuple):
    crypto: Crypto
    file_size: int = 0


class MegaDownloadClient(DownloadClient):
    def __init__(self, manager: Manager) -> None:
        super().__init__(manager, manager.client_manager)
        self.decrypt_mapping: dict[URL, tuple[Crypto, int]] = {}
        self._supports_ranges = False

    async def _append_content(self, media_item: MediaItem, content: aiohttp.StreamReader) -> None:
        """Appends content to a file."""

        assert media_item.task_id is not None
        check_free_space = self.make_free_space_checker(media_item)
        check_download_speed = self.make_speed_checker(media_item)
        await check_free_space()
        await self._pre_download_check(media_item)

        crypto = self.decrypt_mapping.pop(media_item.url)
        chunk_decryptor = MegaChunker(crypto.key, crypto.iv, crypto.meta_mac)

        async with aiofiles.open(media_item.partial_file, mode="ab") as f:
            for _, chunk_size in get_chunks(crypto.file_size):
                await self.manager.states.RUNNING.wait()
                raw_chunk = await content.readexactly(chunk_size)
                chunk = chunk_decryptor.read(raw_chunk)
                await check_free_space()
                chunk_size = len(chunk)

                await self.client_manager.speed_limiter.acquire(chunk_size)
                await f.write(chunk)
                self.manager.progress_manager.file_progress.advance_file(media_item.task_id, chunk_size)
                check_download_speed()

        await self._post_download_check(media_item)
        chunk_decryptor.check_integrity()

    def _pre_download_check(self, media_item: MediaItem) -> Coroutine[Any, Any, None]:
        def prepare() -> None:
            media_item.partial_file.parent.mkdir(parents=True, exist_ok=True)
            media_item.partial_file.unlink(missing_ok=True)  # We can't resume
            media_item.partial_file.touch()

        return asyncio.to_thread(prepare)


class MegaDownloader(Downloader):
    client: MegaDownloadClient

    @property
    def max_attempts(self):
        return 1

    def startup(self) -> None:
        """Starts the downloader."""
        self.client = MegaDownloadClient(self.manager)  # type: ignore[reportIncompatibleVariableOverride]
        self._semaphore = asyncio.Semaphore(self.manager.client_manager.get_download_slots(self.domain))

    def register(self, url: URL, crypto: Crypto, file_size: int) -> None:
        self.client.decrypt_mapping[url] = crypto, file_size
