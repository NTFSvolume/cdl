from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from mega.chunker import MegaChunker, get_chunks

from cyberdrop_dl import aio
from cyberdrop_dl.data_structures import DownloadProtocol
from cyberdrop_dl.downloader.http import HTTPFileDownloader

if TYPE_CHECKING:
    import aiohttp

    from cyberdrop_dl.data_structures import Download


class MegaNzFileDownloader(HTTPFileDownloader):
    PROTOCOL: ClassVar[DownloadProtocol] = DownloadProtocol.MEGA_NZ
    SUPPORT_RANGES: ClassVar[bool] = False

    async def _append_content(self, media_item: Download, content: aiohttp.StreamReader) -> None:
        """Appends content to a file."""

        assert media_item.task_id is not None
        check_free_space = self.make_free_space_checker(media_item)
        check_download_speed = self._create_speed_checker(media_item)
        await check_free_space()
        await self._pre_download_check(media_item)

        crypto, file_size = media_item.extra_info[self.PROTOCOL]["decrypt_mapping"].pop(media_item.url)

        chunk_decryptor = MegaChunker(crypto.key, crypto.iv, crypto.meta_mac)

        async with aio.open(media_item.temp_file, mode="ab") as f:
            for _, chunk_size in get_chunks(file_size):
                raw_chunk = await content.readexactly(chunk_size)
                chunk = chunk_decryptor.read(raw_chunk)
                await check_free_space()
                chunk_size = len(chunk)

                await self.dl_manager.speed_limiter.acquire(chunk_size)
                await f.write(chunk)
                self.manager.tui.downloads.advance_file(media_item.task_id, chunk_size)
                check_download_speed()

        await self._check_file_duration(media_item)
        chunk_decryptor.check_integrity()
