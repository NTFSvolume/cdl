from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import TYPE_CHECKING, Self

from myjdapi import myjdapi

from cyberdrop_dl.exceptions import JDownloaderError

if TYPE_CHECKING:
    from pathlib import Path

    from myjdapi.myjdapi import Jddevice

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL


logger = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True)
class JDownloaderConfig:
    enabled: bool
    username: str
    password: str
    device: str
    download_dir: Path
    autostart: bool

    @staticmethod
    def from_config(config: Config) -> JDownloaderConfig:
        download_dir = config.runtime.jdownloader_download_dir or config.files.download_folder
        return JDownloaderConfig(
            enabled=config.runtime.send_unsupported_to_jdownloader,
            device=config.auth.jdownloader.device,
            username=config.auth.jdownloader.username,
            password=config.auth.jdownloader.password,
            download_dir=download_dir.resolve(),
            autostart=config.runtime.jdownloader_autostart,
        )


@dataclasses.dataclass(slots=True)
class JDownloader:
    """Class that handles connecting and sending links to JDownloader."""

    config: JDownloaderConfig
    _enabled: bool = dataclasses.field(init=False)
    _device: Jddevice | None = dataclasses.field(default=None, init=False)

    @classmethod
    def new(cls, options: Config | JDownloaderConfig, /) -> Self:
        if not isinstance(options, JDownloaderConfig):
            options = JDownloaderConfig.from_config(options)
        return cls(options)

    def __post_init__(self):
        self._enabled = self.config.enabled

    async def _connect(self) -> None:
        if not all((self.config.username, self.config.password, self.config.device)):
            raise JDownloaderError("JDownloader credentials were not provided.")

        api = myjdapi.Myjdapi()
        api.set_app_key("CYBERDROP-DL")
        await asyncio.to_thread(api.connect, self.config.username, self.config.password)
        self._device = api.get_device(self.config.device)

    async def connect(self) -> None:
        if not self._enabled or self._device is not None:
            return
        try:
            return await self._connect()
        except JDownloaderError as e:
            msg = e.message
        except myjdapi.MYJDDeviceNotFoundException:
            msg = f"Device not found ({self.config.device})"
        except myjdapi.MYJDApiException as e:
            msg = e

        logger.error(f"Failed to connect to jDownloader: {msg}")
        self._enabled = False

    async def send(
        self,
        url: AbsoluteHttpURL,
        title: str,
        download_path: Path | None = None,
    ) -> None:
        assert self._device is not None
        try:
            download_folder = self.config.download_dir
            if download_path:
                download_folder = download_folder / download_path

            await asyncio.to_thread(
                self._device.linkgrabber.add_links,
                [
                    {
                        "autostart": self.config.autostart,
                        "links": str(url),
                        "packageName": title if title else "Cyberdrop-DL",
                        "destinationFolder": str(download_folder),
                        "overwritePackagizerRules": True,
                    },
                ],
            )
        except (AssertionError, myjdapi.MYJDException) as e:
            raise JDownloaderError(str(e)) from e
