from __future__ import annotations

import asyncio
import dataclasses
import logging
from typing import TYPE_CHECKING, Self

from myjdapi import myjdapi

from cyberdrop_dl.exceptions import JDownloaderError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pathlib import Path

    from myjdapi.myjdapi import Jddevice

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL
    from cyberdrop_dl.managers.manager import Manager


@dataclasses.dataclass(frozen=True, slots=True)
class Config:
    enabled: bool
    username: str
    password: str
    device: str
    download_dir: Path
    autostart: bool
    whitelist: tuple[str, ...]

    @staticmethod
    def from_manager(manager: Manager) -> Config:
        download_dir = manager.config.runtime_options.jdownloader_download_dir or manager.path_manager.download_folder
        return Config(
            enabled=manager.config.runtime_options.send_unsupported_to_jdownloader,
            device=manager.auth_config.jdownloader.device,
            username=manager.auth_config.jdownloader.username,
            password=manager.auth_config.jdownloader.password,
            download_dir=download_dir.resolve(),
            autostart=manager.config.runtime_options.jdownloader_autostart,
            whitelist=tuple(manager.config.runtime_options.jdownloader_whitelist),
        )


@dataclasses.dataclass(slots=True)
class JDownloader:
    """Class that handles connecting and sending links to JDownloader."""

    config: Config
    _enabled: bool = dataclasses.field(init=False)
    _device: Jddevice | None = dataclasses.field(default=None, init=False)

    @classmethod
    def from_manager(cls, manager: Manager, /) -> Self:
        return cls(Config.from_manager(manager))

    @property
    def enabled(self) -> bool:
        return self._enabled

    def __post_init__(self) -> None:
        self._enabled = self.config.enabled

    def is_enabled_for(self, url: AbsoluteHttpURL) -> bool:
        if not self.enabled:
            return False
        if not self.config.whitelist:
            return True

        return any(domain in url.host for domain in self.config.whitelist)

    async def _connect(self) -> None:
        if not all((self.config.username, self.config.password, self.config.device)):
            raise JDownloaderError("JDownloader credentials were not provided.")

        api = myjdapi.Myjdapi()
        api.set_app_key("CYBERDROP-DL")
        _ = await asyncio.to_thread(api.connect, self.config.username, self.config.password)
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

    async def send(self, url: AbsoluteHttpURL, title: str, download_path: Path | None = None) -> None:
        """Sends links to JDownloader."""

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
        except myjdapi.MYJDException as e:
            raise JDownloaderError(str(e)) from e
