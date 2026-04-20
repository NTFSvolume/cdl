from __future__ import annotations

from typing import ClassVar

from cyberdrop_dl.crawlers._kvs import KernelVideoSharingCrawler
from cyberdrop_dl.url_objects import AbsoluteHttpURL


class XMegaDriveCrawler(KernelVideoSharingCrawler):
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://www.xmegadrive.com")
    DOMAIN: ClassVar[str] = "xmegadrive"
    FOLDER_DOMAIN: ClassVar[str] = "XMegaDrive"
