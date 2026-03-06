from __future__ import annotations

from typing import ClassVar

from cyberdrop_dl.crawlers._one_manager import OneManagerCrawler
from cyberdrop_dl.data_structures import AbsoluteHttpURL


class FlugelAnimeCrawler(OneManagerCrawler):
    DOMAIN: ClassVar[str] = "flugel-anime"
    PRIMARY_URL: ClassVar = AbsoluteHttpURL("https://flugelanime.com")
    FOLDER_DOMAIN: ClassVar = "Flugel-Anime"
