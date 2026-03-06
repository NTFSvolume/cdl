from __future__ import annotations

from typing import ClassVar

from cyberdrop_dl.crawlers._tubecorporate import TubeCorporateCrawler
from cyberdrop_dl.data_structures import AbsoluteHttpURL


class VoyeurHitCrawler(TubeCorporateCrawler):
    DOMAIN: ClassVar[str] = "voyeurhit"
    FOLDER_DOMAIN: ClassVar[str] = "VoyeurHit"
    PRIMARY_URL: ClassVar[AbsoluteHttpURL] = AbsoluteHttpURL("https://voyeurhit.com")
