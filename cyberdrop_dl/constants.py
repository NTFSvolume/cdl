from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import auto
from pathlib import Path
from typing import TYPE_CHECKING, Final

from rich.text import Text

from cyberdrop_dl import env
from cyberdrop_dl.compat import MayBeUpperStrEnum, StrEnum

if TYPE_CHECKING:
    from aiohttp.resolver import AsyncResolver, ThreadedResolver


# TIME
STARTUP_TIME = datetime.now()
STARTUP_TIME_UTC = STARTUP_TIME.astimezone(UTC)
LOGS_DATETIME_FORMAT = "%Y%m%d_%H%M%S"
LOGS_DATE_FORMAT = "%Y_%m_%d"
STARTUP_TIME_STR = STARTUP_TIME.strftime(LOGS_DATETIME_FORMAT)
DNS_RESOLVER: type[AsyncResolver] | type[ThreadedResolver] | None = None
MAX_REDIRECTS: Final[int] = 8


# logging
CONSOLE_LEVEL = 100
MAX_NAME_LENGTHS = {"FILE": 95, "FOLDER": 60}
LOG_OUTPUT_TEXT = Text("")


# regex
RAR_MULTIPART_PATTERN = re.compile(r"^part\d+")
SANITIZE_FILENAME_PATTERN = re.compile(r'[<>:"/\\|?*\']')
REGEX_LINKS = re.compile(r"(?:http.*?)(?=($|\n|\r\n|\r|\s|\"|\[/URL]|']\[|]\[|\[/img]))")
HTTP_REGEX_LINKS = re.compile(
    r"https?://(www\.)?[-a-zA-Z0-9@:%._+~#=]{2,256}\.[a-z]{2,12}\b([-a-zA-Z0-9@:%_+.~#?&/=]*)"
)


class TempExt(StrEnum):
    HLS = ".cdl_hls"
    WRONG_CDL_HLS = ".cdl_hsl"  # used for a while in old versions, has a typo
    PART = ".part"


class BlockedDomains:
    partial_match = (
        "facebook",
        "instagram",
        "fbcdn",
        "gfycat",
        "ko-fi.com",
        "paypal.me",
        "amazon.com",
        "throne.com",
        "youtu.be",
        "youtube.com",
        "linktr.ee",
        "beacons.page",
        "beacons.ai",
        "allmylinks.com",
    )

    exact_match = ()

    if not env.ENABLE_TWITTER:
        partial_match = *partial_match, "twitter.com", ".x.com"
        exact_match = *exact_match, "x.com"


DEFAULT_APP_STORAGE = Path("./AppData")
DEFAULT_DOWNLOAD_STORAGE = Path("./cdl_downloads")
RESERVED_CONFIG_NAMES = ["all", "default"]


class HashType(StrEnum):
    md5 = "md5"
    sha256 = "sha256"
    xxh128 = "xxh128"


class Hashing(MayBeUpperStrEnum):
    OFF = auto()
    IN_PLACE = auto()
    POST_DOWNLOAD = auto()


class BROWSERS(StrEnum):
    chrome = auto()
    firefox = auto()
    safari = auto()
    edge = auto()
    opera = auto()
    brave = auto()
    librewolf = auto()
    opera_gx = auto()
    vivaldi = auto()
    chromium = auto()


class FileFormats:
    IMAGE = frozenset(
        {
            ".gif",
            ".gifv",
            ".heic",
            ".jfif",
            ".jif",
            ".jpe",
            ".jpeg",
            ".jpg",
            ".jxl",
            ".png",
            ".svg",
            ".tif",
            ".tiff",
            ".webp",
        }
    )
    VIDEO = frozenset(
        {
            ".3gp",
            ".avchd",
            ".avi",
            ".f4v",
            ".flv",
            ".m2ts",
            ".m4p",
            ".m4v",
            ".mkv",
            ".mov",
            ".mp2",
            ".mp4",
            ".mpe",
            ".mpeg",
            ".mpg",
            ".mpv",
            ".mts",
            ".ogg",
            ".ogv",
            ".qt",
            ".swf",
            ".ts",
            ".webm",
            ".wmv",
        }
    )
    AUDIO = frozenset(
        {
            ".flac",
            ".m4a",
            ".mka",
            ".mp3",
            ".wav",
        }
    )
    TEXT = frozenset(
        {
            ".htm",
            ".html",
            ".md",
            ".nfo",
            ".txt",
            ".vtt",
            ".sub",
        }
    )
    _7Z = frozenset(
        {
            ".7z",
            ".bz2",
            ".gz",
            ".tar",
            ".zip",
        }
    )
    VIDEO_OR_IMAGE = VIDEO | IMAGE
    MEDIA = AUDIO | VIDEO_OR_IMAGE
