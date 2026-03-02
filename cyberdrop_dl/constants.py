from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import auto
from typing import Literal

from rich.text import Text

from cyberdrop_dl import env
from cyberdrop_dl.compat import CIStrEnum, StrEnum

# TIME
STARTUP_TIME = datetime.now()
STARTUP_TIME_UTC = STARTUP_TIME.astimezone(UTC)


# logging
LOG_OUTPUT_TEXT = Text("")


# regex
REGEX_LINKS = re.compile(r"(?:http.*?)(?=($|\n|\r\n|\r|\s|\"|\[/URL]|']\[|]\[|\[/img]))")
HTTP_REGEX_LINKS = re.compile(
    r"https?://(www\.)?[-a-zA-Z0-9@:%._+~#=]{2,256}\.[a-z]{2,12}\b([-a-zA-Z0-9@:%_+.~#?&/=]*)"
)


class BlockedDomains:
    partial_match = frozenset(
        (
            "allmylinks.com",
            "amazon.com",
            "beacons.ai",
            "beacons.page",
            "facebook",
            "fbcdn",
            "gfycat",
            "instagram",
            "ko-fi.com",
            "linktr.ee",
            "paypal.me",
            "throne.com",
            "youtu.be",
            "youtube.com",
        )
    )

    exact_match = frozenset()

    if not env.ENABLE_TWITTER:
        partial_match = partial_match.union("twitter.com", ".x.com")
        exact_match = exact_match.union("x.com")


class Hashing(CIStrEnum):
    OFF = auto()
    IN_PLACE = auto()
    POST_DOWNLOAD = auto()


class HashAlgorithm(CIStrEnum):
    md5 = auto()
    xxh128 = auto()
    sha256 = auto()


Browser = Literal[
    "chrome",
    "firefox",
    "safari",
    "edge",
    "opera",
    "brave",
    "librewolf",
    "opera_gx",
    "vivaldi",
    "chromium",
]


class TempExt(StrEnum):
    HLS = ".cdl_hls"
    WRONG_CDL_HLS = ".cdl_hsl"  # used for a while in old versions, has a typo
    PART = ".part"


class FileExt:
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
    SEVEN_Z = frozenset(
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
