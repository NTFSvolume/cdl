# ruff: noqa: RUF012
import dataclasses
import logging
import random
import re
from datetime import date, datetime, timedelta
from enum import auto
from functools import cached_property
from pathlib import Path
from typing import Annotated, Literal

import aiohttp
from cyclopts import validators
from cyclopts.group import Group
from cyclopts.parameter import Parameter
from pydantic import (
    ByteSize,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    PrivateAttr,
    field_validator,
)

from cyberdrop_dl.compat import CIStrEnum
from cyberdrop_dl.constants import Browser, HashAlgorithm, Hashing
from cyberdrop_dl.models import AliasModel, AppriseURL, SettingsGroup
from cyberdrop_dl.models.types import (
    ByteSizeSerilized,
    HttpURL,
    ListNonEmptyStr,
    ListNonNegativeInt,
    ListPydanticURL,
    LogPath,
    MainLogPath,
    NonEmptyStr,
    NonEmptyStrOrNone,
    PathOrNone,
)
from cyberdrop_dl.models.validators import falsy_as_none, to_bytesize, to_timedelta

_DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
_DEFAULT_APP_STORAGE = Path("./AppData")
_DEFAULT_DOWNLOAD_STORAGE = Path("./cdl_downloads")
_DEFAULT_REQUIRED_FREE_SPACE = to_bytesize("5GB")
_DEFAULT_CHUNK_SIZE = to_bytesize("10MB")
_MIN_REQUIRED_FREE_SPACE = to_bytesize("512MB")
_LOGS_DATETIME_FORMAT = "%Y%m%d_%H%M%S"
_LOGS_DATE_FORMAT = "%Y_%m_%d"
_SORTING_COMMON_FIELDS = {
    "base_dir",
    "ext",
    "file_date",
    "file_date_iso",
    "file_date_us",
    "filename",
    "parent_dir",
    "sort_dir",
}


class _FormatValidator:
    @classmethod
    def _validate_format(cls, value: str, valid_keys: set[str]) -> None:
        from cyberdrop_dl.utils.strings import validate_format_string

        validate_format_string(value, valid_keys)


@Parameter(name="*")
class Cookies(
    SettingsGroup,
    group=Group(
        "Cookies (choose one)",
        default_parameter=Parameter(negative=""),
        validator=validators.mutually_exclusive,
    ),
):
    cookies: Path | None = None
    "A Netscape formatted file to read cookies from"
    cookies_from: Browser | None = None
    "Automatically extract cookies from this browser"


@Parameter(name="*")
class Dedupe(SettingsGroup):
    auto_dedupe: bool = True
    "Delete duplicate files after downloads"

    hashes: tuple[HashAlgorithm, ...] = (HashAlgorithm.xxh128,)
    "List of hash algorithms to compute and use to compare duplicates"

    hashing: Hashing = Hashing.IN_PLACE
    """OFF: do not compute any hash,
    IN_PLACE: compute hash immediately after each download,
    POST_DOWNLOAD: compute all hashes concurrently after all downloads finish"""

    delete_to_trash: bool = True
    "Deduped files are sent to the trash bin instead of being permanently deleted"

    @field_validator("hashes", mode="after")
    @classmethod
    def unique_list(cls, hashes: list[HashAlgorithm]) -> tuple[HashAlgorithm, ...]:
        extras = sorted(set(hashes).difference({HashAlgorithm.xxh128}))
        return HashAlgorithm.xxh128, *extras


@Parameter(name="*")
class Downloads(SettingsGroup, _FormatValidator):
    sub_folders: bool = True
    "Allow creating nested subfolders while downloading"

    mtime: bool = True
    "Set file upload date as its modification time "

    include_album_id: bool = False
    "Include the album ID (random alphanumeric string) of the album in its folder name"

    include_thread_id: bool = False
    "Include the thread ID of the forum in its folder name"

    include_domain: bool = True
    "Include the domain of website of each download in its folder name"

    max_children: ListNonNegativeInt = []

    separate_posts_format: NonEmptyStr = "{default}"
    "fstring format for the directory created when using --separate-posts"
    separate_posts: bool = False
    "Create a subfolder for each post on sites that have the concept of 'posts'. ex: forums, tiktok, coomer"

    skip_download: bool = False
    "Do not download any actual files"

    mark_completed: bool = False
    "Mark skipped files as completed on the database"

    max_thread_depth: NonNegativeInt = 0
    "Restricts how many levels deep the scraper is allowed to go while crawling a thread"

    max_thread_folder_depth: NonNegativeInt | None = None
    "Restricts the max number of nested folders CDL will create when maximum_thread_depth is greater that 0"

    @field_validator("separate_posts_format", mode="after")
    @classmethod
    def valid_format(cls, value: str) -> str:
        valid_keys = {"default", "title", "id", "number", "date"}
        cls._validate_format(value, valid_keys)
        return value


@Parameter(name="*")
class Filesystem(SettingsGroup):
    download_folder: Annotated[Path, Parameter(alias=("--output", "-o", "-d"))] = _DEFAULT_DOWNLOAD_STORAGE

    dump_json: Annotated[bool, Parameter(alias="-j")] = False
    "Create a json lines files with the information about every processed file (skipped, failed or downloaded)"

    write_pages: bool = False
    "Save to disk a copy of every request as an html file (pages) or json (API requests)"


class Logs(SettingsGroup):
    download_errors: LogPath = Path("download_errors.csv")
    main_log: MainLogPath = Path("cdl.log")
    scrape_errors: LogPath = Path("scrape_errors.csv")
    unsupported: LogPath = Path("unsupported_URLs.csv")

    folder: Path = _DEFAULT_APP_STORAGE / "logs"
    expire_after: timedelta | None = None

    rotate: bool = False
    webhook: Annotated[AppriseURL | None, Parameter(n_tokens=1, accepts_keys=False)] = None
    """The URL of a webhook that you want to send download stats to (Ex: Discord).
    You can add the optional tag attach_logs= as a prefix to include a copy of the main log as an attachment"""

    _created_at: datetime = PrivateAttr(default_factory=datetime.now)

    @property
    def jsonl_file(self) -> Path:
        return self.main_log.with_suffix(".results.jsonl")

    @field_validator("webhook", mode="before")
    @classmethod
    def handle_falsy(cls, value: str) -> str | None:
        return falsy_as_none(value)

    @field_validator("expire_after", mode="before")
    @staticmethod
    def parse_logs_duration(input_date: timedelta | str | int | None) -> timedelta | str | None:
        if value := falsy_as_none(input_date):
            return to_timedelta(value)

    def model_post_init(self, *_) -> None:
        self._resolve_filenames()

    def _resolve_filenames(self) -> None:
        object.__setattr__(self, "log_folder", self.folder.expanduser().resolve().absolute())
        now_file_iso: str = self._created_at.strftime(_LOGS_DATETIME_FORMAT)
        now_folder_iso: str = self._created_at.strftime(_LOGS_DATE_FORMAT)
        for name, log_file in vars(self).items():
            if name == "log_folder" or not isinstance(log_file, Path) or log_file.suffix not in (".csv", ".log"):
                continue

            log_file = self.folder / log_file

            if self.rotate:
                file_name = f"{log_file.stem}_{now_file_iso}{log_file.suffix}"
                log_file = log_file.parent / now_folder_iso / file_name

            object.__setattr__(self, name, log_file)

    def delete_old_logs_and_folders(self) -> None:
        if not self.expire_after:
            return

        for file in self.folder.rglob("*"):
            if file.suffix.lower() not in (".log", ".csv"):
                continue

            if (self._created_at - datetime.fromtimestamp(file.stat().st_ctime)) > self.expire_after:
                file.unlink()


@dataclasses.dataclass(slots=True)
class Range:
    min: float
    max: float

    def __post_init__(self) -> None:
        if not self.max:
            self.max = float("inf")

    def __contains__(self, value: float, /) -> bool:
        return self.min <= value <= self.max


@dataclasses.dataclass(slots=True, frozen=True)
class FileSizeRanges:
    video: Range
    image: Range
    other: Range


@Parameter(name="*")
class FileSizeLimits(SettingsGroup):
    max_image_size: ByteSizeSerilized = ByteSize(0)
    max_other_size: ByteSizeSerilized = ByteSize(0)
    max_video_size: ByteSizeSerilized = ByteSize(0)
    min_image_size: ByteSizeSerilized = ByteSize(0)
    min_other_size: ByteSizeSerilized = ByteSize(0)
    min_video_size: ByteSizeSerilized = ByteSize(0)

    @cached_property
    def ranges(self) -> FileSizeRanges:
        return FileSizeRanges(
            video=Range(
                self.min_video_size,
                self.max_video_size,
            ),
            image=Range(
                self.min_image_size,
                self.max_image_size,
            ),
            other=Range(
                self.min_other_size,
                self.max_other_size,
            ),
        )


@dataclasses.dataclass(slots=True, frozen=True)
class MediaDurationRanges:
    video: Range
    audio: Range


@Parameter(name="*")
class MediaDurationLimits(SettingsGroup):
    max_video_duration: timedelta = timedelta(seconds=0)
    max_audio_duration: timedelta = timedelta(seconds=0)
    min_video_duration: timedelta = timedelta(seconds=0)
    min_audio_duration: timedelta = timedelta(seconds=0)

    @cached_property
    def ranges(self) -> MediaDurationRanges:
        return MediaDurationRanges(
            video=Range(
                self.min_video_duration.total_seconds(),
                self.max_video_duration.total_seconds(),
            ),
            audio=Range(
                self.min_audio_duration.total_seconds(),
                self.max_audio_duration.total_seconds(),
            ),
        )

    @field_validator("*", mode="before")
    @staticmethod
    def parse_runtime_duration(input_date: timedelta | str | int | None) -> timedelta | str:
        return to_timedelta(input_date)


@Parameter(name="*")
class Ignore(SettingsGroup):
    exclude_after: date | None = None
    "Do not download files uploaded after this date"
    exclude_before: date | None = None
    "Do not download files uploaded before this date"

    exclude_files_with_no_extension: bool = True
    """Do not download files without an extension. If disabled, files without an extension are asummed to be .mp4 files
    That means any config option that applies to videos also applies to them"""
    exclude_audio: bool = False
    "Do not download audio files"
    exclude_images: bool = False
    "Do not download images"
    exclude_other: bool = False
    "Do not download non media files"
    exclude_videos: bool = False
    "Do not download videos"

    filename_regex_filter: NonEmptyStrOrNone = None
    "Any download with a filename that matches this regex expression will be skipped"
    ignore_coomer_ads: bool = False
    "Skip posts with the tag #ad on Kemono like sites (Nekohouse, Kemono and Coomer)"
    ignore_coomer_post_content: bool = True
    "Ignore URL found inside the text of posts on Kemono like sites (Nekohouse, Kemono and Coomer)."
    only_hosts: ListNonEmptyStr = []
    "Only scrape/download from sites which host partially matches any of these hosts"
    skip_hosts: ListNonEmptyStr = []
    "Do not scrape/download from sites which host partially matches any of these hosts"

    @field_validator("filename_regex_filter")
    @classmethod
    def is_valid_regex(cls, value: str | None) -> str | None:
        if not value:
            return None
        try:
            _ = re.compile(value)
        except re.error as e:
            raise ValueError("input is not a valid regex") from e
        return value


class Jdownloader(SettingsGroup):
    enabled: Annotated[bool, Parameter(name="--jdownloader")] = False
    "Send unsupported URLs to jdownloader"
    autostart: bool = False
    "Automatically start downloads of URLs send to jdownloader"
    download_dir: PathOrNone = None
    """The base download_dir jdownloader will use for download.
    A null value (the default) will use the cdl's config download_dir"""
    whitelist: ListNonEmptyStr = []
    """List of domain names. An unsupported URL will only be sent to jdownloader if its host is found on the list.
    An empty whitelist (the default) will send any unsupported URL to jdownloader."""


@Parameter(name="*")
class Runtime(SettingsGroup):
    log_level: NonNegativeInt = logging.DEBUG
    "Defines the logging level for messages, according to Python logging levels"
    console_log_level: NonNegativeInt = 100
    "Same as log_level but it controls which messages are shown on the console."
    deep_scrape: bool = False
    ignore_history: bool = False
    "Ignore database history, allowing downloads of previouly downloaded files"
    delete_empty_folders: bool = True
    delete_partial_files: bool = False
    slow_download_speed: ByteSizeSerilized = ByteSize(0)
    "Downloads with a speed lower than this value for more than 10 seconds will be skipped. Set to 0 to disable"


class Sorting(SettingsGroup, _FormatValidator):
    enabled: Annotated[bool, Parameter(name="--sort-downloads")] = False
    output: Path = Path("cdl_sorted_downloads")
    "This is the path to the folder you'd like sorted downloads to be stored in"
    incrementer_format: NonEmptyStr = " ({i})"
    audio_format: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Audio/{filename}{ext}"
    image_format: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Images/{filename}{ext}"
    other_format: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Other/{filename}{ext}"
    video_format: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Videos/{filename}{ext}"

    @field_validator("incrementer_format", mode="after")
    @classmethod
    def valid_sort_incrementer_format(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = {"i"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("audio_format", mode="after")
    @classmethod
    def valid_sorted_audio(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("image_format", mode="after")
    @classmethod
    def valid_sorted_image(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"height", "resolution", "width"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("other_format", mode="after")
    @classmethod
    def valid_sorted_other(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("video_format", mode="after")
    @classmethod
    def valid_sorted_video(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {
                "codec",
                "duration",
                "fps",
                "height",
                "length",
                "resolution",
                "width",
            }
            cls._validate_format(value, valid_keys)
        return value


@Parameter(name="*")
class General(SettingsGroup):
    ssl_context: Literal["truststore", "certifi", "truststore+certifi"] | None = "truststore+certifi"
    disable_crawlers: ListNonEmptyStr = []
    "List of crawlers to disable for the current run"
    flaresolverr: HttpURL | None = None
    "HTTP URL of a flaresolverr instance to bypass Cloudflare and DDoS-Guard protection. Ex: http://192.168.1.44:4000"
    max_file_name_length: PositiveInt = 95
    "Maximum number of characters a filename should have. Longer filenames will be truncated"
    max_folder_name_length: PositiveInt = 60
    "Maximum number of characters a folder should have. Longer filenames will be truncated"
    proxy: HttpURL | None = None
    required_free_space: ByteSizeSerilized = _DEFAULT_REQUIRED_FREE_SPACE
    f"""This is the minimum amount of free space require to start new downloads.
    Values lower that {_MIN_REQUIRED_FREE_SPACE.human_readable()} will be overriden to {_MIN_REQUIRED_FREE_SPACE.human_readable()}"""
    user_agent: Annotated[NonEmptyStr, Parameter(alias="--ua")] = _DEFAULT_USER_AGENT

    @field_validator("ssl_context", mode="before")
    @classmethod
    def ssl(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            value = value.lower().strip()
        return falsy_as_none(value)

    @field_validator("disable_crawlers", mode="after")
    @classmethod
    def unique_list(cls, value: list[str]) -> list[str]:
        return sorted(set(value))

    @field_validator("flaresolverr", "proxy", mode="before")
    @classmethod
    def falsy_urls(cls, value: str) -> str | None:
        return falsy_as_none(value)

    @field_validator("required_free_space", mode="after")
    @classmethod
    def override_min(cls, value: ByteSize) -> ByteSize:
        return max(value, _MIN_REQUIRED_FREE_SPACE)


@Parameter(name="*")
class DownloadLimits(AliasModel):
    retries: Annotated[PositiveInt, Parameter(alias=("-R"))] = 2
    "The number of download attempts per file. Some conditions are never retried (such as a 404 HTTP status)"
    delay: NonNegativeFloat = 0.0
    "Number of seconds to wait between downloads to the same domain."
    max_speed: ByteSizeSerilized = ByteSize(0)
    "Throttle downloads to make sure the combined speed does not exceed this rate (in MB/s). Set to 0 to disable"


@Parameter(name="*")
class RateLimiting(SettingsGroup):
    downloads: DownloadLimits = DownloadLimits()
    jitter: NonNegativeFloat = 0
    "Before downloads, wait an additional random number of seconds in between 0 and <jitter>"
    max_downloads_per_domain: PositiveInt = 5
    max_downloads: PositiveInt = 15
    rate_limit: PositiveFloat = 25
    "Global reate limit. Maximum number of requests (per second) to any site"

    connection_timeout: PositiveFloat = 15
    "Number of seconds to wait while connecting to a website before timing out"
    read_timeout: PositiveFloat | None = 300
    """Number of seconds to wait while reading data from a website before timing out.
    A null value will make CDL keep the socket connection open indefinitely, even if the server is not sending data anymore"""

    @field_validator("read_timeout", mode="before")
    @classmethod
    def parse_timeouts(cls, value: object) -> object | None:
        return falsy_as_none(value)

    @property
    def curl_timeout(self) -> tuple[float, float]:
        return self.connection_timeout, self.read_timeout or 0

    @property
    def aiohttp_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=None,
            sock_connect=self.connection_timeout,
            sock_read=self.read_timeout,
        )

    @property
    def chunk_size(self) -> int:
        if not self.downloads.max_speed:
            return _DEFAULT_CHUNK_SIZE
        return min(_DEFAULT_CHUNK_SIZE, self.downloads.max_speed)

    @property
    def total_download_delay(self) -> NonNegativeFloat:
        """download_delay + jitter"""
        return self.downloads.delay + self.get_jitter()

    def get_jitter(self) -> NonNegativeFloat:
        """Get a random number in the range [0, self.jitter]"""
        return random.uniform(0, self.jitter)


class UIMode(CIStrEnum):
    DISABLED = auto()
    ACTIVITY = auto()
    SIMPLE = auto()
    FULLSCREEN = auto()


class UIOptions(SettingsGroup, group="UI"):
    refresh_rate: Annotated[PositiveInt, Parameter(name="--refresh-rate")] = 10
    mode: UIMode = UIMode.FULLSCREEN
    portrait: Annotated[bool, Parameter(name="--portrait", negative_bool=[])] = False
    "Force a portrait layout for the UI (default is to auto rotate)"


@Parameter(name="*")
class GenericCrawlers(SettingsGroup):
    wordpress_media: ListPydanticURL = []
    wordpress_html: ListPydanticURL = []
    discourse: ListPydanticURL = []
    chevereto: ListPydanticURL = []


@Parameter(name="*")
class ConfigSettings(AliasModel):
    cookies: Cookies = Cookies()
    dedupe: Dedupe = Dedupe()
    download: Downloads = Downloads()
    file_size_limits: FileSizeLimits = FileSizeLimits()
    filesystem: Filesystem = Filesystem()
    general: General = General()
    generic_crawlers: GenericCrawlers = GenericCrawlers()
    ignore: Ignore = Ignore()
    jdownloader: Jdownloader = Jdownloader()
    logs: Logs = Logs()
    media_duration_limits: MediaDurationLimits = MediaDurationLimits()
    rate_limits: RateLimiting = RateLimiting()
    runtime: Runtime = Runtime()
    sort: Sorting = Sorting()
    ui: UIOptions = UIOptions()
