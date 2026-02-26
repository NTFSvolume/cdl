# ruff: noqa: RUF012
import dataclasses
import logging
import random
import re
from datetime import date, datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import Literal

import aiohttp
from pydantic import (
    ByteSize,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    field_serializer,
    field_validator,
)

from cyberdrop_dl import constants
from cyberdrop_dl.constants import BROWSERS, DEFAULT_APP_STORAGE, DEFAULT_DOWNLOAD_STORAGE, Hashing
from cyberdrop_dl.models import AppriseURLModel, Settings, SettingsGroup
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
from cyberdrop_dl.models.validators import falsy_as, falsy_as_none, to_bytesize, to_timedelta

MIN_REQUIRED_FREE_SPACE = to_bytesize("512MB")
DEFAULT_REQUIRED_FREE_SPACE = to_bytesize("5GB")

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


class FormatValidator:
    @classmethod
    def _validate_format(cls, value: str, valid_keys: set[str]) -> None:
        from cyberdrop_dl.utils.strings import validate_format_string

        validate_format_string(value, valid_keys)


class Downloads(FormatValidator, SettingsGroup):
    block_download_sub_folders: bool = False
    disable_file_timestamps: bool = False
    include_album_id_in_folder_name: bool = False
    include_thread_id_in_folder_name: bool = False
    max_children: ListNonNegativeInt = []
    remove_domains_from_folder_names: bool = False
    remove_generated_id_from_filenames: bool = False
    scrape_single_forum_post: bool = False
    separate_posts_format: NonEmptyStr = "{default}"
    separate_posts: bool = False
    skip_download_mark_completed: bool = False
    max_thread_depth: NonNegativeInt = 0
    max_thread_folder_depth: NonNegativeInt | None = None

    @field_validator("separate_posts_format", mode="after")
    @classmethod
    def valid_format(cls, value: str) -> str:
        valid_keys = {"default", "title", "id", "number", "date"}
        cls._validate_format(value, valid_keys)
        return value


class Files(SettingsGroup):
    download_folder: Path = Field(default=DEFAULT_DOWNLOAD_STORAGE, validation_alias="d")
    dump_json: bool = Field(default=False, validation_alias="j")
    input_file: Path = Field(default=DEFAULT_APP_STORAGE / "Configs/{config}/URLs.txt", validation_alias="i")
    save_pages_html: bool = False


class Logs(SettingsGroup):
    download_error_urls: LogPath = Path("Download_Error_URLs.csv")
    last_forum_post: LogPath = Path("Last_Scraped_Forum_Posts.csv")
    log_folder: Path = DEFAULT_APP_STORAGE / "Configs/{config}/Logs"
    logs_expire_after: timedelta | None = None
    main_log: MainLogPath = Path("downloader.log")
    rotate_logs: bool = False
    scrape_error_urls: LogPath = Path("Scrape_Error_URLs.csv")
    unsupported_urls: LogPath = Path("Unsupported_URLs.csv")
    webhook: AppriseURLModel | None = None

    @cached_property
    def jsonl_file(self):
        return self.main_log.with_suffix(".results.jsonl")

    @field_validator("webhook", mode="before")
    @classmethod
    def handle_falsy(cls, value: str) -> str | None:
        return falsy_as(value, None)

    @field_validator("logs_expire_after", mode="before")
    @staticmethod
    def parse_logs_duration(input_date: timedelta | str | int | None) -> timedelta | str | None:
        if value := falsy_as(input_date, None):
            return to_timedelta(value)

    def set_output_filenames(self, now: datetime) -> None:
        self.log_folder.mkdir(exist_ok=True, parents=True)
        current_time_file_iso: str = now.strftime(constants.LOGS_DATETIME_FORMAT)
        current_time_folder_iso: str = now.strftime(constants.LOGS_DATE_FORMAT)
        for attr, log_file in vars(self).items():
            if not isinstance(log_file, Path) or log_file.suffix not in (".csv", ".log"):
                continue

            if self.rotate_logs:
                new_name = f"{log_file.stem}_{current_time_file_iso}{log_file.suffix}"
                log_file = log_file.parent / current_time_folder_iso / new_name
                setattr(self, attr, self.log_folder / log_file)

            log_file.parent.mkdir(exist_ok=True, parents=True)

    def delete_old_logs_and_folders(self, now: datetime | None = None) -> None:
        if not (now and self.logs_expire_after):
            return

        from cyberdrop_dl.utils.utilities import purge_dir_tree

        for file in self.log_folder.rglob("*"):
            if file.suffix not in (".log", ".csv"):
                continue

            file_date = file.stat().st_ctime
            t_delta = now - datetime.fromtimestamp(file_date)
            if t_delta > self.logs_expire_after:
                file.unlink(missing_ok=True)

        _ = purge_dir_tree(self.log_folder)


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
        """Parses `datetime.timedelta`, `str` or `int` into a timedelta format.
        for `str`, the expected format is `value unit`, ex: `5 days`, `10 minutes`, `1 year`
        valid units:
            year(s), week(s), day(s), hour(s), minute(s), second(s), millisecond(s), microsecond(s)
        for `int`, value is assumed as `days`
        """
        if input_date is None:
            return timedelta(seconds=0)
        return to_timedelta(input_date)


class Ignore(SettingsGroup):
    exclude_audio: bool = False
    exclude_images: bool = False
    exclude_other: bool = False
    exclude_videos: bool = False
    filename_regex_filter: NonEmptyStrOrNone = None
    ignore_coomer_ads: bool = False
    ignore_coomer_post_content: bool = True
    only_hosts: ListNonEmptyStr = []
    skip_hosts: ListNonEmptyStr = []
    exclude_files_with_no_extension: bool = True
    exclude_before: date | None = None
    exclude_after: date | None = None

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


class Runtime(SettingsGroup):
    console_log_level: NonNegativeInt = 100
    deep_scrape: bool = False
    delete_partial_files: bool = False
    ignore_history: bool = False
    jdownloader_autostart: bool = False
    jdownloader_download_dir: PathOrNone = None
    jdownloader_whitelist: ListNonEmptyStr = []
    log_level: NonNegativeInt = logging.DEBUG
    send_unsupported_to_jdownloader: bool = False
    skip_check_for_empty_folders: bool = False
    skip_check_for_partial_files: bool = False
    slow_download_speed: ByteSizeSerilized = ByteSize(0)
    update_last_forum_post: bool = True


class Sorting(FormatValidator, SettingsGroup):
    scan_folder: PathOrNone = None
    sort_downloads: bool = False
    sort_folder: Path = DEFAULT_DOWNLOAD_STORAGE / "Cyberdrop-DL Sorted Downloads"
    sort_incrementer_format: NonEmptyStr = " ({i})"
    sorted_audio: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Audio/{filename}{ext}"
    sorted_image: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Images/{filename}{ext}"
    sorted_other: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Other/{filename}{ext}"
    sorted_video: NonEmptyStrOrNone = "{sort_dir}/{base_dir}/Videos/{filename}{ext}"

    @field_validator("sort_incrementer_format", mode="after")
    @classmethod
    def valid_sort_incrementer_format(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = {"i"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("sorted_audio", mode="after")
    @classmethod
    def valid_sorted_audio(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("sorted_image", mode="after")
    @classmethod
    def valid_sorted_image(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"height", "resolution", "width"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("sorted_other", mode="after")
    @classmethod
    def valid_sorted_other(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            cls._validate_format(value, valid_keys)
        return value

    @field_validator("sorted_video", mode="after")
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


class Cookies(SettingsGroup):
    auto_import: bool = False
    browser: BROWSERS | None = BROWSERS.firefox

    def model_post_init(self, *_) -> None:
        if self.auto_import and not self.browser:
            raise ValueError("You need to provide a browser for auto_import to work")


class Dedupe(SettingsGroup):
    add_md5_hash: bool = False
    add_sha256_hash: bool = False
    auto_dedupe: bool = True
    hashing: Hashing = Hashing.IN_PLACE
    send_deleted_to_trash: bool = True


# ruff: noqa: RUF012


class General(SettingsGroup):
    ssl_context: Literal["truststore", "certifi", "truststore+certifi"] | None = "truststore+certifi"
    disable_crawlers: ListNonEmptyStr = []
    flaresolverr: HttpURL | None = None
    max_file_name_length: PositiveInt = 95
    max_folder_name_length: PositiveInt = 60
    proxy: HttpURL | None = None
    required_free_space: ByteSizeSerilized = DEFAULT_REQUIRED_FREE_SPACE
    user_agent: NonEmptyStr = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"

    @field_validator("ssl_context", mode="before")
    @classmethod
    def ssl(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            value = value.lower().strip()
        return falsy_as(value, None)

    @field_validator("disable_crawlers", mode="after")
    @classmethod
    def unique_list(cls, value: list[str]) -> list[str]:
        return sorted(set(value))

    @field_serializer("flaresolverr", "proxy")
    def serialize(self, value: str) -> str | None:
        return falsy_as(value, None, str)

    @field_validator("flaresolverr", "proxy", mode="before")
    @classmethod
    def convert_to_str(cls, value: str) -> str | None:
        return falsy_as(value, None, str)

    @field_validator("required_free_space", mode="after")
    @classmethod
    def override_min(cls, value: ByteSize) -> ByteSize:
        return max(value, MIN_REQUIRED_FREE_SPACE)


class RateLimiting(SettingsGroup):
    download_attempts: PositiveInt = 2
    download_delay: NonNegativeFloat = 0.0
    download_speed_limit: ByteSizeSerilized = ByteSize(0)
    jitter: NonNegativeFloat = 0
    max_simultaneous_downloads_per_domain: PositiveInt = 5
    max_simultaneous_downloads: PositiveInt = 15
    rate_limit: PositiveFloat = 25

    connection_timeout: PositiveFloat = 15
    read_timeout: PositiveFloat | None = 300

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
    def total_delay(self) -> NonNegativeFloat:
        """download_delay + jitter"""
        return self.download_delay + self.get_jitter()

    def get_jitter(self) -> NonNegativeFloat:
        """Get a random number in the range [0, self.jitter]"""
        return random.uniform(0, self.jitter)


class UIOptions(SettingsGroup):
    refresh_rate: PositiveInt = 10


class GenericCrawlerInstances(SettingsGroup):
    wordpress_media: ListPydanticURL = []
    wordpress_html: ListPydanticURL = []
    discourse: ListPydanticURL = []
    chevereto: ListPydanticURL = []


class ConfigSettings(Settings):
    browser_cookies: Cookies = Cookies()
    download: Downloads = Downloads()
    dedupe: Dedupe = Dedupe()
    file_size_limits: FileSizeLimits = FileSizeLimits()
    files: Files = Files()
    general: General = General()
    generic_crawlers_instances: GenericCrawlerInstances = GenericCrawlerInstances()
    ignore: Ignore = Ignore()
    logs: Logs = Logs()
    media_duration_limits: MediaDurationLimits = MediaDurationLimits()
    rate_limits: RateLimiting = RateLimiting()
    runtime: Runtime = Runtime()
    sorting: Sorting = Sorting()
    ui_options: UIOptions = UIOptions()
