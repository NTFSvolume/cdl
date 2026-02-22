# ruff: noqa: RUF012
import itertools
import re
from datetime import date, datetime, timedelta
from logging import DEBUG
from pathlib import Path

from pydantic import ByteSize, Field, NonNegativeInt, field_validator

from cyberdrop_dl import constants
from cyberdrop_dl.constants import BROWSERS, DEFAULT_APP_STORAGE, DEFAULT_DOWNLOAD_STORAGE, Hashing
from cyberdrop_dl.models import HttpAppriseURL
from cyberdrop_dl.models.types import (
    ByteSizeSerilized,
    ListNonEmptyStr,
    ListNonNegativeInt,
    LogPath,
    MainLogPath,
    NonEmptyStr,
    NonEmptyStrOrNone,
    PathOrNone,
)
from cyberdrop_dl.models.validators import falsy_as, to_timedelta
from cyberdrop_dl.utils.strings import validate_format_string
from cyberdrop_dl.utils.utilities import purge_dir_tree

from ._common import ConfigFile, Settings

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


class DownloadOptions(Settings):
    block_download_sub_folders: bool = False
    disable_download_attempt_limit: bool = False
    disable_file_timestamps: bool = False
    include_album_id_in_folder_name: bool = False
    include_thread_id_in_folder_name: bool = False
    maximum_number_of_children: ListNonNegativeInt = []
    remove_domains_from_folder_names: bool = False
    remove_generated_id_from_filenames: bool = False
    scrape_single_forum_post: bool = False
    separate_posts_format: NonEmptyStr = "{default}"
    separate_posts: bool = False
    skip_download_mark_completed: bool = False
    maximum_thread_depth: NonNegativeInt = 0
    maximum_thread_folder_depth: NonNegativeInt | None = None

    @field_validator("separate_posts_format", mode="after")
    @classmethod
    def valid_format(cls, value: str) -> str:
        valid_keys = {"default", "title", "id", "number", "date"}
        validate_format_string(value, valid_keys)
        return value


class Files(Settings):
    download_folder: Path = Field(default=DEFAULT_DOWNLOAD_STORAGE, validation_alias="d")
    dump_json: bool = Field(default=False, validation_alias="j")
    input_file: Path = Field(default=DEFAULT_APP_STORAGE / "Configs/{config}/URLs.txt", validation_alias="i")
    save_pages_html: bool = False


class Logs(Settings):
    download_error_urls: LogPath = Path("Download_Error_URLs.csv")
    last_forum_post: LogPath = Path("Last_Scraped_Forum_Posts.csv")
    log_folder: Path = DEFAULT_APP_STORAGE / "Configs/{config}/Logs"
    logs_expire_after: timedelta | None = None
    main_log: MainLogPath = Path("downloader.log")
    rotate_logs: bool = False
    scrape_error_urls: LogPath = Path("Scrape_Error_URLs.csv")
    unsupported_urls: LogPath = Path("Unsupported_URLs.csv")
    webhook: HttpAppriseURL | None = None

    @field_validator("webhook", mode="before")
    @classmethod
    def handle_falsy(cls, value: str) -> str | None:
        return falsy_as(value, None)

    @field_validator("logs_expire_after", mode="before")
    @staticmethod
    def parse_logs_duration(input_date: timedelta | str | int | None) -> timedelta | str | None:
        if value := falsy_as(input_date, None):
            return to_timedelta(value)

    def _set_output_filenames(self, now: datetime) -> None:
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

    def _delete_old_logs_and_folders(self, now: datetime | None = None) -> None:
        if now and self.logs_expire_after:
            for file in itertools.chain(self.log_folder.rglob("*.log"), self.log_folder.rglob("*.csv")):
                file_date = file.stat().st_ctime
                t_delta = now - datetime.fromtimestamp(file_date)
                if t_delta > self.logs_expire_after:
                    file.unlink(missing_ok=True)
        purge_dir_tree(self.log_folder)


class FileSizeLimits(Settings):
    maximum_image_size: ByteSizeSerilized = ByteSize(0)
    maximum_other_size: ByteSizeSerilized = ByteSize(0)
    maximum_video_size: ByteSizeSerilized = ByteSize(0)
    minimum_image_size: ByteSizeSerilized = ByteSize(0)
    minimum_other_size: ByteSizeSerilized = ByteSize(0)
    minimum_video_size: ByteSizeSerilized = ByteSize(0)


class MediaDurationLimits(Settings):
    maximum_video_duration: timedelta = timedelta(seconds=0)
    maximum_audio_duration: timedelta = timedelta(seconds=0)
    minimum_video_duration: timedelta = timedelta(seconds=0)
    minimum_audio_duration: timedelta = timedelta(seconds=0)

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


class IgnoreOptions(Settings):
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


class Runtime(Settings):
    console_log_level: NonNegativeInt = 100
    deep_scrape: bool = False
    delete_partial_files: bool = False
    ignore_history: bool = False
    jdownloader_autostart: bool = False
    jdownloader_download_dir: PathOrNone = None
    jdownloader_whitelist: ListNonEmptyStr = []
    log_level: NonNegativeInt = DEBUG
    send_unsupported_to_jdownloader: bool = False
    skip_check_for_empty_folders: bool = False
    skip_check_for_partial_files: bool = False
    slow_download_speed: ByteSizeSerilized = ByteSize(0)
    update_last_forum_post: bool = True


class Sorting(Settings):
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
            validate_format_string(value, valid_keys)
        return value

    @field_validator("sorted_audio", mode="after")
    @classmethod
    def valid_sorted_audio(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            validate_format_string(value, valid_keys)
        return value

    @field_validator("sorted_image", mode="after")
    @classmethod
    def valid_sorted_image(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"height", "resolution", "width"}
            validate_format_string(value, valid_keys)
        return value

    @field_validator("sorted_other", mode="after")
    @classmethod
    def valid_sorted_other(cls, value: str | None) -> str | None:
        if value is not None:
            valid_keys = _SORTING_COMMON_FIELDS | {"bitrate", "duration", "length", "sample_rate"}
            validate_format_string(value, valid_keys)
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
            validate_format_string(value, valid_keys)
        return value


class Cookies(Settings):
    auto_import: bool = False
    browser: BROWSERS | None = BROWSERS.firefox

    def model_post_init(self, *_) -> None:
        if self.auto_import and not self.browser:
            raise ValueError("You need to provide a browser for auto_import to work")


class Dedupe(Settings):
    add_md5_hash: bool = False
    add_sha256_hash: bool = False
    auto_dedupe: bool = True
    hashing: Hashing = Hashing.IN_PLACE
    send_deleted_to_trash: bool = True


class ConfigSettings(ConfigFile):
    browser_cookies: Cookies = Cookies()
    download_options: DownloadOptions = DownloadOptions()
    dupe_cleanup_options: Dedupe = Dedupe()
    file_size_limits: FileSizeLimits = FileSizeLimits()
    media_duration_limits: MediaDurationLimits = MediaDurationLimits()
    files: Files = Files()
    ignore_options: IgnoreOptions = IgnoreOptions()
    logs: Logs = Logs()
    runtime_options: Runtime = Runtime()
    sorting: Sorting = Sorting()
