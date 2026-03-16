from __future__ import annotations

import contextlib
import copy
import dataclasses
import datetime
import enum
import logging
import re
from dataclasses import field
from fractions import Fraction
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NamedTuple, Self, overload

import yarl
from typing_extensions import Sentinel

from cyberdrop_dl.exceptions import ScrapeError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Mapping

    from cyberdrop_dl.annotations import copy_signature

    class AbsoluteHttpURL(yarl.URL):
        @copy_signature(yarl.URL.__new__)
        def __new__(cls) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.__truediv__)
        def __truediv__(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.__mod__)
        def __mod__(self) -> AbsoluteHttpURL: ...

        @property
        def host(self) -> str: ...  # pyright: ignore[reportIncompatibleVariableOverride]

        @property
        def scheme(self) -> Literal["http", "https"]: ...  # pyright: ignore[reportIncompatibleVariableOverride]

        @property
        def absolute(self) -> Literal[True]: ...  # pyright: ignore[reportIncompatibleVariableOverride]

        @property
        def parent(self) -> AbsoluteHttpURL: ...  # pyright: ignore[reportIncompatibleVariableOverride]

        @copy_signature(yarl.URL.with_path)
        def with_path(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.with_host)
        def with_host(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.origin)
        def origin(self) -> AbsoluteHttpURL: ...

        @overload
        def with_query(self, query: yarl.Query) -> AbsoluteHttpURL: ...

        @overload
        def with_query(self, **kwargs: yarl.QueryVariable) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.with_query)
        def with_query(self) -> AbsoluteHttpURL: ...

        @overload
        def extend_query(self, query: yarl.Query) -> AbsoluteHttpURL: ...

        @overload
        def extend_query(self, **kwargs: yarl.QueryVariable) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.extend_query)
        def extend_query(self) -> AbsoluteHttpURL: ...

        @overload
        def update_query(self, query: yarl.Query) -> AbsoluteHttpURL: ...

        @overload
        def update_query(self, **kwargs: yarl.QueryVariable) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.update_query)
        def update_query(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.without_query_params)
        def without_query_params(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.with_fragment)
        def with_fragment(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.with_name)
        def with_name(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.with_suffix)
        def with_suffix(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.join)
        def join(self) -> AbsoluteHttpURL: ...

        @copy_signature(yarl.URL.joinpath)
        def joinpath(self) -> AbsoluteHttpURL: ...

else:
    AbsoluteHttpURL = yarl.URL


logger = logging.getLogger(__name__)


_FIELDS_CACHE: dict[type, tuple[str, ...]] = {}
_MISSING = Sentinel("_MISSING")


def _fields(cls: type) -> tuple[str, ...]:
    if fields := _FIELDS_CACHE.get(cls):
        return fields
    fields = _FIELDS_CACHE[cls] = tuple(f.name for f in dataclasses.fields(cls))
    return fields


class _DictDataclass:
    __dataclass_fields__: ClassVar[dict[str, dataclasses.Field[Any]]]

    @classmethod
    def filter_dict(cls, data: Mapping[str, Any], /) -> dict[str, Any]:
        return {k: v for k in _fields(cls) if (v := data.get(k, _MISSING)) is not _MISSING}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], /) -> Self:
        return cls(**cls.filter_dict(data))


class DownloadProtocol(enum.StrEnum):
    HTTP = enum.auto()
    HLS = enum.auto()
    MEGA_NZ = enum.auto()


class ScrapeItemType(enum.Enum):
    FORUM = 0
    FORUM_POST = 1
    FILE_HOST_PROFILE = 2
    FILE_HOST_ALBUM = 3


class HlsSegment(NamedTuple):
    part: str
    name: str
    url: AbsoluteHttpURL


@dataclasses.dataclass(slots=True, kw_only=True)
class Media(_DictDataclass):
    id: int
    domain: str
    db_path: str
    referer: AbsoluteHttpURL
    name: str
    album_id: str | None = None
    size: int | None = None
    duration: float | None = None
    uploaded_at: int | None = None
    upload_date: datetime.datetime | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.uploaded_at:
            assert isinstance(self.uploaded_at, int), f"Invalid {self.uploaded_at =!r} from {self.referer}"
            self.upload_date = datetime.datetime.fromtimestamp(self.uploaded_at, tz=datetime.UTC)


@dataclasses.dataclass(slots=True, kw_only=True)
class Download:
    media: Media
    url: AbsoluteHttpURL
    referer: AbsoluteHttpURL
    folder: Path
    filename: str

    ext: str = ""
    debrid_url: AbsoluteHttpURL | None = None
    hash: str | None = None
    path: Path = field(init=False)
    parents: tuple[AbsoluteHttpURL, ...] = ()

    _attempts: int = field(init=False, default=0)
    is_segment: bool = field(init=False, default=False)
    _headers: dict[str, str] = field(default_factory=dict, compare=False)
    _downloaded: bool = False
    _protocol: DownloadProtocol = DownloadProtocol.HTTP
    metadata: object = field(init=False, default_factory=dict, compare=False)
    extra_info: dict[str, Any] = field(init=False, default_factory=dict, compare=False)

    @property
    def domain(self) -> str:
        return self.media.domain

    def __post_init__(self) -> None:
        self.ext = self.ext or Path(self.filename).suffix
        self.path = self.folder / self.filename

    @property
    def real_url(self) -> AbsoluteHttpURL:
        return self.debrid_url or self.url

    @property
    def temp_file(self) -> Path:
        return self.path.with_suffix(self.path.suffix + ".part")

    @staticmethod
    def from_item(
        media: Media,
        origin: ScrapeItem | Download,
        url: AbsoluteHttpURL,
        /,
        *,
        download_folder: Path,
        filename: str,
        ext: str | None = None,
    ) -> Download:
        return Download(
            media=media,
            url=url,
            folder=download_folder,
            filename=filename,
            referer=origin.url,
            ext=ext or Path(filename).suffix,
            parents=tuple(origin.parents),
        )

    def as_dict(self) -> dict[str, Any]:
        me = dataclasses.asdict(self)
        me["partial_file"] = self.temp_file
        if self.hash:
            me["hash"] = f"xxh128:{self.hash}"
        for name in ("is_segment",):
            _ = me.pop(name)
        return me


@dataclasses.dataclass(kw_only=True, slots=True)
class ScrapeItem:
    url: AbsoluteHttpURL
    parent_title: str = ""
    part_of_album: bool = False
    album_id: str | None = None
    timestamp: int | None = None
    retry_path: Path | None = None

    type: ScrapeItemType | None = field(default=None, init=False)
    completed_at: int | None = None
    created_at: int | None = None
    password: str | None = None

    parents: list[AbsoluteHttpURL] = field(default_factory=list, init=False)
    parent_threads: set[AbsoluteHttpURL] = field(default_factory=set, init=False)
    _children: int = field(default=0, init=False)
    _children_limit: int = field(default=0, init=False)
    _children_limits: list[int] = field(default_factory=list, init=False)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(url={self.url!r}, parent_title={self.parent_title!r}, timestamp={self.timestamp!r}"
        )

    def __post_init__(self) -> None:
        self.password = self.password or self.url.query.get("password")

    @property
    def datetime(self) -> datetime.datetime | None:
        if self.timestamp:
            assert isinstance(self.timestamp, int), f"Invalid {self.timestamp =!r} from {self.url}"
            return datetime.datetime.fromtimestamp(self.timestamp, tz=datetime.UTC)

    def add_to_parent_title(self, title: str) -> None:
        """Adds a title to the parent title."""
        from cyberdrop_dl.utils.filepath import sanitize_folder

        if not title or self.retry_path:
            return

        title = sanitize_folder(title)
        if title.endswith(")") and " (" in title:
            for part in reversed(self.parent_title.split("/")):
                if part.endswith(")") and " (" in part:
                    last_domain_suffix = part.rpartition(" (")[-1]
                    break
            else:
                last_domain_suffix = None

            if last_domain_suffix:
                og_title, _, domain_suffix = title.rpartition(" (")
                if last_domain_suffix == domain_suffix:
                    title = og_title

        self.parent_title = (self.parent_title + "/" + title) if self.parent_title else title

    def reset_childen(self) -> None:
        self._children = self._children_limit = 0
        if self.type is None:
            return
        try:
            self._children_limit = self._children_limits[self.type.value]
        except (IndexError, TypeError):
            pass

    def add_children(self, number: int = 1) -> None:
        self._children += number
        if self._children_limit and self._children >= self._children_limit:
            raise RecursionError("Max number of children reached", self)

    def reset(self, *, reset_parents: bool = False) -> None:
        """Resets `album_id`, `type` and `posible_datetime` back to `None`

        Reset `part_of_album` back to `False`
        """
        self.album_id = self.timestamp = self.type = None
        self.part_of_album = False
        self.reset_childen()
        if reset_parents:
            self.parents = []
            self.parent_threads = set()

    def create_new(
        self,
        url: AbsoluteHttpURL,
        *,
        new_title_part: str | None = None,
        add_parent: AbsoluteHttpURL | bool | None = None,
    ) -> Self:
        """Creates a scrape item."""

        me = self.copy()

        if add_parent:
            parent = self.url if add_parent is True else add_parent
            me.parents.append(parent)

        if new_title_part:
            me.add_to_parent_title(new_title_part)

        me.url = url
        return me

    def create_child(self, url: AbsoluteHttpURL, *, new_title_part: str | None = None) -> Self:
        return self.create_new(url, add_parent=True, new_title_part=new_title_part)

    def setup_as(self, title: str, type: ScrapeItemType, /, album_id: str | None = None) -> None:
        self.part_of_album = True
        if album_id:
            self.album_id = album_id
        if self.type != type:
            self.type = type
            self.reset_childen()
        self.add_to_parent_title(title)

    def setup_as_album(self, title: str, /, album_id: str | None = None) -> None:
        return self.setup_as(title, ScrapeItemType.FILE_HOST_ALBUM, album_id=album_id)

    def setup_as_profile(self, title: str, /, album_id: str | None = None) -> None:
        return self.setup_as(title, ScrapeItemType.FILE_HOST_PROFILE, album_id=album_id)

    def setup_as_forum(self, title: str, /, album_id: str | None = None) -> None:
        return self.setup_as(title, ScrapeItemType.FORUM, album_id=album_id)

    def setup_as_post(self, title: str, /, album_id: str | None = None) -> None:
        return self.setup_as(title, ScrapeItemType.FORUM_POST, album_id=album_id)

    @property
    def origin(self) -> AbsoluteHttpURL | None:
        if self.parents:
            return self.parents[0]

    @property
    def parent(self) -> AbsoluteHttpURL | None:
        if self.parents:
            return self.parents[-1]

    def create_download_path(self, domain: str) -> Path:
        if self.retry_path:
            return self.retry_path
        if self.parent_title:
            if self.part_of_album:
                return Path(self.parent_title)
            return Path(self.parent_title) / f"Loose Files ({domain})"
        return Path(f"Loose Files ({domain})")

    def copy(self) -> Self:
        """Returns a deep copy of this scrape_item"""
        return copy.deepcopy(self)

    @contextlib.contextmanager
    def track_changes(self) -> Generator[None]:
        og_url = self.url
        try:
            yield
        finally:
            if og_url != self.url:
                logger.info(f"URL transformation applied : \n  old_url: {og_url}\n  new_url: {self.url}")


@dataclasses.dataclass(slots=True, order=True)
class DatetimeRange:
    before: datetime.datetime
    after: datetime.datetime

    def __post_init__(self) -> None:
        if self.before <= self.after:
            raise ValueError

    @classmethod
    def from_url(cls, url: AbsoluteHttpURL) -> DatetimeRange | None:
        return cls(
            cls._extract(url.query, "before") or datetime.datetime.max,
            cls._extract(url.query, "after") or datetime.datetime.min,
        )

    def __contains__(self, other: object) -> bool:
        if not isinstance(other, datetime.datetime):
            return False
        return self.before <= other <= self.after

    def as_query(self) -> dict[str, str]:
        query: dict[str, str] = {}
        if self.before != datetime.datetime.max:
            query["before"] = self.before.isoformat()
        if self.after != datetime.datetime.min:
            query["after"] = self.after.isoformat()
        return query

    @staticmethod
    def _extract(query: Mapping[str, str], name: str) -> datetime.datetime | None:
        if value := query.get(name):
            return datetime.datetime.fromisoformat(value).astimezone(datetime.UTC)


_VIDEO_CODECS = "avc1", "avc2", "avc3", "avc4", "av1", "hevc", "hev1", "hev2", "hvc1", "hvc2", "vp8", "vp9", "vp10"
_AUDIO_CODECS = "ac-3", "ec-3", "mp3", "mp4a", "opus", "vorbis"


class Codecs(NamedTuple):
    video: str | None
    audio: str | None

    @staticmethod
    def parse(codecs: str | None) -> Codecs:
        if not codecs:
            return Codecs(None, None)
        video_codec = audio_codec = None

        def match_codec(codec: str, lookup_array: Iterable[str]) -> str | None:
            codec, *_ = codec.split(".")
            clean_codec = codec[:-1].replace("0", "") + codec[-1]
            return next((key for key in lookup_array if clean_codec.startswith(key)), None)

        for codec in codecs.split(","):
            if not video_codec and (found := match_codec(codec, _VIDEO_CODECS)):
                video_codec = found
            elif not audio_codec and (found := match_codec(codec, _AUDIO_CODECS)):
                audio_codec = found
            if video_codec and audio_codec:
                break

        assert video_codec
        if "avc" in video_codec:
            video_codec = "avc1"
        elif "hev" in video_codec or "hvc" in video_codec:
            video_codec = "hevc"
        return Codecs(video_codec, audio_codec)


class Resolution(NamedTuple):
    width: int
    height: int

    @property
    def name(self) -> str:
        if 7600 < self.width < 8200:
            return "8K"
        if 3800 < self.width < 4100:
            return "4K"
        return f"{self.height}p"

    @property
    def aspect_ratio(self) -> Fraction:
        return Fraction(self.width, self.height)

    @staticmethod
    def parse(url_number_or_string: yarl.URL | str | int | None, /) -> Resolution:
        if url_number_or_string is None:
            return UNKNOWN_RESOLUTION

        if isinstance(url_number_or_string, int):
            return Resolution._from_height(url_number_or_string)

        if not isinstance(url_number_or_string, str):
            for resolution in COMMON_RESOLUTIONS:
                if str(resolution.height) in url_number_or_string.parts:
                    return resolution

            url_number_or_string = url_number_or_string.path

        # "1080p", "720i", "480P", the most common case
        if (height := url_number_or_string.rstrip("pPiI")).isdecimal():
            return Resolution._from_height(height)

        # "1920x1080", "1280X720" or "640,480"
        if match := re.search(r"(?P<width>\d+)[xX,](?P<height>\d+)", url_number_or_string):
            return Resolution(
                width=int(match.group("width")),
                height=int(match.group("height")),
            )

        #  "1080p", "720i", "480P" w regex, slower but works with substrings
        if match := re.search(r"(?<![a-zA-Z0-9])(\d+)[pPiI](?![a-zA-Z0-9])", url_number_or_string):
            return Resolution._from_height(match.group(1))

        # "2K", "4K", "8K"
        if match := re.search(r"\b([248])[kK]\b", url_number_or_string):
            height = {"2": 1440, "4": 2160, "8": 4320}[match.group(1)]
            return Resolution._from_height(height)

        raise ValueError(f"Unable to parse resolution from {url_number_or_string}")

    @staticmethod
    def _from_height(height: str | int, aspect_ratio: float = 16 / 9) -> Resolution:
        height = int(height)
        width = round(height * aspect_ratio)
        return Resolution(width, height)

    @staticmethod
    def unknown() -> Resolution:
        return UNKNOWN_RESOLUTION

    @staticmethod
    def highest() -> Resolution:
        return HIGHEST_RESOLUTION

    @staticmethod
    def make_parser() -> Callable[[yarl.URL | str | int | None], Resolution]:
        """Returns a callable wrapper around `Resolution.parse` that can return `Resolution.unknown()`

        Raises `ScrapeError` if more that 1 unknown resolution is parsed"""

        default_res: Resolution | None = None

        def parse(quality: yarl.URL | str | int | None, /) -> Resolution:
            nonlocal default_res
            try:
                return Resolution.parse(quality)
            except ValueError as e:
                if default_res is not None:
                    msg = "Unable to select best quality. Resource has more that 1 unknown resolution"
                    raise ScrapeError(422, msg) from e

                default_res = Resolution.unknown()
                return default_res

        return parse


UNKNOWN_RESOLUTION = Resolution.parse(0)
HIGHEST_RESOLUTION = Resolution(9999, 9999)


COMMON_RESOLUTIONS: Final = tuple(
    Resolution(*resolution)  # Best to worst
    for resolution in (
        (7680, 4320),
        (3840, 2160),
        (2560, 1440),
        (1920, 1080),
        (1280, 720),
        (640, 480),
        (640, 360),
        (480, 320),
        (426, 240),
        (320, 240),
        (256, 144),
    )
)


class ISO639Subtitle(NamedTuple):
    """`lang_code` MUST be a valid ISO639 code (ex: en, eng, fra)"""

    url: AbsoluteHttpURL | str
    lang_code: str
    name: str | None = None


Subtitle = ISO639Subtitle
