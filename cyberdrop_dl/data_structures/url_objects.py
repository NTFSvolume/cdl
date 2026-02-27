from __future__ import annotations

import contextlib
import copy
import datetime
import logging
from dataclasses import asdict, dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Self, overload

import yarl

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

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


class ScrapeItemType(IntEnum):
    FORUM = 0
    FORUM_POST = 1
    FILE_HOST_PROFILE = 2
    FILE_HOST_ALBUM = 3


FORUM = ScrapeItemType.FORUM
FORUM_POST = ScrapeItemType.FORUM_POST
FILE_HOST_PROFILE = ScrapeItemType.FILE_HOST_PROFILE
FILE_HOST_ALBUM = ScrapeItemType.FILE_HOST_ALBUM


class HlsSegment(NamedTuple):
    part: str
    name: str
    url: AbsoluteHttpURL


@dataclass(unsafe_hash=True, slots=True, kw_only=True)
class MediaItem:
    url: AbsoluteHttpURL
    domain: str
    referer: AbsoluteHttpURL
    download_folder: Path
    filename: str
    original_filename: str
    download_filename: str | None = None
    filesize: int | None = None
    ext: str
    db_path: str
    debrid_url: AbsoluteHttpURL | None = None
    duration: float | None = None
    is_segment: bool = False
    album_id: str | None = None
    timestamp: int | None = None

    parents: list[AbsoluteHttpURL] = field(default_factory=list)
    parent_threads: set[AbsoluteHttpURL] = field(default_factory=set)
    current_attempt: int = field(default=0)
    complete_file: Path = field(init=False)
    hash: str | None = None

    headers: dict[str, str] = field(default_factory=dict, compare=False)
    downloaded: bool = False
    metadata: object = field(init=False, default_factory=dict, compare=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(domain={self.domain!r}, url={self.url!r}, referer={self.referer!r}, filename={self.filename!r}"

    def __post_init__(self) -> None:
        if self.url.scheme == "metadata":
            self.db_path = ""

        self.complete_file = self.download_folder / self.filename

    @property
    def real_url(self) -> AbsoluteHttpURL:
        return self.debrid_url or self.url

    @property
    def partial_file(self) -> Path:
        return self.complete_file.with_suffix(self.complete_file.suffix + ".part")

    def datetime_obj(self) -> datetime.datetime | None:
        if self.timestamp:
            assert isinstance(self.timestamp, int), f"Invalid {self.timestamp =!r} from {self.referer}"
            return datetime.datetime.fromtimestamp(self.timestamp, tz=datetime.UTC)

    @staticmethod
    def from_item(
        origin: ScrapeItem | MediaItem,
        url: AbsoluteHttpURL,
        domain: str,
        /,
        *,
        download_folder: Path,
        filename: str,
        db_path: str,
        original_filename: str | None = None,
        ext: str = "",
    ) -> MediaItem:
        return MediaItem(
            url=url,
            domain=domain,
            download_folder=download_folder,
            filename=filename,
            db_path=db_path,
            referer=origin.url,
            album_id=origin.album_id,
            ext=ext or Path(filename).suffix,
            original_filename=original_filename or filename,
            parents=origin.parents.copy(),
            timestamp=origin.timestamp,
            parent_threads=origin.parent_threads.copy(),
        )

    def as_jsonable_dict(self) -> dict[str, Any]:
        item = asdict(self)
        if datetime := self.datetime_obj():
            item["datetime"] = datetime
        item["attempts"] = item.pop("current_attempt")
        item["partial_file"] = self.partial_file
        if self.hash:
            item["hash"] = f"xxh128:{self.hash}"
        for name in ("is_segment",):
            _ = item.pop(name)
        return item


@dataclass(kw_only=True, slots=True)
class ScrapeItem:
    url: AbsoluteHttpURL
    parent_title: str = ""
    part_of_album: bool = False
    album_id: str | None = None
    timestamp: int | None = None
    retry_path: Path | None = None

    parents: list[AbsoluteHttpURL] = field(default_factory=list, init=False)
    parent_threads: set[AbsoluteHttpURL] = field(default_factory=set, init=False)
    children: int = field(default=0, init=False)
    children_limit: int = field(default=0, init=False)
    type: ScrapeItemType | None = field(default=None, init=False)
    completed_at: int | None = field(default=None, init=False)
    created_at: int | None = field(default=None, init=False)
    children_limits: list[int] = field(default_factory=list, init=False)
    password: str | None = field(default=None, init=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(url={self.url!r}, parent_title={self.parent_title!r}, possible_datetime={self.timestamp!r}"

    def __post_init__(self) -> None:
        self.password = self.url.query.get("password")

    def add_to_parent_title(self, title: str) -> None:
        """Adds a title to the parent title."""
        from cyberdrop_dl.utils import sanitize_folder

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

    def set_type(self, scrape_item_type: ScrapeItemType | None, *_) -> None:
        self.type = scrape_item_type
        self.reset_childen()

    def reset_childen(self) -> None:
        self.children = self.children_limit = 0
        if self.type is None:
            return
        try:
            self.children_limit = self.children_limits[self.type]
        except (IndexError, TypeError):
            pass

    def add_children(self, number: int = 1) -> None:
        self.children += number
        if self.children_limit and self.children >= self.children_limit:
            from cyberdrop_dl.exceptions import MaxChildrenError

            raise MaxChildrenError(origin=self)

    def reset(self, reset_parents: bool = False, reset_parent_title: bool = False) -> None:
        """Resets `album_id`, `type` and `posible_datetime` back to `None`

        Reset `part_of_album` back to `False`
        """
        self.album_id = self.timestamp = self.type = None
        self.part_of_album = False
        self.reset_childen()
        if reset_parents:
            self.parents = []
            self.parent_threads = set()
        if reset_parent_title:
            self.parent_title = ""

    def setup_as(self, title: str, type: ScrapeItemType, *, album_id: str | None = None) -> None:
        self.part_of_album = True
        if album_id:
            self.album_id = album_id
        if self.type != type:
            self.set_type(type)
        self.add_to_parent_title(title)

    def create_new(
        self,
        url: AbsoluteHttpURL,
        *,
        new_title_part: str = "",
        part_of_album: bool = False,
        album_id: str | None = None,
        possible_datetime: int | None = None,
        add_parent: AbsoluteHttpURL | bool | None = None,
    ) -> Self:
        """Creates a scrape item."""
        from cyberdrop_dl.utils import is_absolute_http_url

        scrape_item = self.copy()
        assert is_absolute_http_url(url)

        if add_parent:
            new_parent = add_parent if isinstance(add_parent, AbsoluteHttpURL) else self.url
            assert is_absolute_http_url(new_parent)
            scrape_item.parents.append(new_parent)

        if new_title_part:
            scrape_item.add_to_parent_title(new_title_part)

        scrape_item.url = url
        scrape_item.part_of_album = part_of_album or scrape_item.part_of_album
        scrape_item.timestamp = possible_datetime or scrape_item.timestamp
        scrape_item.album_id = album_id or scrape_item.album_id
        return scrape_item

    def create_child(
        self,
        url: AbsoluteHttpURL,
        *,
        new_title_part: str = "",
        album_id: str | None = None,
        possible_datetime: int | None = None,
    ) -> Self:
        return self.create_new(
            url,
            part_of_album=True,
            add_parent=True,
            new_title_part=new_title_part,
            album_id=album_id,
            possible_datetime=possible_datetime,
        )

    def setup_as_album(self: ScrapeItem, title: str, *, album_id: str | None = None) -> None:
        return self.setup_as(title, type=FILE_HOST_ALBUM, album_id=album_id)

    def setup_as_profile(self: ScrapeItem, title: str, *, album_id: str | None = None) -> None:
        return self.setup_as(title, type=FILE_HOST_PROFILE, album_id=album_id)

    def setup_as_forum(self: ScrapeItem, title: str, *, album_id: str | None = None) -> None:
        return self.setup_as(title, type=FORUM, album_id=album_id)

    def setup_as_post(self: ScrapeItem, title: str, *, album_id: str | None = None) -> None:
        return self.setup_as(title, type=FORUM_POST, album_id=album_id)

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
        if self.parent_title and self.part_of_album:
            return Path(self.parent_title)
        if self.parent_title:
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


@dataclass(slots=True, order=True)
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
        from cyberdrop_dl.utils.dates import parse_aware_iso_datetime

        if value := query.get(name):
            return parse_aware_iso_datetime(value)
