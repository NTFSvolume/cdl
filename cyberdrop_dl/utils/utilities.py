from __future__ import annotations

import contextlib
import dataclasses
import functools
import inspect
import itertools
import logging
import os
import platform
import re
import sys
from collections.abc import Generator
from functools import partial, wraps
from http import HTTPStatus
from pathlib import Path
from stat import S_ISREG
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Concatenate,
    ParamSpec,
    Protocol,
    Self,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)

from aiohttp import ClientConnectorError, TooManyRedirects
from mega.errors import MegaNzError
from pydantic import ValidationError
from yarl import URL

from cyberdrop_dl import constants
from cyberdrop_dl.data_structures import AbsoluteHttpURL
from cyberdrop_dl.exceptions import (
    CDLBaseError,
    ErrorLogMessage,
    InvalidURLError,
    TooManyCrawlerErrors,
    create_error_msg,
    get_origin,
)
from cyberdrop_dl.utils import json
from cyberdrop_dl.utils.logger import log_with_color

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator, Iterable

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem, ScrapeItem
    from cyberdrop_dl.downloader.downloader import Downloader
    from cyberdrop_dl.managers.manager import Manager

    class _HasManager(Protocol):
        manager: Manager

    _HasManagerT = TypeVar("_HasManagerT", bound=_HasManager)
    _Origin = TypeVar("_Origin", bound=ScrapeItem | MediaItem | URL)

_P = ParamSpec("_P")
_T = TypeVar("_T")
_R = TypeVar("_R")


class Dataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]


logger = logging.getLogger(__name__)


_FIELDS_CACHE: dict[type, tuple[str, ...]] = {}


def _fields(cls: type) -> tuple[str, ...]:
    if fields := _FIELDS_CACHE.get(cls):
        return fields
    fields = _FIELDS_CACHE[cls] = tuple(f.name for f in dataclasses.fields(cls))
    return fields


class DictDataclass(Dataclass, Protocol):
    @classmethod
    def filter_dict(cls, data: dict[str, Any], /) -> dict[str, Any]:
        return {name: data.get(name) for name in _fields(cls)}

    @classmethod
    def from_dict(cls, data: dict[str, Any], /, **overrides: Any) -> Self:
        if overrides:
            data.update(overrides)
        return cls(**cls.filter_dict(data))


_BLOB_OR_SVG = ("data:", "blob:", "javascript:")


@contextlib.contextmanager
def error_handling_context(self: _HasManager, item: ScrapeItem | MediaItem | URL) -> Generator[None]:
    link: URL = item if isinstance(item, URL) else item.url
    error_log_msg = origin = exc_info = None
    link_to_show: URL | str = ""
    is_segment: bool = getattr(item, "is_segment", False)
    is_downloader: bool = bool(getattr(self, "log_prefix", False))
    try:
        yield
    except TooManyCrawlerErrors:
        return
    except CDLBaseError as e:
        error_log_msg = ErrorLogMessage(e.ui_failure, str(e))
        origin = e.origin
        link_to_show: URL | str = getattr(e, "url", None) or link_to_show
    except NotImplementedError as e:
        error_log_msg = ErrorLogMessage("NotImplemented")
        exc_info = e
    except TooManyRedirects as e:
        ui_failure = "Too Many Redirects"
        info = json.dumps({"url": e.request_info.real_url, "history": [r.real_url for r in e.history]}, indent=4)
        error_log_msg = ErrorLogMessage(ui_failure, f"{ui_failure}\n{info}")
    except MegaNzError as e:
        if code := getattr(e, "code", None):
            if http_code := {
                -9: HTTPStatus.GONE,
                -16: HTTPStatus.FORBIDDEN,
                -24: 509,
                -401: 509,
            }.get(code):
                ui_failure = create_error_msg(http_code)
            else:
                ui_failure = f"MegaNZ Error [{code}]"
        else:
            ui_failure = "MegaNZ Error"

        error_log_msg = ErrorLogMessage(ui_failure, str(e))

    except TimeoutError as e:
        error_log_msg = ErrorLogMessage("Timeout", repr(e))
    except ClientConnectorError as e:
        ui_failure = "Client Connector Error"
        suffix = "" if (link.host or "").startswith(e.host) else f" from {link}"
        log_msg = f"{e}{suffix}. If you're using a VPN, try turning it off"
        error_log_msg = ErrorLogMessage(ui_failure, log_msg)
    except ValidationError as e:
        exc_info = e
        ui_failure = create_error_msg(422)
        log_msg = str(e).partition("For further information")[0].strip()
        error_log_msg = ErrorLogMessage(ui_failure, log_msg)
    except Exception as e:
        exc_info = e
        error_log_msg = ErrorLogMessage.from_unknown_exc(e)

    if error_log_msg is None or is_segment:
        return

    link_to_show = link_to_show or link
    origin = origin or get_origin(item)
    if is_downloader:
        self, item = cast("Downloader", self), cast("MediaItem", item)
        self.write_download_error(item, error_log_msg, exc_info)
        return

    logger.error(f"Scrape Failed: {link_to_show} ({error_log_msg.main_log_msg})", exc_info=exc_info)
    self.manager.logs.write_scrape_error(link_to_show, error_log_msg.csv_log_msg, origin)
    self.manager.progress_manager.scrape_stats_progress.add_failure(error_log_msg.ui_failure)


@overload
def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, _Origin, _P], _R],
) -> Callable[Concatenate[_HasManagerT, _Origin, _P], _R]: ...


@overload
def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, _Origin, _P], Coroutine[None, None, _R]],
) -> Callable[Concatenate[_HasManagerT, _Origin, _P], Coroutine[None, None, _R]]: ...


def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, _Origin, _P], _R | Coroutine[None, None, _R]],
) -> Callable[Concatenate[_HasManagerT, _Origin, _P], _R | Coroutine[None, None, _R]]:
    """Wrapper handles errors for url scraping."""

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(self: _HasManagerT, item: _Origin, *args: _P.args, **kwargs: _P.kwargs) -> _R:
            with error_handling_context(self, item):
                return await func(self, item, *args, **kwargs)

        return async_wrapper

    @wraps(func)
    def wrapper(self: _HasManagerT, item: _Origin, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        with error_handling_context(self, item):
            result = func(self, item, *args, **kwargs)
            assert not inspect.isawaitable(result)
            return result

    return wrapper


def get_download_path(manager: Manager, scrape_item: ScrapeItem, domain: str) -> Path:
    """Returns the path to the download folder."""
    download_dir = manager.config.files.download_folder

    return download_dir / scrape_item.create_download_path(domain)


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


def get_size(path: os.DirEntry[str]) -> int | None:
    try:
        return path.stat(follow_symlinks=False).st_size
    except (OSError, ValueError):
        return


def purge_dir_tree(dirname: Path | str) -> bool:
    """walks and removes in place"""

    has_non_empty_files = False
    has_non_empty_subfolders = False

    try:
        for entry in os.scandir(dirname):
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            if is_dir:
                deleted = purge_dir_tree(entry.path)
                if not deleted:
                    has_non_empty_subfolders = True
            elif get_size(entry) == 0:
                os.unlink(entry)  # noqa: PTH108
            else:
                has_non_empty_files = True

    except (OSError, PermissionError):
        pass

    if has_non_empty_files or has_non_empty_subfolders:
        return False
    try:
        os.rmdir(dirname)  # noqa: PTH106
        return True
    except OSError:
        return False


def check_partials_and_empty_folders(manager: Manager) -> None:
    """Checks for partial downloads, deletes partial files and empty folders."""
    settings = manager.config_manager.settings_data.runtime_options
    if settings.delete_partial_files:
        delete_partial_files(manager)
    if not settings.skip_check_for_partial_files:
        check_for_partial_files(manager)
    if not settings.skip_check_for_empty_folders:
        delete_empty_folders(manager)


def _partial_files(dir: Path | str) -> Generator[Path]:
    try:
        for entry in os.scandir(dir):
            try:
                if entry.is_dir(follow_symlinks=False):
                    yield from _partial_files(entry.path)
                    continue
            except OSError:
                pass

            suffix = entry.name.rpartition(".")[-1]
            if f".{suffix}" in constants.TempExt:
                yield Path(entry.path)
    except OSError:
        return


def delete_partial_files(manager: Manager) -> None:
    """Deletes partial download files recursively."""
    log_red("Deleting partial downloads...")
    for file in _partial_files(manager.config.files.download_folder):
        file.unlink(missing_ok=True)


def check_for_partial_files(manager: Manager) -> None:
    """Checks if there are partial downloads in any subdirectory and logs if found."""
    log_yellow("Checking for partial downloads...")
    has_partial_files = next(_partial_files(manager.config.files.download_folder), None)
    if has_partial_files:
        log_yellow("There are partial downloads in the downloads folder")


def delete_empty_folders(manager: Manager) -> None:
    """Deletes empty folders efficiently."""
    log_yellow("Checking for empty folders...")
    purge_dir_tree(manager.config.files.download_folder)

    sorted_folder = manager.config.sorting.sort_folder
    if sorted_folder and manager.config_manager.settings_data.sorting.sort_downloads:
        purge_dir_tree(sorted_folder)


def get_text_between(original_text: str, start: str, end: str) -> str:
    """Extracts the text between two strings in a larger text. Result will be stripped"""
    start_index = original_text.index(start) + len(start)
    end_index = original_text.index(end, start_index)
    return original_text[start_index:end_index].strip()


def _str_to_url(link_str: str) -> URL:
    def fix_query_params_encoding(link: str) -> str:
        if "?" not in link:
            return link
        parts, query_and_frag = link.split("?", 1)
        query_and_frag = query_and_frag.replace("+", "%20")
        return f"{parts}?{query_and_frag}"

    def fix_multiple_slashes(link_str: str) -> str:
        return re.sub(r"(?:https?)?:?(\/{3,})", "//", link_str)

    if not link_str:
        raise InvalidURLError("link_str is empty", url=link_str)

    try:
        clean_link_str = fix_multiple_slashes(fix_query_params_encoding(link_str))
        return URL(clean_link_str, encoded="%" in clean_link_str)

    except (AttributeError, ValueError, TypeError) as e:
        raise InvalidURLError(str(e), url=link_str) from e


def parse_url(
    link_str: AbsoluteHttpURL | URL | str, relative_to: AbsoluteHttpURL | None = None, *, trim: bool = True
) -> AbsoluteHttpURL:
    """Parse a string into an absolute URL, handling relative URLs, encoding and optionally removes trailing slash (trimming).
    Raises:
        InvalidURLError: If the input string is not a valid URL or if any other error occurs during parsing.
        TypeError: If `relative_to` is `None` and the parsed URL is relative or has no scheme.
    """

    url = _str_to_url(link_str) if isinstance(link_str, str) else link_str
    if not url.absolute:
        if not relative_to:
            raise InvalidURLError("Relative URL with no known base", url=link_str)
        url = relative_to.join(url)
    if not url.scheme:
        url = url.with_scheme(relative_to.scheme if relative_to else "https")
    assert is_absolute_http_url(url)
    if not trim:
        return url
    return remove_trailing_slash(url)


def is_absolute_http_url(url: URL) -> TypeGuard[AbsoluteHttpURL]:
    return url.absolute and url.scheme.startswith("http")


def remove_trailing_slash(url: AbsoluteHttpURL) -> AbsoluteHttpURL:
    if url.name or url.path == "/":
        return url
    return url.parent.with_fragment(url.fragment).with_query(url.query)


def remove_parts(
    url: AbsoluteHttpURL, *parts_to_remove: str, keep_query: bool = True, keep_fragment: bool = True
) -> AbsoluteHttpURL:
    if not parts_to_remove:
        return url
    new_parts = [p for p in url.parts[1:] if p not in parts_to_remove]
    return url.with_path("/".join(new_parts), keep_fragment=keep_fragment, keep_query=keep_query)


def get_size_or_none(path: Path) -> int | None:
    """Checks if this is a file and returns its size with a single system call.

    Returns `None` otherwise"""

    try:
        stat = path.stat()
        if S_ISREG(stat.st_mode):
            return stat.st_size
    except (OSError, ValueError):
        return None


@functools.cache
def get_system_information() -> str:
    def get_common_name() -> str:
        system = platform.system()

        if system in ("Linux",):
            try:
                return platform.freedesktop_os_release()["PRETTY_NAME"]
            except OSError:
                pass

        if system == "Android" and sys.version_info >= (3, 13):
            ver = platform.android_ver()
            os_name = f"{system} {ver.release}"
            for component in (ver.manufacturer, ver.model, ver.device):
                if component:
                    os_name += f" ({component})"
            return os_name

        default = platform.platform(aliased=True, terse=True).replace("-", " ")
        if system == "Windows" and (edition := platform.win32_edition()):
            return f"{default} {edition}"
        return default

    system_info = platform.uname()._asdict() | {
        "architecture": str(platform.architecture()),
        "python": f"{platform.python_version()} {platform.python_implementation()}",
        "common_name": get_common_name(),
    }
    _ = system_info.pop("node", None)
    return json.dumps(system_info, indent=4)


def is_blob_or_svg(link: str) -> bool:
    return any(link.startswith(x) for x in _BLOB_OR_SVG)


def unique(iterable: Iterable[_T], *, hashable: bool = True) -> Iterable[_T]:
    """Yields unique values from iterable, keeping original order"""
    if hashable:
        seen: set[_T] | list[_T] = set()
        add: Callable[[_T], None] = seen.add
    else:
        seen = []
        add = seen.append

    for value in iterable:
        if value not in seen:
            add(value)
            yield value


def xor_decrypt(encrypted_data: bytes, key: bytes) -> str:
    data = bytearray(b_input ^ b_key for b_input, b_key in zip(encrypted_data, itertools.cycle(key)))
    return data.decode("utf-8", errors="ignore")


log_yellow = partial(log_with_color, style="yellow", level=20)
log_red = partial(log_with_color, style="red", level=20)
