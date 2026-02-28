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
from http import HTTPStatus
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Concatenate,
    ParamSpec,
    Protocol,
    SupportsInt,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)

from aiohttp import ClientConnectorError, TooManyRedirects
from mega.errors import MegaNzError
from pydantic import ValidationError
from yarl import URL

from cyberdrop_dl import config, constants
from cyberdrop_dl.exceptions import (
    CDLBaseError,
    ErrorLogMessage,
    InvalidURLError,
    TooManyCrawlerErrors,
    create_error_msg,
    get_origin,
)

from . import json

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator, Mapping

    from cyberdrop_dl.data_structures.url_objects import AbsoluteHttpURL, MediaItem, ScrapeItem
    from cyberdrop_dl.managers import Manager

    _P = ParamSpec("_P")
    _T = TypeVar("_T")
    _R = TypeVar("_R")

    class Dataclass(Protocol):
        __dataclass_fields__: ClassVar[dict[str, Any]]

    class _HasManager(Protocol):
        manager: Manager

    _HasManagerT = TypeVar("_HasManagerT", bound=_HasManager)
    Origin = TypeVar("Origin", bound=ScrapeItem | MediaItem | URL)


logger = logging.getLogger(__name__)


def get_download_path(manager: Manager, scrape_item: ScrapeItem, domain: str) -> Path:
    """Returns the path to the download folder."""
    download_dir = config.get().files.download_folder

    return download_dir / scrape_item.create_download_path(domain)


@contextlib.contextmanager
def error_handling_context(self: _HasManager, item: ScrapeItem | MediaItem | URL) -> Generator[None]:
    link: URL = item if isinstance(item, URL) else item.url
    error_log_msg = origin = exc_info = None
    link_to_show: URL | str = ""
    is_segment: bool = getattr(item, "is_segment", False)
    is_media_item: bool = hasattr(item, "is_segment")
    try:
        yield
    except TooManyCrawlerErrors:
        return
    except CDLBaseError as e:
        error_log_msg = ErrorLogMessage(e.ui_failure, str(e))
        origin = e.origin
        link_to_show = getattr(e, "url", None) or link_to_show
    except NotImplementedError as e:
        error_log_msg = ErrorLogMessage("NotImplemented")
        exc_info = e
    except TooManyRedirects as e:
        ui_failure = "Too Many Redirects"
        info = json.dumps(
            {
                "url": e.request_info.real_url,
                "history": [r.real_url for r in e.history],
            },
            indent=4,
            default=str,
        )
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
    if is_media_item:
        item = cast("MediaItem", item)
        msg = f"Download Failed: {item.url} ({error_log_msg.main_log_msg}) \n -> Referer: {item.referer}"
        logger.error(msg, exc_info=exc_info)
        self.manager.logs.write_download_error(item, error_log_msg.csv_log_msg)
        self.manager.tui.download_errors.add(error_log_msg.ui_failure)
        self.manager.tui.files.add_failed()
        return

    logger.error(f"Scrape Failed: {link_to_show} ({error_log_msg.main_log_msg})", exc_info=exc_info)
    self.manager.logs.write_scrape_error(link_to_show, error_log_msg.csv_log_msg, origin)
    self.manager.tui.scrape_errors.add(error_log_msg.ui_failure)


@overload
def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, Origin, _P], _R],
) -> Callable[Concatenate[_HasManagerT, Origin, _P], _R]: ...


@overload
def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, Origin, _P], Coroutine[None, None, _R]],
) -> Callable[Concatenate[_HasManagerT, Origin, _P], Coroutine[None, None, _R]]: ...


def error_handling_wrapper(
    func: Callable[Concatenate[_HasManagerT, Origin, _P], _R | Coroutine[None, None, _R]],
) -> Callable[Concatenate[_HasManagerT, Origin, _P], _R | Coroutine[None, None, _R]]:
    """Wrapper handles errors for url scraping."""

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self: _HasManagerT, item: Origin, *args: _P.args, **kwargs: _P.kwargs) -> _R:
            with error_handling_context(self, item):
                return await func(self, item, *args, **kwargs)

        return async_wrapper

    @functools.wraps(func)
    def wrapper(self: _HasManagerT, item: Origin, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        with error_handling_context(self, item):
            result = func(self, item, *args, **kwargs)
            assert not inspect.isawaitable(result)
            return result

    return wrapper


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


def _get_size(path: os.DirEntry[str]) -> int | None:
    try:
        return path.stat(follow_symlinks=False).st_size
    except (OSError, ValueError):
        return


def check_partials_and_empty_folders(config: config.Config) -> None:
    """Checks for partial downloads, deletes partial files and empty folders."""
    download_folder = config.files.download_folder
    _check_for_partial_files(download_folder)
    if config.runtime.delete_partial_files:
        logger.info("Deleting partial downloads...")
        for file in _partial_files(download_folder):
            file.unlink(missing_ok=True)

    if config.runtime.skip_check_for_empty_folders:
        return

    logger.info("Deleting empty files and folders...")
    _ = delete_empty_files_and_folders(download_folder)

    if config.sorting.sort_downloads and not config.sorting.sort_folder.is_relative_to(download_folder):
        _ = delete_empty_files_and_folders(config.sorting.sort_folder)


def delete_empty_files_and_folders(dir: Path | str) -> bool:
    """walks and removes empty files/folder recursively

    Returns `True` if `path` itself was deleted (AKA, it only had empty files and folders inside), `False` othwerwise"""

    has_non_empty_files = False
    has_non_empty_subfolders = False

    try:
        for entry in os.scandir(dir):
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            if is_dir:
                deleted = delete_empty_files_and_folders(entry.path)
                if not deleted:
                    has_non_empty_subfolders = True
            elif _get_size(entry) == 0:
                os.unlink(entry)  # noqa: PTH108
            else:
                has_non_empty_files = True

    except (OSError, PermissionError):
        pass

    if has_non_empty_files or has_non_empty_subfolders:
        return False
    try:
        os.rmdir(dir)  # noqa: PTH106
        return True
    except OSError:
        return False


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


def _check_for_partial_files(path: Path) -> None:
    logger.info("Checking for partial downloads...")
    has_partial_files = next(_partial_files(path), None)
    if has_partial_files:
        logger.warning("There are partial downloads in the downloads folder")


def get_valid_dict(dataclass: Dataclass | type[Dataclass], info: Mapping[str, Any]) -> dict[str, Any]:
    """Remove all keys that are not fields in the dataclass"""
    fields_names = tuple(f.name for f in dataclasses.fields(dataclass))
    return {name: info[name] for name in fields_names if name in info}


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

    try:
        assert link_str, "link_str is empty"
        assert isinstance(link_str, str), f"link_str must be a string object, got: {link_str!r}"
        clean_link_str = fix_multiple_slashes(fix_query_params_encoding(link_str))
        is_encoded = "%" in clean_link_str
        return URL(clean_link_str, encoded=is_encoded)

    except (AssertionError, AttributeError, ValueError, TypeError) as e:
        raise InvalidURLError(str(e), url=link_str) from e


def parse_url(link_str: URL | str, relative_to: AbsoluteHttpURL | None = None, *, trim: bool = True) -> AbsoluteHttpURL:
    """Parse a string into an absolute URL, handling relative URLs, encoding and optionally removes trailing slash (trimming).
    Raises:
        InvalidURLError: If the input string is not a valid URL or if any other error occurs during parsing.
        TypeError: If `relative_to` is `None` and the parsed URL is relative or has no scheme.
    """

    url = _str_to_url(link_str) if isinstance(link_str, str) else link_str
    if not url.absolute:
        assert relative_to
        url = relative_to.join(url)
    if not url.scheme:
        url = url.with_scheme("https")
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
    return any(link.startswith(x) for x in ("data:", "blob:", "javascript:"))


def get_valid_kwargs(
    func: Callable[..., Any], kwargs: Mapping[str, _T], accept_kwargs: bool = True
) -> Mapping[str, _T]:
    """Get the subset of ``kwargs`` that are valid params for ``func`` and their values are not `None`

    If func takes **kwargs, returns everything"""
    params = inspect.signature(func).parameters
    if accept_kwargs and any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return kwargs

    return {k: v for k, v in kwargs.items() if k in params.keys() and v is not None}


def call_w_valid_kwargs(cls: Callable[..., _R], kwargs: Mapping[str, Any]) -> _R:
    return cls(**get_valid_kwargs(cls, kwargs))


def type_adapter(func: Callable[..., _R], aliases: dict[str, str] | None = None) -> Callable[[dict[str, Any]], _R]:
    """Like `pydantic.TypeAdapter`, but without type validation of attributes (faster)

    Ignores attributes with `None` as value"""
    param_names = inspect.signature(func).parameters.keys()

    def call(kwargs: dict[str, Any]):
        if aliases:
            for original, alias in aliases.items():
                if original not in kwargs:
                    kwargs[original] = kwargs.get(alias)

        return func(**{name: value for name in param_names if (value := kwargs.get(name)) is not None})

    return call


def xor_decrypt(encrypted_data: bytes, key: bytes) -> str:
    data = bytearray(b_input ^ b_key for b_input, b_key in zip(encrypted_data, itertools.cycle(key)))
    return data.decode("utf-8", errors="ignore")


def filter_query(
    query: Mapping[str, str | SupportsInt | float],
    *keep: str | tuple[str, str | SupportsInt | float],
) -> dict[str, str | SupportsInt | float]:
    """Returns a dictionary with only the `keep` keys.

     Each `keep` argument can be either:
    - A string: The key will be kept only if was present in `query`
    - A tuple `(key, default_value)`: If `key` is not found in `query`, it will be added with `default_value`.
    """

    defaults: dict[str, str | SupportsInt | float] = {}
    keys: set[str] = set()
    for key in keep:
        if isinstance(key, str):
            keys.add(key)
            continue
        name, default = key
        defaults[name] = default
        keys.add(name)

    def get_key(key: str):
        if key in query:
            return query[key]
        return defaults.get(key)

    return {k: value for k in sorted(keys) if (value := get_key(k)) is not None}


def keep_query_params(url: AbsoluteHttpURL, *keep: str | tuple[str, str | SupportsInt | float]) -> AbsoluteHttpURL:
    """Returns a new URL with only the `keep` keys as query params.

    Each `keep` argument can be either:
    - A string: The key will be kept only if was present in `url.query`
    - A tuple `(key, default_value)`: If `key` is not found in `url.query`, it will be added with `default_value`.
    """
    return url.with_query(filter_query(url.query, *keep))
