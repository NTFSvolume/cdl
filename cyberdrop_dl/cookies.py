from __future__ import annotations

import asyncio
import functools
import logging
import os
import sys
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Final, ParamSpec, TypeVar

if sys.version_info < (3, 14):
    from http import cookies

    # https://github.com/python/cpython/issues/112713
    cookies.Morsel._reserved["partitioned"] = "partitioned"  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
    cookies.Morsel._flags.add("partitioned")  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]

from http.cookiejar import Cookie, CookieJar, MozillaCookieJar
from http.cookies import SimpleCookie

from cyberdrop_dl.dependencies import browser_cookie3

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
    from pathlib import Path

    from cyberdrop_dl.constants import Browser

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


logger = logging.getLogger(__name__)


class UnsupportedBrowserError(browser_cookie3.BrowserCookieError): ...


_COOKIE_EXTRACTORS: Final = {func.__name__: func for func in browser_cookie3.all_browsers}
_CHROMIUM_BROWSERS = frozenset(
    (
        "chrome",
        "chromium",
        "opera",
        "opera_gx",
        "brave",
        "edge",
        "vivaldi",
        "arc",
    )
)


def cookie_wrapper(func: Callable[_P, Awaitable[_R]]) -> Callable[_P, Awaitable[_R]]:
    """Wrapper handles errors for cookie extraction."""

    @functools.wraps(func)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return await func(*args, **kwargs)
        except PermissionError as e:
            msg = (
                "We've encountered a Permissions Error. Please close all browsers and try again\n"
                "If you are still having issues, make sure all browsers processes are closed in Task Manager\n"
                f"ERROR: {e!s}"
            )

        except (ValueError, UnsupportedBrowserError) as e:
            msg = f"ERROR: {e!s}"

        except browser_cookie3.BrowserCookieError as e:
            msg = (
                "Browser extraction ran into an error, the selected browser may not be available on your system\n"
                "If you are still having issues, make sure all browsers processes are closed in Task Manager.\n"
                f"ERROR: {e!s}"
            )

        raise browser_cookie3.BrowserCookieError(f"{msg}\n\nNothing has been saved.")

    return wrapper


@cookie_wrapper
async def get_cookies_from_browser(browser: Browser, *domains_to_filter: str) -> MozillaCookieJar:
    extracted_cookies = await _extract_cookies(browser)
    cookie_jar = MozillaCookieJar()
    for cookie in extracted_cookies:
        if not domains_to_filter:
            cookie_jar.set_cookie(cookie)
        else:
            for domain in domains_to_filter:
                if domain in cookie.domain:
                    cookie_jar.set_cookie(cookie)

    return cookie_jar


async def _extract_cookies(browser: Browser) -> CookieJar:
    extract = _COOKIE_EXTRACTORS[str(browser)]
    try:
        return await asyncio.to_thread(extract)
    except browser_cookie3.BrowserCookieError as e:
        if (
            "Unable to get key for cookie decryption" in (msg := str(e))
            and browser in _CHROMIUM_BROWSERS
            and os.name == "nt"
        ):
            msg = f"Cookie extraction from {browser.capitalize()} is not supported on Windows - {msg}"
            raise UnsupportedBrowserError(msg) from None
        raise


async def read_netscape_files(files: Iterable[Path]) -> AsyncIterable[tuple[str, SimpleCookie]]:
    now = int(time.time())

    domains_seen: set[str] = set()
    cookie_jars = await asyncio.gather(*map(_read_netscape_file, files))

    for file, cookie_jar in zip(files, cookie_jars, strict=True):
        if not cookie_jar:
            continue

        domains: set[str] = set()
        has_expired_cookies: set[str] = set()
        for cookie in cookie_jar:
            if not cookie.value:
                continue

            domain = cookie.domain.lstrip(".")
            if domain not in domains:
                logger.info(f"Found cookies for {domain} in file '{file}'")
                domains.add(domain)
                if domain in domains_seen:
                    logger.warning(f"Previous cookies for {domain} detected. They will be overwritten")

            if (domain not in has_expired_cookies) and cookie.is_expired(now):
                has_expired_cookies.add(domain)
                logger.info(f"Cookies for {domain} are expired")

            domains_seen.add(domain)
            simple_cookie = make_simple_cookie(cookie, now)
            yield cookie.domain, simple_cookie


async def _read_netscape_file(file: Path) -> MozillaCookieJar | None:
    def read():
        cookie_jar = MozillaCookieJar(file)
        try:
            cookie_jar.load(ignore_discard=True)
            return cookie_jar
        except OSError as e:
            logger.error(f"Unable to load cookies from '{file.name}':\n  {e!s}")

    return await asyncio.to_thread(read)


def make_simple_cookie(cookie: Cookie, now: float) -> SimpleCookie:
    simple_cookie = SimpleCookie()
    assert cookie.value is not None
    simple_cookie[cookie.name] = cookie.value
    morsel = simple_cookie[cookie.name]
    morsel["domain"] = cookie.domain
    morsel["path"] = cookie.path
    morsel["secure"] = cookie.secure
    if cookie.expires:
        morsel["max-age"] = str(max(0, cookie.expires - int(now)))
    else:
        morsel["max-age"] = ""
    return simple_cookie
