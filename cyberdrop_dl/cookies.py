from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from functools import wraps
from textwrap import dedent
from typing import TYPE_CHECKING, NamedTuple, ParamSpec, TypeVar

if sys.version_info < (3, 14):
    from http import cookies

    # https://github.com/python/cpython/issues/112713
    cookies.Morsel._reserved["partitioned"] = "partitioned"  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
    cookies.Morsel._flags.add("partitioned")  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]

from http.cookiejar import Cookie, CookieJar, MozillaCookieJar
from http.cookies import SimpleCookie

from cyberdrop_dl.dependencies import browser_cookie3

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Callable
    from pathlib import Path

    from cyberdrop_dl.constants import BROWSERS

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


class CookieExtractor(NamedTuple):
    name: str
    extract: Callable[..., CookieJar]


logger = logging.getLogger(__name__)


class UnsupportedBrowserError(browser_cookie3.BrowserCookieError): ...


_COOKIE_EXTRACTORS = {
    c.name: c for c in (CookieExtractor(func.__name__, func) for func in browser_cookie3.all_browsers)
}
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


def cookie_wrapper(func: Callable[_P, _R]) -> Callable[_P, _R]:
    """Wrapper handles errors for cookie extraction."""

    @wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        try:
            return func(*args, **kwargs)
        except PermissionError as e:
            msg = """We've encountered a Permissions Error. Please close all browsers and try again
                     If you are still having issues, make sure all browsers processes are closed in Task Manager"""
            msg = f"{dedent(msg)}\nERROR: {e!s}"

        except (ValueError, UnsupportedBrowserError) as e:
            msg = f"ERROR: {e!s}"

        except browser_cookie3.BrowserCookieError as e:
            msg = """Browser extraction ran into an error, the selected browser(s) may not be available on your system
                     If you are still having issues, make sure all browsers processes are closed in Task Manager."""
            msg = f"{dedent(msg)}\nERROR: {e!s}"

        raise browser_cookie3.BrowserCookieError(f"{msg}\n\nNothing has been saved.")

    return wrapper


@cookie_wrapper
def get_cookies_from_browser(browser: BROWSERS, domain: str, *domains: str) -> MozillaCookieJar:
    extractor_name = browser.lower()
    domains_to_extract: set[str] = set(domain, *domains)
    extracted_cookies = extract_cookies(extractor_name)
    cookie_jar = MozillaCookieJar()
    for cookie in extracted_cookies:
        for domain in domains_to_extract:
            if domain in cookie.domain:
                cookie_jar.set_cookie(cookie)

    return cookie_jar


def extract_cookies(extractor_name: str) -> CookieJar:
    extractor = _COOKIE_EXTRACTORS[extractor_name]
    try:
        return extractor.extract()
    except browser_cookie3.BrowserCookieError as e:
        msg = str(e)
        if (
            "Unable to get key for cookie decryption" in msg
            and extractor.name in _CHROMIUM_BROWSERS
            and os.name == "nt"
        ):
            msg = f"Cookie extraction from {extractor.name.capitalize()} is not supported on Windows - {msg}"
            raise UnsupportedBrowserError(msg) from None
        raise


async def read_netscape_files(cookie_files: list[Path]) -> AsyncIterable[tuple[str, SimpleCookie]]:
    now = int(time.time())
    domains_seen = set()
    cookie_jars = await asyncio.gather(*(_read_netscape_file(file) for file in cookie_files))
    for file, cookie_jar in zip(cookie_files, cookie_jars, strict=True):
        if not cookie_jar:
            continue
        current_cookie_file_domains: set[str] = set()
        expired_cookies_domains: set[str] = set()
        for cookie in cookie_jar:
            if not cookie.value:
                continue
            simplified_domain = cookie.domain.removeprefix(".")
            if simplified_domain not in current_cookie_file_domains:
                logger.info(f"Found cookies for {simplified_domain} in file '{file.name}'")
                current_cookie_file_domains.add(simplified_domain)
                if simplified_domain in domains_seen:
                    logger.warning(
                        f"Previous cookies for domain {simplified_domain} detected. They will be overwritten"
                    )

            if (simplified_domain not in expired_cookies_domains) and cookie.is_expired(now):
                expired_cookies_domains.add(simplified_domain)
                logger.info(f"Cookies for {simplified_domain} are expired")

            domains_seen.add(simplified_domain)
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
