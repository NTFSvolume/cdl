from __future__ import annotations

import asyncio
import logging
import os
import time
from http.cookiejar import Cookie, CookieJar, MozillaCookieJar
from http.cookies import SimpleCookie
from typing import TYPE_CHECKING, Final

from cyberdrop_dl.dependencies import browser_cookie3

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from cyberdrop_dl.constants import Browser
    from cyberdrop_dl.managers.manager import Manager


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


async def _extract_cookies(browser: Browser) -> CookieJar:
    extract = _COOKIE_EXTRACTORS[browser]
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


async def extract_cookies(browser: Browser) -> CookieJar:

    try:
        return await _extract_cookies(browser)
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


async def split_and_save_cookies(manager: Manager, extracted_cookies: CookieJar) -> None:
    manager.appdata.cookies.mkdir(parents=True, exist_ok=True)
    cookie_jars: dict[str, MozillaCookieJar] = {}

    for domain, cookie in ((cookie.domain.lstrip(".").removeprefix("www."), cookie) for cookie in extracted_cookies):
        cookie_jar = cookie_jars.get(domain)
        if cookie_jar is None:
            cookie_jar = MozillaCookieJar(manager.appdata.cookies / f"{domain}.txt")
        cookie_jar.set_cookie(cookie)

    _ = await asyncio.gather(
        *(asyncio.to_thread(cj.save, ignore_discard=True, ignore_expires=True) for cj in cookie_jars.values())
    )


async def read_netscape_files(cookie_files: list[Path]) -> AsyncGenerator[tuple[str, SimpleCookie]]:
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

            if (simplified_domain not in expired_cookies_domains) and cookie.is_expired(now):  # type: ignore
                expired_cookies_domains.add(simplified_domain)
                logger.warning(f"Cookies for {simplified_domain} are expired")

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
