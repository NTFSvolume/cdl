from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import shutil
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Final

from pydantic import ValidationError

from cyberdrop_dl.dependencies import apprise
from cyberdrop_dl.logger import copy_main_log_buffer, enable_3p_logger
from cyberdrop_dl.models import AppriseURLModel, format_validation_error

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterable


logger = logging.getLogger(__name__)

_DEFAULT_MSG: Final = {
    "body": "Finished downloading. Enjoy :)",
    "title": "Cyberdrop-DL",
    "body_format": "text",
}


@dataclasses.dataclass(slots=True, order=True)
class AppriseURL:
    url: str
    tags: set[str]

    def __str__(self) -> str:
        if not self.tags:
            return self.url
        return f"{','.join(sorted(self.tags))}={self.url}"


_OS_URLS = "windows://", "macosx://", "dbus://", "qt://", "glib://", "kde://"


def read(*, file: Path | None = None, urls: list[str] | None = None) -> tuple[AppriseURL, ...]:
    """
    Get Apprise URLs from the specified file or directly from a provided URL.

    Args:
        file (Path, optional): The path to the file containing Apprise URLs.
        url (str, optional): A single Apprise URL to be processed.

    Returns:
        list[AppriseURL] | None: A list of processed Apprise URLs, or None if no valid URLs are found.
    """

    if not (urls or file):
        raise ValueError("Neither url of file were supplied")
    if urls and file:
        raise ValueError("url of file are mutually exclusive")

    if file:
        if not file.is_file():
            return ()
        with file.open(encoding="utf8") as apprise_file:
            urls = [line.strip() for line in apprise_file if line.strip()]

    if not urls:
        return ()

    if apprise is None:
        logger.warning("Found apprise URLs for notifications but apprise is not installed. Ignoring")
        return ()
    try:
        return tuple(_simplify(AppriseURLModel.model_validate({"url": url}) for url in set(urls)))

    except ValidationError as e:
        logger.error(format_validation_error(e, file))
        sys.exit(1)


def _simplify(apprise_urls: Iterable[AppriseURLModel]) -> Generator[AppriseURL]:
    valid_tags = {"no_logs", "attach_logs", "simplified"}

    def is_os_url(url: str) -> bool:
        return any(scheme in url.casefold() for scheme in _OS_URLS)

    for model in apprise_urls:
        url = str(model.url.get_secret_value())
        tags = model.tags
        if not tags.intersection(valid_tags):
            tags = tags | {"no_logs"}

        if is_os_url(url):
            tags = (tags - valid_tags) | {"simplified"}

        yield AppriseURL(url=url, tags=tags)


async def send_apprise_notifications(content: str, *urls: AppriseURL, main_log: Path) -> None:
    if not urls:
        return

    logger.info("Sending Apprise notifications.. ")
    apprise_obj = apprise.Apprise()
    send_logs: bool = False
    for server in urls:
        if not send_logs:
            send_logs = "attach_logs" in server.tags
        _ = apprise_obj.add(server.url, tag=sorted(server.tags))  # pyright: ignore[reportUnknownMemberType]

    messages: dict[str, dict[str, str]] = {
        "no_logs": _DEFAULT_MSG | {"body": content},
        "attach_logs": _DEFAULT_MSG | {"body": content},
        "simplified": _DEFAULT_MSG,
    }

    async def notify() -> None:
        with enable_3p_logger("apprise", level=logging.INFO):
            _ = await asyncio.gather(*(apprise_obj.async_notify(**msg, tag=tag) for tag, msg in messages.items()))

    if not send_logs:
        await notify()
        return

    async with _temp_copy_of_main_log(main_log) as file:
        if file:
            messages["attach_logs"]["attach"] = str(file)

        await notify()


@contextlib.asynccontextmanager
async def _temp_copy_of_main_log(main_log: Path) -> AsyncGenerator[Path | None]:
    temp = await asyncio.to_thread(tempfile.TemporaryDirectory, prefix="cdl_", ignore_cleanup_errors=True)
    temp_file = Path(temp.name) / main_log.name
    try:
        try:
            _ = await asyncio.to_thread(shutil.copy, main_log, temp_file)
        except OSError:
            _ = await asyncio.to_thread(copy_main_log_buffer, temp_file)
    except (OSError, LookupError):
        logger.exception("Unable to get copy of the main log file. 'attach_logs' URLs will be processed without it")
        yield
    else:
        yield temp_file
    finally:
        await asyncio.to_thread(temp.cleanup)
