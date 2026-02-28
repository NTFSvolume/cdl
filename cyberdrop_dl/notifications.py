from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import shutil
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from aiohttp.formdata import FormData
from pydantic import ValidationError

from cyberdrop_dl import aio, constants
from cyberdrop_dl.dependencies import apprise
from cyberdrop_dl.logger import copy_main_log_buffer, enable_3p_logger, spacer
from cyberdrop_dl.models import AppriseURLModel, format_validation_error

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterable

    import aiohttp

logger = logging.getLogger(__name__)

_DEFAULT_MSG: Final = {
    "body": "Finished downloading. Enjoy :)",
    "title": "Cyberdrop-DL",
    "body_format": "text",
}

_DEFAULT_DIFF_LINE_FORMAT: str = "{}"
_STYLE_TO_DIFF_MAP = {
    "green": "+   {}",
    "red": "-   {}",
    "yellow": "*** {}",
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


def read_apprise_urls(*, file: Path | None = None, urls: list[str] | None = None) -> tuple[AppriseURL, ...]:
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
        return tuple(_simplify_apprise_urls(AppriseURLModel.model_validate({"url": url}) for url in set(urls)))

    except ValidationError as e:
        logger.error(format_validation_error(e, file))
        sys.exit(1)


def _simplify_apprise_urls(apprise_urls: Iterable[AppriseURLModel]) -> Generator[AppriseURL]:
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
    temp_dir = await asyncio.to_thread(tempfile.TemporaryDirectory, prefix="cdl_", ignore_cleanup_errors=True)
    temp_file = Path(temp_dir.name) / main_log.name
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
        await asyncio.to_thread(temp_dir.cleanup)


def _prepare_diff_text() -> str:
    """Returns the `rich.text` in the current log buffer as a plain str with diff syntax."""

    def prepare_lines():
        for text_line in constants.LOG_OUTPUT_TEXT.split(allow_blank=True):
            line_str = text_line.plain.rstrip("\n")
            first_span = text_line.spans[0] if text_line.spans else None
            style: str = str(first_span.style) if first_span else ""

            color = style.split(" ")[0] or "black"  # remove console hyperlink markup (if any)
            line_format: str = _STYLE_TO_DIFF_MAP.get(color) or _DEFAULT_DIFF_LINE_FORMAT
            yield line_format.format(line_str)

    return "\n".join(prepare_lines())


async def _prepare_form(main_log: Path | None = None) -> FormData:
    diff_text = _prepare_diff_text()
    form = FormData()

    if main_log and (size := await aio.get_size(main_log)):
        if size <= 25 * 1024 * 1024:  # 25MB
            form.add_field("file", await aio.read_bytes(main_log), filename=main_log.name)

        else:
            diff_text += "\n\nWARNING: log file too large to send as attachment\n"

    form.add_fields(
        ("content", f"```diff\n{diff_text}```"),
        ("username", "CyberDrop-DL"),
    )
    return form


async def send_webhook_notification(session: aiohttp.ClientSession, webhook: AppriseURLModel, main_log: Path) -> None:
    """Outputs the stats to a code block for webhook messages."""

    logger.info("Sending webhook notifications.. ")
    url = webhook.url.get_secret_value()
    form = await _prepare_form(main_log if "attach_logs" in webhook.tags else None)

    try:
        async with session.post(url, data=form) as response:
            if response.ok:
                result = "Success"

            else:
                json_resp: dict[str, Any] = await response.json()
                json_resp.pop("content", None)
                result = f"Failed \n{json_resp!s}"

    except Exception:
        logger.exception("Unable to send webhook notification")
    else:
        logger.info(spacer())
        logger.info(f"Webhook notifications results: {result}")
