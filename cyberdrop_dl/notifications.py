from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from typing import TYPE_CHECKING, Any, Final

from aiohttp.formdata import FormData
from pydantic import ValidationError

from cyberdrop_dl import aio, constants
from cyberdrop_dl.dependencies import apprise
from cyberdrop_dl.logger import adopt_logger, get_logs_content, spacer
from cyberdrop_dl.models import AppriseURL, format_validation_error

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    import aiohttp

logger = logging.getLogger(__name__)


_DEFAULT_MSG: Final[dict[str, str]] = {
    "body": "Finished downloading. Enjoy :)",
    "title": "Cyberdrop-DL",
    "body_format": "text",
}
_DEFAULT_DIFF_LINE_FORMAT: str = "{}"
_STYLE_TO_DIFF_MAP: Final[dict[str, str]] = {
    "green": "+   {}",
    "red": "-   {}",
    "yellow": "*** {}",
}


def read_apprise_urls(file: Path) -> tuple[str, ...]:
    try:
        with file.open(encoding="utf8") as apprise_file:
            return tuple(line.strip() for line in apprise_file if line.strip())
    except OSError:
        return ()


def parse_apprise_url(*urls: str) -> tuple[AppriseURL, ...]:
    if apprise is None:
        logger.warning("Found apprise URLs for notifications but apprise is not installed. Ignoring")
        return ()
    try:
        return tuple(AppriseURL.model_validate({"url": url}) for url in set(urls))

    except ValidationError as e:
        logger.error(format_validation_error(e))
        sys.exit(1)


async def send_apprise_notifications(content: str, *urls: AppriseURL, main_log: Path) -> None:
    if not urls:
        return

    logger.info("Sending Apprise notifications.. ")
    apprise_obj = apprise.Apprise()
    attach_logs: bool = False
    for webhook in urls:
        if not attach_logs:
            attach_logs = webhook.attach_logs
        _ = apprise_obj.add(str(webhook.url.get_secret_value()), tag=sorted(webhook.tags))  # pyright: ignore[reportUnknownMemberType]

    messages: dict[str, dict[str, str]] = {
        "no_logs": _DEFAULT_MSG | {"body": content},
        "attach_logs": _DEFAULT_MSG | {"body": content},
        "simplified": _DEFAULT_MSG,
    }

    async def notify() -> None:
        with adopt_logger("apprise", level=logging.INFO):
            _ = await asyncio.gather(*(apprise_obj.async_notify(**msg, tag=tag) for tag, msg in messages.items()))

    if not attach_logs:
        await notify()
        return

    async with _temp_copy_of_main_log(main_log) as file:
        if file:
            messages["attach_logs"]["attach"] = str(file)

        await notify()


@contextlib.asynccontextmanager
async def _temp_copy_of_main_log(main_log: Path) -> AsyncGenerator[Path | None]:
    async with aio.temp_dir() as temp_dir:
        temp_file = temp_dir / main_log.name
        if content := await get_logs_content(main_log):
            _ = await aio.write_bytes(temp_file, content)
            yield temp_file
        else:
            yield


async def _prepare_form(diff_text: str, main_log: Path | None = None) -> FormData:
    form = FormData()
    if main_log and (content := await get_logs_content(main_log)):
        form.add_field("file", content, filename=main_log.name)

    form.add_fields(
        ("content", f"```diff\n{diff_text}```"),
        ("username", "cyberdrop-dl"),
    )
    return form


async def send_webhook_notification(session: aiohttp.ClientSession, webhook: AppriseURL, main_log: Path) -> None:
    logger.info("Sending webhook notifications.. ")
    url = webhook.url.get_secret_value()
    diff_text = _prepare_diff_text()
    form = await _prepare_form(diff_text, main_log if webhook.attach_logs else None)

    logger.info(spacer())
    error: dict[str, Any] | None = None
    try:
        async with session.post(url, data=form) as response:
            if not response.ok:
                error = await response.json()

    except Exception:
        logger.exception("Unable to send webhook notification")
    else:
        if error:
            error.pop("content", None)
            result = f"Failed \n{error!s}"
            level = logging.ERROR
        else:
            result = "Success"
            level = logging.INFO

        logger.log(level, f"Webhook notifications result: {result}")


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
