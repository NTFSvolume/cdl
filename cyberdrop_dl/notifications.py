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
from cyberdrop_dl.logger import MAX_LOGS_SIZE, adopt_logger, export_logs, spacer
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
    send_logs: bool = False
    for server in urls:
        if not send_logs:
            send_logs = "attach_logs" in server.tags
        _ = apprise_obj.add(str(server.url.get_secret_value()), tag=sorted(server.tags))  # pyright: ignore[reportUnknownMemberType]

    messages: dict[str, dict[str, str]] = {
        "no_logs": _DEFAULT_MSG | {"body": content},
        "attach_logs": _DEFAULT_MSG | {"body": content},
        "simplified": _DEFAULT_MSG,
    }

    async def notify() -> None:
        with adopt_logger("apprise", level=logging.INFO):
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
    async with aio.temp_dir() as temp_dir:
        temp_file = temp_dir / main_log.name
        if content := await _get_logs_content(main_log):
            _ = await aio.write_bytes(temp_file, content)
            yield temp_file
        else:
            yield


async def _get_logs_content(path: Path) -> bytes | None:
    try:
        try:
            if (size := await aio.get_size(path)) and size > MAX_LOGS_SIZE:
                raise RuntimeError("Logs file is too big (>=25MB)")
            return await aio.read_bytes(path)
        except OSError:
            return (await asyncio.to_thread(export_logs)).encode("utf-8")
    except Exception:
        logger.exception("Unable to get copy of the main log file. 'attach_logs' URLs will be processed without it")


async def _prepare_form(diff_text: str, main_log: Path | None = None) -> FormData:
    form = FormData()
    if main_log and (content := await _get_logs_content(main_log)):
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
    try:
        async with session.post(url, data=form) as response:
            if response.ok:
                logger.info("Webhook notifications result: Success")

            else:
                json_resp: dict[str, Any] = await response.json()
                json_resp.pop("content", None)
                logger.error(f"Webhook notifications failed \n{json_resp!s}")

    except Exception:
        logger.exception("Unable to send webhook notification")


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
