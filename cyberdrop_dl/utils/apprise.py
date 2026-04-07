from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Final

from cyberdrop_dl import aio
from cyberdrop_dl.dependencies import apprise
from cyberdrop_dl.models import AppriseURL
from cyberdrop_dl.utils.logger import MAIN_LOG_FILE, borrow_logger, export_logs

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_MSG: Final[dict[str, str]] = {
    "body": "Finished downloading. Enjoy :)",
    "title": "Cyberdrop-DL",
    "body_format": "text",
}


def read_apprise_urls(file: Path) -> tuple[AppriseURL, ...]:
    return _parse_apprise_url(*_read_apprise_urls(file))


def _read_apprise_urls(file: Path) -> tuple[str, ...]:
    try:
        with file.open(encoding="utf8") as apprise_file:
            return tuple(line.strip() for line in apprise_file if line.strip())
    except OSError:
        logger.exception(f"Unable to read apprise URL from '{file}'. Ignoring")
        return ()


def _parse_apprise_url(*urls: str) -> tuple[AppriseURL, ...]:
    if not urls:
        return ()

    if apprise is None:
        logger.warning("Found apprise URLs for notifications but apprise is not installed. Ignoring")
        return ()

    return tuple(AppriseURL.model_validate({"url": url}) for url in set(urls))


async def notify(content: str, *urls: AppriseURL) -> None:
    if not urls:
        return

    logger.info("Sending Apprise notifications.. ")
    apprise_obj = apprise.Apprise()
    attach_logs: bool = False
    for webhook in urls:
        if not attach_logs:
            attach_logs = webhook.attach_logs
        _ = apprise_obj.add(str(webhook.url.get_secret_value()), tag=sorted(webhook.tags))

    messages: dict[str, dict[str, str]] = {
        "no_logs": _DEFAULT_MSG | {"body": content},
        "attach_logs": _DEFAULT_MSG | {"body": content},
        "simplified": _DEFAULT_MSG,
    }

    async def notify() -> None:
        with borrow_logger("apprise", level=logging.INFO):
            _ = await asyncio.gather(*(apprise_obj.async_notify(**msg, tag=tag) for tag, msg in messages.items()))

    if not attach_logs:
        await notify()
        return

    async with _temp_copy_of_main_log() as file:
        messages["attach_logs"]["attach"] = str(file)

        await notify()


@contextlib.asynccontextmanager
async def _temp_copy_of_main_log() -> AsyncGenerator[Path | None]:
    async with aio.temp_dir() as temp_dir:
        temp_file = temp_dir / MAIN_LOG_FILE.get().name
        content = export_logs(size_limit=25 * 1e6)
        _ = await aio.write_bytes(temp_file, content)
        yield temp_file
