from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import aiohttp
from aiohttp import FormData

from cyberdrop_dl.utils.logger import MAIN_LOG_FILE, export_logs, log_spacer

if TYPE_CHECKING:
    from cyberdrop_dl.models import AppriseURL

    pass

logger = logging.getLogger(__name__)


async def _prepare_form(content: str, *, attach_logs: bool) -> FormData:
    form = FormData()
    if attach_logs:
        try:
            logs = await asyncio.to_thread(export_logs, size_limit=25 * 1e6)
        except Exception:
            logger.exception("Unable to attach log for webhook notification")
        else:
            form.add_field("file", logs, filename=MAIN_LOG_FILE.get().name)

    form.add_fields(
        ("content", content),
        ("username", "cyberdrop-dl"),
    )
    return form


async def send_webhook_message(content: str, webhook: AppriseURL) -> None:

    logger.info("Sending webhook notifications.. ")
    url = str(webhook.url.get_secret_value())
    form = await _prepare_form(content, attach_logs=webhook.attach_logs)

    log_spacer()
    try:
        async with aiohttp.request("POST", url, data=form) as response:
            if response.ok:
                logger.info("Webhook notifications: Success", extra={"color": "green"})
            else:
                try:
                    error: dict[str, Any] = await response.json()
                except Exception:
                    response.raise_for_status()
                    raise
                else:
                    _ = error.pop("content", None)
                    logger.error(f"Webhook notification failed: {error}", extra={"color": "red"})

    except Exception:
        logger.exception("Unable to send webhook notification")
