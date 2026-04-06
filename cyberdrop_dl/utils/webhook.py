from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import aiohttp

from cyberdrop_dl.utils.logger import MAIN_LOG_FILE, export_logs, log_spacer

if TYPE_CHECKING:
    import yarl

    from cyberdrop_dl.models import AppriseURL


logger = logging.getLogger(__name__)


async def send_webhook_notification(webhook: AppriseURL, content: str | None = None) -> None:
    log_spacer()
    url, form = await _prepare_webhook(webhook)
    if content:
        form.add_field("content", content)

    await _send_webhook(url, form)


async def _prepare_webhook(webhook: AppriseURL) -> tuple[str, aiohttp.FormData]:
    url = str(webhook.url.get_secret_value())
    logs_content = None
    if webhook.attach_logs:
        try:
            logs_content = await asyncio.to_thread(export_logs, size_limit=25 * 1e6)
        except Exception:
            logger.exception("Unable to attach log for webhook notification")

    form = aiohttp.FormData()
    if logs_content is not None:
        form.add_field("file", logs_content, filename=MAIN_LOG_FILE.get().name)

    form.add_field("username", "cyberdrop-dl")
    return url, form


async def _send_webhook(url: yarl.URL | str, form: aiohttp.FormData) -> None:
    logger.info("Sending webhook notifications.. ")
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
