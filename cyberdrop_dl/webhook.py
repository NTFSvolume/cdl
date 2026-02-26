from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp.formdata import FormData

from cyberdrop_dl import config, constants
from cyberdrop_dl.logger import spacer
from cyberdrop_dl.utils import aio

if TYPE_CHECKING:
    from pathlib import Path

    import aiohttp

    from cyberdrop_dl.models._base import AppriseURLModel


logger = logging.getLogger(__name__)
_DEFAULT_DIFF_LINE_FORMAT: str = "{}"
_STYLE_TO_DIFF_MAP = {
    "green": "+   {}",
    "red": "-   {}",
    "yellow": "*** {}",
}


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


async def send_notification(session: aiohttp.ClientSession, webhook: AppriseURLModel | None = None) -> None:
    """Outputs the stats to a code block for webhook messages."""

    if not webhook:
        return

    logger.info("Sending webhook notifications.. ")
    url = webhook.url.get_secret_value()
    form = await _prepare_form(config.get().logs.main_log if "attach_logs" in webhook.tags else None)

    try:
        async with session.post(url, data=form) as response:
            if response.ok:
                result = "Success"

            else:
                json_resp: dict[str, Any] = await response.json()
                json_resp.pop("content", None)
                resp_text = json.dumps(json_resp, indent=4, ensure_ascii=False)
                result = f"Failed \n{resp_text}"

    except Exception:
        logger.exception("Unable to send webhook notification")
    else:
        logger.info(spacer())
        logger.info(f"Webhook notifications results: {result}")
