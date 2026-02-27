from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import shutil
import sys
import tempfile
from collections.abc import Generator
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final

import apprise
import rich
from pydantic import ValidationError

from cyberdrop_dl import constants
from cyberdrop_dl.logger import enable_3p_logger, log, log_debug, log_spacer
from cyberdrop_dl.models import AppriseURLModel, format_validation_error

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Iterable


logger = logging.getLogger(__name__)

_DEFAULT_MSG: Final = {
    "body": "Finished downloading. Enjoy :)",
    "title": "Cyberdrop-DL",
    "body_format": apprise.NotifyFormat.TEXT,
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


class LogLevel(IntEnum):
    NOTSET = 0
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


_LOG_LEVEL_NAMES = tuple(x.name for x in LogLevel)


@dataclasses.dataclass(slots=True, order=True)
class LogLine:
    level: LogLevel = LogLevel.INFO
    msg: str = ""


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


def _process_results(
    all_urls: list[str], results: dict[str, bool | None], apprise_logs: str
) -> tuple[constants.NotificationResult, list[LogLine]]:
    result = [r for r in results.values() if r is not None]
    result_dict = {}
    for key, value in results.items():
        if value:
            result_dict[key] = str(constants.NotificationResult.SUCCESS.value)
        elif value is None:
            result_dict[key] = str(constants.NotificationResult.NONE.value)
        else:
            result_dict[key] = str(constants.NotificationResult.FAILED.value)

    if all(result):
        final_result = constants.NotificationResult.SUCCESS
    elif any(result):
        final_result = constants.NotificationResult.PARTIAL
    else:
        final_result = constants.NotificationResult.FAILED

    log_spacer(10, log_to_console=False, log_to_file=not all(result))
    rich.print("Apprise notifications results:", final_result.value)
    logger = log_debug if all(result) else log
    logger(f"Apprise notifications results: {final_result.value}")
    logger(f"PARSED_APPRISE_URLs: \n{json.dumps(all_urls, indent=4)}\n")
    logger(f"RESULTS_BY_TAGS: \n{json.dumps(result_dict, indent=4)}")
    log_spacer(10, log_to_console=False, log_to_file=not all(result))
    parsed_log_lines = _parse_apprise_logs(apprise_logs)
    for line in parsed_log_lines:
        logger(level=line.level.value, message=line.msg)
    return final_result, parsed_log_lines


def _reduce_logs(apprise_logs: str) -> list[str]:
    lines = apprise_logs.splitlines()
    to_exclude = ["Running Post-Download Processes For Config"]
    return [line for line in lines if all(word not in line for word in to_exclude)]


def _parse_apprise_logs(apprise_logs: str) -> list[LogLine]:
    lines = _reduce_logs(apprise_logs)
    current_line: LogLine = LogLine()
    parsed_lines: list[LogLine] = []
    for line in lines:
        log_level = line[0:8].strip()
        if log_level and log_level not in _LOG_LEVEL_NAMES:  # pragma: no cover
            current_line.msg += f"\n{line}"
            continue

        if current_line.msg != "":
            parsed_lines.append(current_line)
        current_line = LogLine(LogLevel[log_level], line[10::])
    if lines:
        parsed_lines.append(current_line)
    return parsed_lines


async def send_notifications(text: str, *apprise_urls: AppriseURL, attachment: Path) -> None:
    if not apprise_urls:
        return

    logger.info("Sending Apprise notifications.. ")
    apprise_obj = apprise.Apprise()
    send_logs: bool = False
    for apprise_url in apprise_urls:
        if not send_logs:
            send_logs = "attach_logs" in apprise_url.tags
        _ = apprise_obj.add(apprise_url.url, tag=sorted(apprise_url.tags))  # pyright: ignore[reportUnknownMemberType]

    messages: dict[str, dict[str, str]] = {
        "no_logs": _DEFAULT_MSG | {"body": text},
        "attach_logs": _DEFAULT_MSG | {"body": text},
        "simplified": _DEFAULT_MSG,
    }
    with enable_3p_logger("apprise", level=logging.INFO):
        async with _temp_copy(send_logs, attachment) as file:
            if file:
                messages["attach_logs"]["attach"] = str(file)

            _ = await asyncio.gather(*(apprise_obj.async_notify(**msg, tag=tag) for tag, msg in messages.items()))


@contextlib.asynccontextmanager
async def _temp_copy(send_logs: bool, attachment: Path) -> AsyncGenerator[Path | None]:
    if not send_logs:
        yield
        return

    with tempfile.TemporaryDirectory(prefix="cdl_", ignore_cleanup_errors=True) as temp_dir:
        file = Path(temp_dir) / attachment.name
        try:
            _ = await asyncio.to_thread(shutil.copy, attachment, file)
        except OSError:
            logger.error("Unable to get copy of the main log file. 'attach_logs' URLs will be processed without it")
            yield
        else:
            yield await asyncio.to_thread(file.resolve)
