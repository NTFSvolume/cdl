from __future__ import annotations

import dataclasses
import functools
from typing import ClassVar, Self

from rich.panel import Panel
from rich.progress import BarColumn, TaskID

from cyberdrop_dl.tui.common import ColumnsType, UIPanel


@dataclasses.dataclass(slots=True, order=True)
class UIFailure:
    msg: str
    count: int
    code: int | None = None

    @classmethod
    def parse(cls, msg: str, count: int) -> Self:
        if len(parts := msg.split(" ", 1)) == 2:
            error_code, msg = parts
            try:
                return cls(msg, count, int(error_code))
            except ValueError:
                pass

        return cls(msg, count)


class _ErrorsPanel(UIPanel):
    """Base class that keeps track of errors and reasons."""

    columns: ClassVar[ColumnsType] = (
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        "{task.completed:,}",
    )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(error_count={self._error_count!r}, errors={tuple(self._errors.keys())!r})"

    def __init__(self) -> None:
        super().__init__()
        self._title: str = type(self).__name__.removesuffix("Errors") + " Failures"
        self._errors: dict[str, TaskID] = {}
        self._error_count: int = 0
        self._panel: Panel = Panel(
            self._progress,
            title=self._title,
            border_style="green",
            padding=(1, 1),
            subtitle=self._subtitle,
        )

    @property
    def _subtitle(self) -> str:
        return f"Total {self._title}: [white]{self._error_count:,}"

    def add(self, error: str) -> None:
        self._error_count += 1
        name = _get_pretty_error(error)
        if (task_id := self._errors.get(name)) is not None:
            self._progress.advance(task_id)
        else:
            self._errors[name] = self._progress.add_task(name, total=self._error_count, completed=1)

        self._redraw()

    def _redraw(self) -> None:
        self._panel.subtitle = self._subtitle
        for task_id in self._errors.values():
            self._progress.update(task_id, total=self._error_count)

        tasks = list(self._tasks.values())
        tasks_sorted = sorted(tasks, key=lambda x: x.completed, reverse=True)
        if tasks == tasks_sorted:
            return

        for task in tasks_sorted:
            self._progress.remove_task(task.id)
            self._errors[task.description] = self._progress.add_task(
                task.description,
                total=task.total,
                completed=int(task.completed),
            )

    def results(self) -> list[UIFailure]:
        return sorted(
            UIFailure.parse(msg, int(self._tasks[task_id].completed)) for msg, task_id in self._errors.items()
        )


class DownloadErrors(_ErrorsPanel):
    """Class that keeps track of download failures and reasons."""


class ScrapeErrors(_ErrorsPanel):
    """Class that keeps track of scraping failures and reasons."""

    def __init__(self) -> None:
        super().__init__()
        self._unsupported: int = 0
        self._sent_to_jdownloader: int = 0
        self._skipped: int = 0

    def add_unsupported(self, *, sent_to_jdownloader: bool = False) -> None:
        self._unsupported += 1
        if sent_to_jdownloader:
            self._sent_to_jdownloader += 1
        else:
            self._skipped += 1


@functools.cache
def _get_pretty_error(failure: str) -> str:
    return _FAILURE_OVERRIDES.get(failure) or _capitalize_words(failure)


def _capitalize_words(text: str) -> str:
    """Capitalize first letter of each word

    Unlike `str.capwords()`, this only caps the first letter of each word without modifying the rest of the word"""

    def cap(word: str) -> str:
        return word[0].capitalize() + word[1:]

    return " ".join([cap(word) for word in text.split()])


_FAILURE_OVERRIDES = {
    "ClientConnectorCertificateError": "Client Connector Certificate Error",
    "ClientConnectorDNSError": "Client Connector DNS Error",
    "ClientConnectorError": "Client Connector Error",
    "ClientConnectorSSLError": "Client Connector SSL Error",
    "ClientHttpProxyError": "Client HTTP Proxy Error",
    "ClientPayloadError": "Client Payload Error",
    "ClientProxyConnectionError": "Client Proxy Connection Error",
    "ConnectionTimeoutError": "Connection Timeout",
    "ContentTypeError": "Content Type Error",
    "InvalidURL": "Invalid URL",
    "InvalidUrlClientError": "Invalid URL Client Error",
    "InvalidUrlRedirectClientError": "Invalid URL Redirect",
    "NonHttpUrlRedirectClientError": "Non HTTP URL Redirect",
    "RedirectClientError": "Redirect Error",
    "ServerConnectionError": "Server Connection Error",
    "ServerDisconnectedError": "Server Disconnected",
    "ServerFingerprintMismatch": "Server Fingerprint Mismatch",
    "ServerTimeoutError": "Server Timeout Error",
    "SocketTimeoutError": "Socket Timeout Error",
}
