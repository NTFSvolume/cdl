from __future__ import annotations

import dataclasses
import functools

from rich.panel import Panel
from rich.progress import BarColumn, TaskID

from cyberdrop_dl.progress.common import ProgressProxy


@dataclasses.dataclass(slots=True, order=True)
class UIFailure:
    full_msg: str
    total: int
    error_code: int | None = None
    msg: str = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        parts = self.full_msg.split(" ", 1)
        if len(parts) > 1 and parts[0].isdigit():
            error_code, self.msg = parts
            self.error_code = int(error_code)
        else:
            self.msg = self.full_msg


class _ErrorsPanel(ProgressProxy):
    """Base class that keeps track of errors and reasons."""

    _columns = (
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        "{task.completed:,}",
    )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(failed_files={self.failed_files!r}, failures={self._failures.keys()!r})"

    def __init__(self) -> None:
        super().__init__()
        self.title = type(self).__name__.removesuffix("Errors") + " Failures"
        self._failures: dict[str, TaskID] = {}
        self.failed_files: int = 0
        self._renderable: Panel = Panel(  # pyright: ignore[reportIncompatibleVariableOverride]
            self._progress,
            title=self.title,
            border_style="green",
            padding=(1, 1),
            subtitle=self._subtitle,
        )

    @property
    def _subtitle(self) -> str:
        return f"Total {self.title}: [white]{self.failed_files:,}"

    def add_failure(self, failure: str) -> None:
        self.failed_files += 1
        key = _get_pretty_error(failure)
        if (task_id := self._failures.get(key)) is not None:
            self._progress.advance(task_id)
        else:
            self._failures[key] = self._progress.add_task(key, total=self.failed_files, completed=1)

        self._redraw()

    def _redraw(self) -> None:
        self._renderable.subtitle = self._subtitle
        for task_id in self._failures.values():
            self._progress.update(task_id, total=self.failed_files)

        tasks = list(self._tasks.values())
        tasks_sorted = sorted(tasks, key=lambda x: x.completed, reverse=True)
        if tasks == tasks_sorted:
            return

        for task in tasks_sorted:
            self._progress.remove_task(task.id)
            self._failures[task.description] = self._progress.add_task(
                task.description,
                total=task.total,
                completed=int(task.completed),
            )

    def return_totals(self) -> list[UIFailure]:
        """Returns the total number of failed sites and reasons."""

        return sorted(UIFailure(msg, int(self._tasks[task_id].completed)) for msg, task_id in self._failures.items())


class DownloadErrors(_ErrorsPanel):
    """Class that keeps track of download failures and reasons."""


class ScrapeErrors(_ErrorsPanel):
    """Class that keeps track of scraping failures and reasons."""

    def __init__(self) -> None:
        super().__init__()
        self.unsupported_urls: int = 0
        self.sent_to_jdownloader: int = 0
        self.unsupported_urls_skipped: int = 0

    def add_unsupported(self, *, sent_to_jdownloader: bool = False) -> None:
        self.unsupported_urls += 1
        if sent_to_jdownloader:
            self.sent_to_jdownloader += 1
        else:
            self.unsupported_urls_skipped += 1


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
