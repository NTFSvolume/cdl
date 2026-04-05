from __future__ import annotations

import asyncio
import dataclasses
import functools
import random
from types import MappingProxyType
from typing import TYPE_CHECKING, Self

import rich
from rich.panel import Panel
from rich.progress import BarColumn, TaskID

from cyberdrop_dl.progress import DictProgress, create_live

if TYPE_CHECKING:
    from collections.abc import Iterator


@functools.cache
def _pretty_format(error: str) -> str:
    return _ERROR_OVERRIDES.get(error) or _capitalize_words(error)


def _capitalize_words(text: str) -> str:
    """Capitalize first letter of each word

    Unlike `str.title()`, this caps the first letter of each word without modifying the rest of the word"""

    def cap(word: str) -> str:
        return word[0].capitalize() + word[1:]

    return " ".join([cap(word) for word in text.split()])


@dataclasses.dataclass(slots=True, order=True)
class Error:
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


class _ErrorsPanel:
    """Base class that keeps track of errors and reasons."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}(error_count={self._total!r}, errors={tuple(self._errors_map)!r})"

    def __init__(self) -> None:
        self._progress: DictProgress = DictProgress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>6.2f}%",
            "━",
            "{task.completed:,}",
        )

        self._errors_map: dict[str, TaskID] = {}
        self._total: int = 0
        self._changed: bool = False
        self._panel: Panel = Panel(
            self._progress,
            title=type(self).__name__.removesuffix("Errors") + " Errors",
            border_style="green",
        )

    def __rich__(self) -> Panel:
        if self._changed:
            self._sort_tasks()
            self._changed = False

        self._panel.subtitle = f"Total: [white]{self._total:,}"
        return self._panel

    def add(self, error: str) -> None:
        self._total += 1
        name = _pretty_format(error)
        if (task_id := self._errors_map.get(name)) is not None:
            self._progress.advance(task_id)
        else:
            self._errors_map[name] = self._progress.add_task(name, total=self._total, completed=1)
        self._changed = True

    def _sort_tasks(self) -> None:
        for task_id in self._errors_map.values():
            self._progress.update(task_id, total=self._total)

        self._progress.sort_tasks(
            lambda tasks: sorted(tasks, key=lambda x: x.completed, reverse=True),
        )

    def __iter__(self) -> Iterator[Error]:
        tasks = {task.id: task for task in self._progress.tasks}
        return iter((Error.parse(msg, int(tasks[task_id].completed)) for msg, task_id in self._errors_map.items()))

    async def simulate(self) -> None:
        self.add("404 not found")
        for error in random.choices(tuple(_ERROR_OVERRIDES), k=40):
            self.add(error)
            await asyncio.sleep(random.random() * 5)


class DownloadErrors(_ErrorsPanel): ...


class ScrapeErrors(_ErrorsPanel):
    def __init__(self) -> None:
        super().__init__()
        self._unsupported: int = 0
        self.sent_to_jdownloader: int = 0
        self.skipped: int = 0

    def add_unsupported(self, *, sent_to_jdownloader: bool = False) -> None:
        self._unsupported += 1
        if sent_to_jdownloader:
            self.sent_to_jdownloader += 1
        else:
            self.skipped += 1


_ERROR_OVERRIDES = MappingProxyType(
    {
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
)


if __name__ == "__main__":
    panel = DownloadErrors()
    with create_live(panel, transient=True):
        asyncio.run(panel.simulate())
        rich.print(sorted(panel))
