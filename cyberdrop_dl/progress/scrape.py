from __future__ import annotations

import contextlib
from contextvars import ContextVar
from typing import TYPE_CHECKING, ClassVar

from rich.columns import Columns
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from cyberdrop_dl import __version__
from cyberdrop_dl.progress.common import ProgressHook, ProgressProxy, UIOverFlowPanel

if TYPE_CHECKING:
    from collections.abc import Generator

    from yarl import URL


_downloads: ContextVar[ProgressHook] = ContextVar("_downloads")


class ScrapingPanel(UIOverFlowPanel):
    unit: ClassVar[str] = "URLs"
    _columns = SpinnerColumn(), "[progress.description]{task.description}"

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=5)

    def new_task(self, url: URL) -> TaskID:  # type: ignore[reportIncompatibleMethodOverride]
        return self._add_task(str(url))


class DownloadsPanel(UIOverFlowPanel):
    unit: ClassVar[str] = "files"
    _columns = (
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        DownloadColumn(),
        "━",
        TransferSpeedColumn(),
        "━",
        TimeRemainingColumn(),
    )

    def __init__(self) -> None:
        self.total_data_written: int = 0
        super().__init__(visible_tasks_limit=10)

    @property
    def current_hook(self) -> ProgressHook:
        return _downloads.get()

    def new(self, filename: str, size: float | None = None) -> ProgressHook:
        description = self._clean_task_desc(str(filename).rsplit("/", 1)[-1])
        hook = self.new_hook(description, size)
        _ = _downloads.set(hook)
        return hook

    def _advance(self, task_id: TaskID, amount: int) -> None:
        self.total_data_written += amount
        super()._advance(task_id, amount)

    def advance_file(self, task_id: TaskID, amount: int) -> None:
        self._advance(task_id, amount)


class StatusMessage(ProgressProxy):
    _columns = (
        SpinnerColumn(style="green", spinner_name="dots"),
        "[progress.description]{task.description}",
    )

    def __init__(self) -> None:
        super().__init__()
        self.activity = Progress(*self._columns)
        _ = self.activity.add_task(f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0)
        self._task_id = self._progress.add_task("", total=100, completed=0, visible=False)
        self._renderable = Columns([self.activity, self._progress])

    def _update(self, description: str | None = None) -> None:
        self._progress.update(self._task_id, description=description, visible=bool(description))

    def __str__(self) -> str:
        return self._tasks[self._task_id].description

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self!s})"

    @contextlib.contextmanager
    def show(self, msg: str | None) -> Generator[None]:
        try:
            self._update(msg)
            yield
        finally:
            self._update()
