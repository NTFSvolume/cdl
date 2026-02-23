from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from rich.console import Group
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    SpinnerColumn,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from cyberdrop_dl.progress._common import ProgressHook, ProgressProxy

if TYPE_CHECKING:
    from yarl import URL

_COLOR: str = "plum3"


class OverFlow(ProgressProxy):
    _desc: ClassVar[str] = "[{color}]... and {number:,} other {name}"
    _columns = ("[progress.description]{task.description}",)

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name: str = name
        self._count: int = 0
        self._task_id: TaskID = self._progress.add_task(str(self), visible=False)

    def __str__(self) -> str:
        return self._desc.format(color=_COLOR, number=self._count, name=self.name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(desc={self!s})"

    def update(self, count: int) -> None:
        self._count = count
        self._progress.update(self._task_id, description=str(self), visible=count > 0)


class UIPanel(ProgressProxy):
    unit: ClassVar[str]
    _desc_fmt: ClassVar[str] = "[{color}]{description}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(progress={self._progress!r})"

    def __init__(self, visible_tasks_limit: int) -> None:
        super().__init__()
        self.title = type(self).__name__.removesuffix("Panel")
        self._overflow = OverFlow(self.unit)
        self._limit = visible_tasks_limit
        self._panel = Panel(
            Group(self._progress, self._overflow),
            title=self.title,
            border_style="green",
            padding=(1, 1),
        )

    def __rich__(self) -> Panel:
        return self._panel

    def _redraw(self) -> None:
        self._overflow.update(count=len(self._tasks) - self._limit)

    def add_task(self, description: str, total: float | None = None) -> TaskID:
        task_id = self._progress.add_task(
            self._desc_fmt.format(color=_COLOR, description=description),
            total=total,
            visible=len(self._tasks) < self._limit,
        )
        self._redraw()
        return task_id

    def remove_task(self, task_id: TaskID) -> None:
        self._progress.remove_task(task_id)
        self._redraw()

    def new_hook(self, description: object, total: float | None = None) -> ProgressHook:
        task_id = self.add_task(str(description), total)

        def advance(amount: int) -> None:
            self._advance(task_id, amount)

        def done() -> None:
            self.remove_task(task_id)

        def speed() -> float:
            return self.get_speed(task_id)

        return ProgressHook(advance, done, speed)

    def _advance(self, task_id: TaskID, amount: int) -> None:
        self._progress.advance(task_id, amount)

    def get_speed(self, task_id: TaskID) -> float:
        task = self._tasks[task_id]
        return task.finished_speed or task.speed or 0


class ScrapingPanel(UIPanel):
    unit: ClassVar[str] = "URLs"
    _columns = SpinnerColumn(), "[progress.description]{task.description}"

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=5)

    def new_task(self, url: URL) -> TaskID:  # type: ignore[reportIncompatibleMethodOverride]
        return self.add_task(str(url))


class DownloadsPanel(UIPanel):
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

    def new_task(self, *, domain: str, filename: str, expected_size: int | None = None) -> TaskID:  # type: ignore[reportIncompatibleMethodOverride]
        description = self._clean_task_desc(filename.rsplit("/", 1)[-1])
        return self.add_task(description, expected_size)

    def _advance(self, task_id: TaskID, amount: int) -> None:
        self.total_data_written += amount
        super()._advance(task_id, amount)

    def advance_file(self, task_id: TaskID, amount: int) -> None:
        self._advance(task_id, amount)
