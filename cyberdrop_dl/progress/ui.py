from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar

from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

if TYPE_CHECKING:
    from yarl import URL

_COLOR: str = "plum3"


def truncate(s: str, length: int = 40, placeholder: str = "...") -> str:
    return f"{s[: length - len(placeholder)]}{placeholder}" if len(s) >= length else s.ljust(length)


class OverFlow:
    _desc: ClassVar[str] = "[{color}]... and {number:,} other {name}"

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.progress: Progress = Progress("[progress.description]{task.description}")
        self._task_id = self.progress.add_task(self._format(count=0), visible=False)

    def _format(self, count: int) -> str:
        return self._desc.format(color=_COLOR, number=count, name=self.name)

    def update(self, count: int) -> None:
        self.progress.update(self._task_id, description=self._format(count=count), visible=count > 0)


class UIPanel:
    title: ClassVar[str]
    type_str: ClassVar[str] = "files"
    desc_fmt: ClassVar[str] = "[{color}]{description}"

    def __init__(self, progress: Progress, visible_tasks_limit: int) -> None:
        self._progress = progress
        self._overflow = OverFlow(self.type_str)
        self._limit = visible_tasks_limit
        self._tasks = MappingProxyType(self._progress._tasks)

    @classmethod
    def _clean_task_desc(cls, desc: str) -> str:
        return escape(truncate(desc.encode("ascii", "ignore").decode().strip(), length=40))

    def __rich__(self) -> Panel:
        return self.get_renderable()

    def get_renderable(self) -> Panel:
        return Panel(
            Group(self._progress, self._overflow.progress),
            title=self.title,
            border_style="green",
            padding=(1, 1),
        )

    def add_task(self, description: str, total: float | None = None) -> TaskID:
        task_id = self._progress.add_task(
            self.desc_fmt.format(color=_COLOR, description=description),
            total=total,
            visible=len(self._tasks) < self._limit,
        )
        self.redraw()
        return task_id

    def remove_task(self, task_id: TaskID) -> None:
        self._progress.remove_task(task_id)
        self.redraw()

    def redraw(self) -> None:
        self._overflow.update(count=len(self._tasks) - self._limit)


class ScrapingPanel(UIPanel):
    title: ClassVar[str] = "Scraping"
    type_str: ClassVar[str] = "URLs"

    def __init__(self) -> None:
        progress = Progress(SpinnerColumn(), "[progress.description]{task.description}")
        super().__init__(progress, visible_tasks_limit=5)

    def new_task(self, url: URL) -> TaskID:  # type: ignore[reportIncompatibleMethodOverride]
        return self.add_task(str(url))


class DownloadsPanel(UIPanel):
    title: ClassVar[str] = "Downloads"
    _base_columns = (SpinnerColumn(), "[progress.description]{task.description}", BarColumn(bar_width=None))
    _horizontal = (
        *_base_columns,
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        DownloadColumn(),
        "━",
        TransferSpeedColumn(),
        "━",
        TimeRemainingColumn(),
    )
    _vertical = (*_base_columns, DownloadColumn(), "━", TransferSpeedColumn())

    def __init__(self) -> None:
        self.total_data_written: int = 0
        progress = Progress(*self._vertical) if True else Progress(*self._horizontal)
        super().__init__(progress, visible_tasks_limit=10)

    def new_task(self, *, domain: str, filename: str, expected_size: int | None = None) -> TaskID:  # type: ignore[reportIncompatibleMethodOverride]
        description = self._clean_task_desc(filename.split("/")[-1])
        if not True:
            description = f"({domain.upper()}) {description}"

        return super().add_task(description, expected_size)

    def advance_file(self, task_id: TaskID, amount: int) -> None:
        self.total_data_written += amount
        self._progress.advance(task_id, amount)

    def get_speed(self, task_id: TaskID) -> float:
        task = self._tasks[task_id]
        return task.finished_speed or task.speed or 0
