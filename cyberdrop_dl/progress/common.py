from __future__ import annotations

import dataclasses
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar, Self

from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, TaskID

if TYPE_CHECKING:
    from collections.abc import Callable

    from rich.console import RenderableType
    from rich.progress import ProgressColumn, Task, TaskID

_COLOR: str = "plum3"


def _truncate(s: str, length: int = 40, placeholder: str = "...") -> str:
    return f"{s[: length - len(placeholder)]}{placeholder}" if len(s) >= length else s.ljust(length)


@dataclasses.dataclass(slots=True)
class TaskCounter:
    id: TaskID
    count: int = 0


@dataclasses.dataclass(slots=True, frozen=True)
class ProgressHook:
    advance: Callable[[int], None]
    done: Callable[[], None]
    speed: Callable[[], float]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.done()

    def as_segment(self) -> ProgressHook:
        return ProgressHook(self.advance, lambda: None, self.speed)


@dataclasses.dataclass(slots=True)
class RichProxy:
    _renderable: RenderableType

    def __rich__(self) -> RenderableType:
        return self._renderable


@dataclasses.dataclass(slots=True)
class ProgressProxy(RichProxy):
    _columns: ClassVar[tuple[ProgressColumn | str, ...]]
    _progress: Progress
    _tasks: MappingProxyType[TaskID, Task]
    _tasks_map: dict[str, TaskCounter]

    @classmethod
    def _clean_task_desc(cls, desc: str) -> str:
        return escape(_truncate(desc.encode("ascii", "ignore").decode().strip()))

    def __init__(self) -> None:
        self._progress = Progress(*self._columns)
        self._tasks = MappingProxyType(self._progress._tasks)
        self._tasks_map = {}
        super().__init__(self._progress)


class UIPanel(ProgressProxy):
    _renderable: Panel  # pyright: ignore[reportIncompatibleVariableOverride]


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


class UIOverFlowPanel(UIPanel):
    unit: ClassVar[str]
    _desc_fmt: ClassVar[str] = "[{color}]{description}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(progress={self._progress!r})"

    def __init__(self, visible_tasks_limit: int) -> None:
        super().__init__()
        self.title = type(self).__name__.removesuffix("Panel")
        self._overflow = OverFlow(self.unit)
        self._limit = visible_tasks_limit
        self._renderable = Panel(
            Group(self._progress, self._overflow),
            title=self.title,
            border_style="green",
            padding=(1, 1),
        )

    def _redraw(self) -> None:
        self._overflow.update(count=len(self._tasks) - self._limit)

    def _add_task(self, description: str, total: float | None = None) -> TaskID:
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
        task_id = self._add_task(str(description), total)

        def advance(amount: int) -> None:
            self._advance(task_id, amount)

        def done() -> None:
            self.remove_task(task_id)

        def speed() -> float:
            return self.get_speed(task_id)

        return ProgressHook(advance, done, speed)

    def __call__(self, description: object, total: float | None = None) -> ProgressHook:
        return self.new_hook(description, total)

    def _advance(self, task_id: TaskID, amount: int) -> None:
        self._progress.advance(task_id, amount)

    def get_speed(self, task_id: TaskID) -> float:
        task = self._tasks[task_id]
        return task.finished_speed or task.speed or 0
