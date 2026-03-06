from __future__ import annotations

import asyncio
import dataclasses
import random
from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING, ClassVar, Final, Self, final

from rich import get_console
from rich.console import Group
from rich.live import Live
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, ProgressColumn, Task, TaskID

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from rich.console import RenderableType
    from rich.progress import Task, TaskID

_COLOR: str = "plum3"


def _truncate(s: str, length: int = 40, placeholder: str = "...") -> str:
    return f"{s[: length - len(placeholder)]}{placeholder}" if len(s) >= length else s.ljust(length)


@dataclasses.dataclass(slots=True, order=True)
class TaskCounter:
    id: TaskID
    count: int = 0


class RichProxy(ABC):
    @abstractmethod
    def __rich__(self) -> RenderableType: ...

    def __repr__(self) -> str:
        return f"<{type(self).__name__}(renderable={self.__rich__()!r})>"


class DictProgress(Progress):
    """A progress with a dict like interface to access tasks"""

    def __getitem__(self, task_id: TaskID) -> Task:
        with self._lock:
            return self._tasks[task_id]

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)


@dataclasses.dataclass(slots=True)
class ProgressHook:
    advance: Callable[[int], None]
    get_speed: Callable[[], float]
    done: Callable[[], None]

    _done: bool = dataclasses.field(init=False, default=False)

    @property
    def speed(self) -> float:
        return self.get_speed()

    def __enter__(self) -> Self:
        if self._done:
            raise RuntimeError
        return self

    def __exit__(self, *_) -> None:
        if self._done:
            raise RuntimeError
        self.done()
        self._done = True


ColumnsType = tuple[ProgressColumn | str, ...]


class _OverFlow(RichProxy):
    def __init__(self, unit: str) -> None:
        self._progress: Progress = Progress("[progress.description]{task.description}")
        self.unit: str = unit
        self.total: int = 0
        self._task_id: TaskID = self._progress.add_task(str(self), visible=False)

    def __rich__(self) -> Progress:
        return self._progress

    def __str__(self) -> str:
        return f"[{_COLOR}]... and {self.total:,} other {self.unit}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(unit={self.unit!r}, total={self.total!r}, desc={self!s})"

    def update(self, count: int) -> None:
        self.total = count
        self._progress.update(self._task_id, description=str(self), visible=count > 0)


class UIComponent(RichProxy, ABC):
    """A section of the TUI."""

    columns: ClassVar[ColumnsType]

    def __init__(self) -> None:
        self._progress: Final[DictProgress] = DictProgress(*self.columns, expand=True)
        self._counters: Final[dict[str, TaskCounter]] = {}

    @final
    @classmethod
    def _escape(cls, desc: str) -> str:
        return escape(desc)
        # return escape(_truncate(desc.encode("ascii", "ignore").decode().strip()))

    def _clean_task_description(self, description: object, /) -> object:
        return description

    def _increase_counter(self, task_name: str) -> None:
        task_counter = self._counters[task_name]
        task_counter.count += 1
        self._progress.advance(task_counter.id)


class OverflowPanel(UIComponent):
    unit: ClassVar[str]

    def __init__(self, visible_tasks_limit: int) -> None:
        super().__init__()
        self._title: Final[str] = type(self).__name__.removesuffix("Panel")
        self._overflow: Final[_OverFlow] = _OverFlow(self.unit)
        self._limit: Final[int] = visible_tasks_limit
        self._invisible_queue: Final[deque[TaskID]] = deque()
        self._visible_tasks: int = 0
        self._total_amount: int = 0

        self._panel: Panel = Panel(
            Group(self._progress, self._overflow),
            title=self._title,
            border_style="green",
            padding=(1, 1),
        )

    def __rich__(self) -> RenderableType:
        return self._panel

    @final
    def _update_overflow(self) -> None:
        self._overflow.update(count=len(self._progress) - self._visible_tasks)

    @final
    def new_task(self, description: object, /, total: float | None = None) -> ProgressHook:
        task = self._add_task(description, total)

        def advance(amount: int = 1) -> None:
            self._total_amount += amount
            self._progress.advance(task.id, amount)

        def on_exit() -> None:
            self._remove_task(task)

        def get_speed() -> float:
            return task.finished_speed or task.speed or 0

        return ProgressHook(advance, get_speed, on_exit)

    @final
    def _add_task(self, description: object, total: float | None = None, /, *, completed: int = 0) -> Task:
        visible = self._visible_tasks < self._limit
        task_id = self._progress.add_task(
            f"[{_COLOR}]{self._clean_task_description(description)}",
            total=total,
            visible=visible,
            completed=completed,
        )
        if visible:
            self._visible_tasks += 1
        else:
            self._invisible_queue.append(task_id)

        self._update_overflow()
        return self._progress[task_id]

    @final
    def _remove_task(self, task: Task) -> None:
        was_visible = task.visible
        self._progress.remove_task(task.id)
        if was_visible:
            while True:
                try:
                    invisible_task_id = self._invisible_queue.popleft()
                except IndexError:
                    self._visible_tasks -= 1
                    break

                try:
                    self._progress.update(invisible_task_id, visible=True)
                except KeyError:
                    continue
                else:
                    break

        self._update_overflow()


# THESE ARE JUST FOR TESTING


class Random:
    choice = random.choice
    choices = random.choices

    @staticmethod
    def float(start: float, end: float) -> float:
        return random.uniform(start, end)

    @staticmethod
    def int(start: float = 0.0, end: float = 1e12) -> int:
        return random.randint(int(start), int(end))

    @staticmethod
    def int_until(target: int, min_step: float, max_step: float) -> Generator[int, None, None]:
        total = 0
        while total < target:
            new = min(random.randint(int(min_step), int(max_step)), target - total)
            yield new
            total += new

    @staticmethod
    def sleep(delay: float = 0.1):
        return asyncio.sleep(delay)


def create_live(renderable: RichProxy) -> Live:
    return Live(
        console=get_console(),
        auto_refresh=True,
        refresh_per_second=20,
        transient=False,
        get_renderable=renderable.__rich__,
    )
