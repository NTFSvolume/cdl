from __future__ import annotations

import dataclasses
from collections import deque
from typing import TYPE_CHECKING, ClassVar, Final, final

from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Task, TaskID

from cyberdrop_dl.progress import DictProgress

if TYPE_CHECKING:
    from rich.progress import ProgressColumn, Task, TaskID

_COLOR: str = "plum3"


@final
@dataclasses.dataclass(slots=True)
class OverFlow:
    unit: str
    count: int = 0

    def __bool__(self) -> bool:
        return self.count > 0

    def __rich__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return f"[{_COLOR}]... and {self.count:,} other {self.unit}"


class OverflowPanel:
    unit: ClassVar[str]

    def __init__(self, *columns: ProgressColumn | str, visible_tasks_limit: int, expand: bool = True) -> None:
        self._progress: Final[DictProgress] = DictProgress(*columns, expand=expand)
        self._overflow: Final[OverFlow] = OverFlow(self.unit)
        self._limit: Final[int] = visible_tasks_limit
        self._invisible_queue: Final[deque[TaskID]] = deque()
        self._visible_tasks: int = 0
        self._panel: Final[Panel] = Panel(
            self._progress,
            title=type(self).__name__.removesuffix("Panel"),
            border_style="green",
            padding=(1, 1),
        )

    def __rich__(self) -> Panel:
        self._overflow.count = len(self._progress) - self._visible_tasks
        self._panel.renderable = self._progress if not self._overflow else Group(self._progress, self._overflow)
        return self._panel

    @final
    def _add_task(self, description: object, total: float | None = None, /, *, completed: int = 0) -> Task:
        visible = self._visible_tasks < self._limit
        task_id = self._progress.add_task(
            f"[{_COLOR}]{escape(str(description))}",
            total=total,
            visible=visible,
            completed=completed,
        )
        if visible:
            self._visible_tasks += 1
        else:
            self._invisible_queue.append(task_id)

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
