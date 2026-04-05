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
        if self:
            return str(self)
        return ""

    def __str__(self) -> str:
        return f"[{_COLOR}]... and {self.count:,} other {self.unit}{'s' if self.count > 1 else ''}"


class OverflowPanel:
    unit: ClassVar[str]

    def __init__(self, *columns: ProgressColumn | str, max_rows: int, expand: bool = True) -> None:
        self.max_rows: int = max_rows
        self._progress: Final[DictProgress] = DictProgress(*columns, expand=expand)
        self._overflow: Final[OverFlow] = OverFlow(self.unit)
        self._invisible_queue: Final[deque[TaskID]] = deque()
        self._visible_tasks: int = 0
        self._panel: Final[Panel] = Panel(
            Group(self._progress, self._overflow),
            title=type(self).__name__.removesuffix("Panel"),
            border_style="green",
            padding=(1, 1),
        )

    @final
    def __rich__(self) -> Panel:
        self._overflow.count = len(self._progress) - self._visible_tasks
        return self._panel

    @final
    def _add_task(self, description: object, total: float | None = None, /, *, completed: int = 0) -> Task:
        visible = self._visible_tasks < self.max_rows
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
            self._visible_tasks -= 1
            try:
                self._push_one_invisible()
            except IndexError:
                pass

    @final
    def _push_one_invisible(self) -> None:
        while True:
            invisible_task_id = self._invisible_queue.popleft()
            try:
                self._progress.update(invisible_task_id, visible=True)
            except KeyError:
                continue
            else:
                self._visible_tasks += 1
                break
