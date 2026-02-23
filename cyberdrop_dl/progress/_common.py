from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from rich.console import RenderableType
    from rich.progress import TaskID


from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar

from rich.markup import escape
from rich.progress import (
    Progress,
    ProgressColumn,
    Task,
    TaskID,
)


def truncate(s: str, length: int = 40, placeholder: str = "...") -> str:
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

    def __enter__(self) -> Callable[[int], None]:
        return self.advance

    def __exit__(self, *_) -> None:
        self.done()


class ProgressProxy:
    _columns: ClassVar[tuple[ProgressColumn | str, ...]]

    @classmethod
    def _clean_task_desc(cls, desc: str) -> str:
        return escape(truncate(desc.encode("ascii", "ignore").decode().strip()))

    def __init__(self) -> None:
        self._progress: Progress = Progress(*self._columns)
        self._tasks: MappingProxyType[TaskID, Task] = MappingProxyType(self._progress._tasks)
        self._tasks_map: dict[str, TaskCounter] = {}

    def __rich__(self) -> RenderableType:
        return self._progress
