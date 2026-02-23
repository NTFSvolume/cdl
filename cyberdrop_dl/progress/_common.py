from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.progress import TaskID


@dataclasses.dataclass(slots=True)
class TaskCounter:
    id: TaskID
    count: int = 0


class TasksMap(dict[str, TaskCounter]):
    def __getattr__(self, name: str, /) -> TaskCounter:
        return self[name]
