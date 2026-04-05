from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

from rich.live import Live
from rich.markup import escape
from rich.progress import Progress, ProgressColumn, Task, TaskID
from rich.text import Text

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from rich.console import RenderableType


def create_live(renderable: RenderableType, transient: bool = False) -> Live:
    return Live(
        auto_refresh=True,
        refresh_per_second=20,
        transient=transient,
        get_renderable=lambda: renderable,
    )


def hyperlink(file_path: Path, text: str | None = None) -> Text:
    text = escape(text or str(file_path))
    return Text.from_markup(f"[link={file_path.as_uri()}]{text}[/link]", style="blue")


class DictProgress(Progress):
    """A progress with a dict like interface to access tasks"""

    def __getitem__(self, task_id: TaskID) -> Task:
        with self._lock:
            return self._tasks[task_id]

    def __len__(self) -> int:
        with self._lock:
            return len(self._tasks)

    def sort_tasks(self, sort_fn: Callable[[Iterable[Task]], list[Task]]) -> None:
        with self._lock:
            sorted_tasks = sort_fn(self._tasks.values())
            self._tasks.clear()
            self._tasks.update((task.id, task) for task in sorted_tasks)


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


class ProgressProxy(ABC):
    def __init__(self, *columns: ProgressColumn | str, disable: bool = False, expand: bool = False) -> None:
        self._progress: Progress = Progress(*columns, disable=disable, expand=expand)
        self._progress.live._get_renderable = self.__rich__

    def __enter__(self) -> Self:
        self._progress.start()
        return self

    def __exit__(self, *_) -> None:
        self._progress.stop()

    @property
    def disable(self) -> bool:
        return self._progress.disable

    @disable.setter
    def disable(self, value: bool) -> None:
        self._progress.disable = value

    @abstractmethod
    def __rich__(self) -> RenderableType: ...
