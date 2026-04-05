from __future__ import annotations

import asyncio
import dataclasses
import random
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

from rich.live import Live
from rich.markup import escape
from rich.progress import Progress, ProgressColumn, Task, TaskID
from rich.text import Text

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from pathlib import Path

    from rich.console import RenderableType


def create_live(renderable: RenderableType) -> Live:
    return Live(
        auto_refresh=True,
        refresh_per_second=20,
        transient=False,
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
            self._tasks.update((tasks.id, tasks) for tasks in sorted_tasks)


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
    async def sleep(delay: float = 0.1) -> None:
        await asyncio.sleep(delay)


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
