from __future__ import annotations

import contextlib
import dataclasses
import itertools
import time
from collections import deque
from typing import TYPE_CHECKING, ClassVar, Final, final

from rich.columns import Columns
from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import ProgressColumn, SpinnerColumn, Task, TaskID
from rich.spinner import Spinner
from rich.text import Text

from cyberdrop_dl import __version__
from cyberdrop_dl.progress import DictProgress, create_live

if TYPE_CHECKING:
    from collections.abc import Generator

    from rich.progress import Task, TaskID

_COLOR: str = "plum3"


_generate_unique_id = itertools.count(1).__next__


@final
@dataclasses.dataclass(slots=True, frozen=True)
class StatusMessage:
    description: Text | str = f"Running cyberdrop-dl [blue]v{__version__}[/blue]"
    _messages: dict[int, tuple[Spinner, Text]] = dataclasses.field(init=False, default_factory=dict)
    _cols: Columns = dataclasses.field(init=False, default_factory=Columns)

    def __post_init__(self) -> None:
        self._cols.renderables.extend([Spinner("dots", style="green"), self.description])

    def __rich__(self) -> Columns:
        return self._cols

    @contextlib.contextmanager
    def __call__(self, msg: object) -> Generator[None]:
        msg_id = _generate_unique_id()
        try:
            self._messages[msg_id] = new_msg = Spinner("dots", style="green"), Text(escape(str(msg)))
            self._cols.renderables.extend(new_msg)
            yield
        finally:
            _ = self._messages.pop(msg_id)
            self._cols.renderables[2:] = itertools.chain.from_iterable(self._messages.values())


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


@final
class ScrapingPanel(OverflowPanel):
    unit: ClassVar[str] = "URLs"

    def __init__(self) -> None:
        super().__init__(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            visible_tasks_limit=3,
            expand=False,
        )

    @contextlib.contextmanager
    def new(self, url: object) -> Generator[None]:
        task = self._add_task(str(url))
        try:
            yield
        finally:
            self._remove_task(task)


if __name__ == "__main__":
    panel = ScrapingPanel()
    status = StatusMessage()
    with create_live(status):
        time.sleep(2)
        with status("test 1"):
            time.sleep(2)
            with status("test 2"):
                time.sleep(2)
                with status("test 3"):
                    time.sleep(2)
                time.sleep(2)
                with status("test 4"):
                    time.sleep(2)
                time.sleep(2)
        time.sleep(2)

        with status("test 5"):
            time.sleep(2)

    with create_live(panel):
        a = panel._add_task("url_a")
        b = panel._add_task("url_b")
        c = panel._add_task("url_c")
        time.sleep(5)
        d = panel._add_task("url_d")
        _ = panel._add_task("url_e")
        time.sleep(5)
        panel._remove_task(a)
        panel._remove_task(b)
        panel._remove_task(c)
        panel._remove_task(d)
        time.sleep(2)
        with panel.new("http://github.com"):
            time.sleep(2)
            with panel.new("http://github2.com"):
                time.sleep(2)
            with panel.new("http://github3.com"):
                time.sleep(2)
