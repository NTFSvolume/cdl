from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, ClassVar

from rich.columns import Columns
from rich.progress import Progress, SpinnerColumn, Task

from cyberdrop_dl import __version__
from cyberdrop_dl.progress.common import ColumnsType, OverflowPanel, UIComponent

if TYPE_CHECKING:
    from collections.abc import Generator


class ScrapingPanel(OverflowPanel):
    unit: ClassVar[str] = "URLs"
    columns: ClassVar[ColumnsType] = SpinnerColumn(), "[progress.description]{task.description}"

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=5)


class StatusMessage(UIComponent):
    columns: ClassVar[ColumnsType] = (SpinnerColumn(), "[progress.description]{task.description}")

    def __init__(self, description: str = f"Running Cyberdrop-DL: v{__version__}") -> None:
        super().__init__()
        self.activity: Progress = Progress(*self.columns, transient=True, expand=True)
        _ = self.activity.add_task(description)
        task_id = self._progress.add_task("", total=100, completed=0, visible=False)
        self._task: Task = self._progress[task_id]
        self._renderable: Columns = Columns([self.activity, self._progress])

    def __rich__(self) -> Columns:
        return self._renderable

    def _update(self, description: object = None) -> None:
        self._progress.update(
            self._task.id,
            description=str(description) if description is not None else None,
            visible=bool(description),
        )

    def __str__(self) -> str:
        return self._task.description

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self!s})"

    @contextlib.contextmanager
    def __call__(self, msg: str | None) -> Generator[None]:
        try:
            self._update(msg)
            yield
        finally:
            self._update()
