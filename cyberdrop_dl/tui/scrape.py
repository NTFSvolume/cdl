from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, ClassVar

from rich.columns import Columns
from rich.progress import Progress, SpinnerColumn, TaskID

from cyberdrop_dl import __version__
from cyberdrop_dl.tui.common import ColumnsType, OverflowingPanel, UIPanel

if TYPE_CHECKING:
    from collections.abc import Generator

    from rich.console import RenderableType


class ScrapingPanel(OverflowingPanel):
    unit: ClassVar[str] = "URLs"
    columns: ClassVar[ColumnsType] = SpinnerColumn(), "[progress.description]{task.description}"

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=5)


class StatusMessage(UIPanel):
    columns: ClassVar[ColumnsType] = (SpinnerColumn(), "[progress.description]{task.description}")

    def __init__(self) -> None:
        super().__init__()
        self.activity: Progress = Progress(*self.columns)
        _ = self.activity.add_task(f"Running Cyberdrop-DL: v{__version__}", total=100, completed=0)
        self._task_id: TaskID = self._progress.add_task("", total=100, completed=0, visible=False)
        self._renderable: RenderableType = Columns([self.activity, self._progress])

    def _update(self, description: object = None) -> None:
        self._progress.update(
            self._task_id,
            description=str(description) if description is not None else None,
            visible=bool(description),
        )

    def __str__(self) -> str:
        return self._tasks[self._task_id].description

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self!s})"

    @contextlib.contextmanager
    def __call__(self, msg: str | None) -> Generator[None]:
        try:
            self._update(msg)
            yield
        finally:
            self._update()
