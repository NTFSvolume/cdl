from __future__ import annotations

from typing import ClassVar

from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl.tui.common import ColumnsType, CounterPanel, TaskCounter


class FileStats(CounterPanel):
    """Class that keeps track of completed, skipped and failed files."""

    _columns: ClassVar[ColumnsType] = (
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        "{task.completed:,}",
    )

    def __repr__(self) -> str:
        fields = ", ".join(f"{name}={value!r}" for name, value in vars(self).items())
        return f"{type(self).__name__}({fields})"

    def __init__(self) -> None:
        super().__init__()
        self._total: int = 0

        for name, color, desc in (
            ("completed", "green", "Completed"),
            ("previously_completed", "yellow", "Previously Downloaded"),
            ("skipped", "yellow", "Skipped By Configuration"),
            ("queued", "cyan", "Queued"),
            ("failed", "red", "Failed"),
        ):
            self._tasks_map[name] = TaskCounter(self._progress.add_task(f"[{color}]{desc}", total=0))

        self.simple: Progress = Progress(*self.columns)
        self._tasks_map["simple"] = TaskCounter(self.simple.add_task("Completed", total=0))
        self._panel: Panel = Panel(
            self._progress,
            title="Files",
            border_style="green",
            padding=(1, 1),
            subtitle=self._subtitle,
        )

    @property
    def _subtitle(self) -> str:
        return f"Total Files: [white]{self._total:,}"

    def _redraw(self, increase_total: bool = True) -> None:
        self._panel.subtitle = self._subtitle
        self._total += 1
        for name, task in self._tasks_map.items():
            if name == "simple":
                completed = self._total - self._tasks_map["queued"].count
                progress = self.simple
            else:
                progress, completed = self._progress, None

            progress.update(task.id, total=self._total, completed=completed)

    def _advance(self, task_name: str) -> None:
        super()._advance(task_name)
        self._redraw()

    def add_completed(self) -> None:
        self._advance("completed")

    def add_previously_completed(self) -> None:
        self._advance("previously_completed")

    def add_skipped(self) -> None:
        self._advance("skipped")

    def add_failed(self) -> None:
        self._advance("failed")

    def update_queued(self, count: int) -> None:
        # TODO: This is probably wrong
        self._tasks_map["queued"].count = count
        self._progress.update(self._tasks_map["queued"].id, completed=count)

    @property
    def skipped_files(self) -> int:
        return self._tasks_map["skipped"].count

    @property
    def failed_files(self) -> int:
        return self._tasks_map["failed"].count

    @property
    def completed_files(self) -> int:
        return self._tasks_map["completed"].count

    @property
    def previously_completed(self) -> int:
        return self._tasks_map["previously_completed"].count
