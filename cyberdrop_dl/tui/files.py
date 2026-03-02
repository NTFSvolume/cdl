from __future__ import annotations

from typing import ClassVar

from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl.tui.common import ColumnsType, TaskCounter, UIPanel


class FileStatsPanel(UIPanel):
    """Class that keeps track of completed, skipped and failed files."""

    columns: ClassVar[ColumnsType] = (
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
            self._counters[name] = TaskCounter(self._progress.add_task(f"[{color}]{desc}", total=0))

        self.simple: Progress = Progress(*self.columns)
        self._counters["simple"] = TaskCounter(self.simple.add_task("Completed", total=0))
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
        for name, task in self._counters.items():
            if name == "simple":
                completed = self._total - self._counters["queued"].count
                progress = self.simple
            else:
                progress, completed = self._progress, None

            progress.update(task.id, total=self._total, completed=completed)

    def _increase_counter(self, task_name: str) -> None:
        super()._increase_counter(task_name)
        self._redraw()

    def add_completed(self) -> None:
        self._increase_counter("completed")

    def add_prev_completed(self) -> None:
        self._increase_counter("previously_completed")

    def add_skipped(self) -> None:
        self._increase_counter("skipped")

    def add_failed(self) -> None:
        self._increase_counter("failed")

    def update_queued(self, count: int) -> None:
        # TODO: This is probably wrong
        counter = self._counters["queued"]
        counter.count = count
        self._progress.update(counter.id, completed=count)

    @property
    def skipped_files(self) -> int:
        return self._counters["skipped"].count

    @property
    def failed_files(self) -> int:
        return self._counters["failed"].count

    @property
    def completed_files(self) -> int:
        return self._counters["completed"].count

    @property
    def previously_completed(self) -> int:
        return self._counters["previously_completed"].count
