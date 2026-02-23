from __future__ import annotations

from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl.progress._common import TaskCounter, TasksMap


class FileStats:
    """Class that keeps track of completed, skipped and failed files."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}({vars(self)!r})"

    def __init__(self) -> None:
        self._progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>6.2f}%",
            "━",
            "{task.completed:,}",
        )

        self._total_files = 0

        self._panel = Panel(
            self._progress,
            title="Files",
            border_style="green",
            padding=(1, 1),
            subtitle=f"Total Files: [white]{self._total_files:,}",
        )
        self.simple_progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>6.2f}%",
            "━",
            "{task.completed:,}",
        )

        self._tasks = TasksMap()

        for name, color, desc in (
            ("completed", "green", "Completed"),
            ("previously_completed", "yellow", "Previously Downloaded"),
            ("skipped", "yellow", "Skipped By Configuration"),
            ("queued", "cyan", "Queued"),
            ("failed", "red", "Failed"),
        ):
            self._tasks[name] = TaskCounter(self._progress.add_task(f"[{color}]{desc}", total=0))

        self._tasks["simple"] = TaskCounter(self.simple_progress.add_task("Completed", total=0))

    def __rich__(self) -> Panel:
        return self._panel

    def update_total(self, increase_total: bool = True) -> None:
        self._panel.subtitle = f"Total Files: [white]{self._total_files:,}"
        if not increase_total:
            return

        self._total_files = self._total_files + 1
        self._progress.update(self._tasks.completed.id, total=self._total_files)
        self._progress.update(self._tasks.previously_completed.id, total=self._total_files)
        self._progress.update(self._tasks.skipped.id, total=self._total_files)
        self._progress.update(self._tasks.failed.id, total=self._total_files)
        self._progress.update(self._tasks.queued.id, total=self._total_files)
        self.simple_progress.update(
            self._tasks.simple.id,
            total=self._total_files,
            completed=self._total_files - self._tasks.queued.count,
        )

    def add_completed(self) -> None:
        self._progress.advance(self._tasks.completed.id)
        self._tasks.completed.count += 1

    def add_previously_completed(self, increase_total: bool = True) -> None:
        if increase_total:
            self.update_total()

        self._tasks.previously_completed.count += 1
        self._progress.advance(self._tasks.previously_completed.id)

    def add_skipped(self) -> None:
        self._progress.advance(self._tasks.skipped.id)
        self._tasks.skipped.count += 1

    def add_failed(self) -> None:
        self._progress.advance(self._tasks.failed.id)
        self._tasks.failed.count += 1

    def update_queued(self, count: int) -> None:
        self._tasks.queued.count = count
        self._progress.update(self._tasks.queued.id, completed=count)

    @property
    def skipped_files(self) -> int:
        return self._tasks.skipped.count

    @property
    def failed_files(self) -> int:
        return self._tasks.failed.count

    @property
    def completed_files(self) -> int:
        return self._tasks.completed.count

    @property
    def previously_completed(self) -> int:
        return self._tasks.previously_completed.count
