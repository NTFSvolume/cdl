from __future__ import annotations

from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl.progress.common import TaskCounter, UIPanel


class FileStats(UIPanel):
    """Class that keeps track of completed, skipped and failed files."""

    _columns = (
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        "{task.completed:,}",
    )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({vars(self)!r})"

    def __init__(self) -> None:
        super().__init__()
        self._total_files: int = 0

        for name, color, desc in (
            ("completed", "green", "Completed"),
            ("previously_completed", "yellow", "Previously Downloaded"),
            ("skipped", "yellow", "Skipped By Configuration"),
            ("queued", "cyan", "Queued"),
            ("failed", "red", "Failed"),
        ):
            self._tasks_map[name] = TaskCounter(self._progress.add_task(f"[{color}]{desc}", total=0))

        self.simple: Progress = Progress(*self._columns)
        self._tasks_map["simple"] = TaskCounter(self.simple.add_task("Completed", total=0))
        self._renderable = Panel(
            self._progress,
            title="Files",
            border_style="green",
            padding=(1, 1),
            subtitle=self.subtitle,
        )

    @property
    def subtitle(self) -> str:
        return f"Total Files: [white]{self._total_files:,}"

    def update_total(self, increase_total: bool = True) -> None:
        self._renderable.subtitle = self.subtitle
        if not increase_total:
            return

        self._total_files += 1
        for name, task in self._tasks_map.items():
            if name == "simple":
                completed = self._total_files - self._tasks_map["queued"].count
                progress = self.simple
            else:
                progress, completed = self._progress, None

            progress.update(task.id, total=self._total_files, completed=completed)

    def add_completed(self) -> None:
        self._progress.advance(self._tasks_map["completed"].id)
        self._tasks_map["completed"].count += 1

    def add_previously_completed(self, increase_total: bool = True) -> None:
        if increase_total:
            self.update_total()

        self._tasks_map["previously_completed"].count += 1
        self._progress.advance(self._tasks_map["previously_completed"].id)

    def add_skipped(self) -> None:
        self._progress.advance(self._tasks_map["skipped"].id)
        self._tasks_map["skipped"].count += 1

    def add_failed(self) -> None:
        self._progress.advance(self._tasks_map["failed"].id)
        self._tasks_map["failed"].count += 1

    def update_queued(self, count: int) -> None:
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
