from __future__ import annotations

from typing import ClassVar

from rich.progress import BarColumn, SpinnerColumn, TaskID

from cyberdrop_dl.tui.common import OverflowingPanel


class SortingPanel(OverflowingPanel):
    """Class that keeps track of sorted files."""

    unit: ClassVar[str] = "Folders"
    columns = (
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        "{task.completed}/{task.total} files",
    )

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=1)
        self.audio_count = self.video_count = self.image_count = self.other_count = 0

    def _add_task(self, description: str, total: float | None = None, /, *, completed: int = 0) -> TaskID:
        description = self._clean_task_desc(description)
        return super()._add_task(description, total)

    def add_audio(self) -> None:
        self.audio_count += 1

    def add_video(self) -> None:
        self.video_count += 1

    def add_image(self) -> None:
        self.image_count += 1

    def add_other(self) -> None:
        self.other_count += 1
