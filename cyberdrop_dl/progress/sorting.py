from __future__ import annotations

from typing import ClassVar

from rich.progress import BarColumn, SpinnerColumn, TaskID

from cyberdrop_dl.progress.common import UIPanel


class SortingPanel(UIPanel):
    """Class that keeps track of sorted files."""

    unit: ClassVar[str] = "Folders"
    _columns = (
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

    def new_task(self, folder: str, expected_size: int | None) -> TaskID:
        description = self._clean_task_desc(folder)
        return super()._add_task(description, expected_size)

    def advance_folder(self, task_id: TaskID, amount: int = 1) -> None:
        self._advance(task_id, amount)

    def increment_audio(self) -> None:
        self.audio_count += 1

    def increment_video(self) -> None:
        self.video_count += 1

    def increment_image(self) -> None:
        self.image_count += 1

    def increment_other(self) -> None:
        self.other_count += 1
