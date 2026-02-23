from __future__ import annotations

from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID

from cyberdrop_dl.progress.panels import UIPanel


class SortingPanel(UIPanel):
    """Class that keeps track of sorted files."""

    title = "Sorting"
    name = "Folders"

    def __init__(self, visible_tasks_limit: int) -> None:
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>6.2f}%",
            "━",
            "{task.completed}/{task.total} files",
        )
        super().__init__(progress, visible_tasks_limit)

        self.audio_count = self.video_count = self.image_count = self.other_count = 0

    def new_task(self, folder: str, expected_size: int | None) -> TaskID:
        description = self._clean_task_desc(folder)
        return super().add_task(description, expected_size)

    def advance_folder(self, task_id: TaskID, amount: int = 1) -> None:
        self._progress.advance(task_id, amount)

    def increment_audio(self) -> None:
        self.audio_count += 1

    def increment_video(self) -> None:
        self.video_count += 1

    def increment_image(self) -> None:
        self.image_count += 1

    def increment_other(self) -> None:
        self.other_count += 1
