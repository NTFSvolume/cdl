from __future__ import annotations

from typing import ClassVar

from rich.progress import BarColumn, SpinnerColumn
from typing_extensions import override

from cyberdrop_dl.tui.common import ColumnsType, OverflowingPanel


class SortingPanel(OverflowingPanel):
    """Class that keeps track of sorted files."""

    unit: ClassVar[str] = "Folders"
    columns: ClassVar[ColumnsType] = (
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

    @override
    def _clean_task_description(self, description: object, /) -> str:
        return self._remove_non_ascii(str(description))

    def add_audio(self) -> None:
        self.audio_count += 1

    def add_video(self) -> None:
        self.video_count += 1

    def add_image(self) -> None:
        self.image_count += 1

    def add_other(self) -> None:
        self.other_count += 1
