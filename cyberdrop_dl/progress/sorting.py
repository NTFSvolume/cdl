from __future__ import annotations

import dataclasses
import time
from pathlib import Path
from typing import ClassVar, Self, final

from rich.console import Group
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskID
from rich.spinner import Spinner
from rich.text import Text

from cyberdrop_dl.progress import hyperlink


@dataclasses.dataclass(slots=True)
class SortStats:
    videos: int = 0
    audios: int = 0
    images: int = 0
    others: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return sum(dataclasses.astuple(self))


@final
class SortingUI:
    """Class that keeps track of sorted files."""

    columns: ClassVar = (
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.1f}%",
        "━",
        "{task.completed:,}",
    )

    def __init__(self, source: Path, dest: Path) -> None:
        self._stats: SortStats = SortStats()
        self._progress: Progress = Progress(*self.columns)
        self._tasks_map: dict[str, TaskID] = {}
        for name, emoji in [
            ("audios", "musical_notes"),
            ("videos", "movie_camera"),
            ("images", "framed_picture"),
            ("others", "spiral_note_pad"),
            ("errors", "cross_mark"),
        ]:
            color = "red" if "errors" in name else "blue"
            self._tasks_map[name] = self._progress.add_task(f"[{color}] {name.capitalize()} :{emoji}: ", total=None)

        def file_row(name: str, file: Path) -> Text:
            return Text.assemble((f"{name}: ", "green"), Text.from_markup(hyperlink(file)))

        self._total: int = 0

        self._panel: Panel = Panel(
            Group(
                file_row("Source", source),
                file_row("Destination", dest),
                "",
                self._progress,
                "",
                Spinner("dots", "Sorting files....", style="green"),
            ),
            title="Sorting Downloads ",
            border_style="green",
            padding=(1, 1),
        )
        self._progress.live._get_renderable = self.__rich__

    def __enter__(self) -> Self:
        self._progress.start()
        return self

    def __exit__(self, *_) -> None:
        self._progress.stop()

    @property
    def stats(self) -> SortStats:
        return self._stats

    def __rich__(self) -> Panel:
        current_total = self._stats.total
        if current_total != self._total:
            for name, task_id in self._tasks_map.items():
                self._progress.update(task_id, total=current_total, completed=getattr(self._stats, name))
            self._total = current_total

        self._panel.subtitle = f"Total Files: [white]{current_total:,}"
        return self._panel


if __name__ == "__main__":
    panel = SortingUI(
        Path("/folder1/cdl_downloads"),
        Path("/folder1/cdl_downloads_sorted"),
    )

    with panel:
        time.sleep(3)
        panel.stats.audios += 1
        time.sleep(1)
        panel.stats.audios += 5
        time.sleep(1)
        panel.stats.videos += 15
        time.sleep(1)
        panel.stats.audios += 1
        time.sleep(1)
        panel.stats.errors += 10
        time.sleep(3)
