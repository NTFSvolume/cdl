from __future__ import annotations

import dataclasses
from pathlib import Path
from time import time
from typing import TYPE_CHECKING

from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskID

from . import LiveUI

if TYPE_CHECKING:
    from collections.abc import Generator

    from cyberdrop_dl.managers.manager import Manager


def _generic_progress() -> Progress:
    return Progress("[progress.description]{task.description}", BarColumn(bar_width=None), "{task.completed:,}")


@dataclasses.dataclass(slots=True)
class HashingStats:
    xxh128: int = 0
    md5: int = 0
    sha256: int = 0

    new_hashed: int = 0
    prev_hashed: int = 0

    @property
    def total(self) -> int:
        return self.new_hashed + self.prev_hashed


class DedupeUI(LiveUI):
    def __init__(self, manager: Manager, base_dir: Path) -> None:
        self.manager = manager
        self._hash_progress = _generic_progress()
        self._remove_progress = _generic_progress()
        self._match_progress = _generic_progress()

        # hashing
        self._stats: HashingStats = HashingStats()
        self.hash_progress_group = Group("[green]Base dir: [blue]" + escape(f"{base_dir}"), self._hash_progress)

        self._tasks_map: dict[str, TaskID] = dict(self._init_tasks())

        # remove
        self.removed_files = 0
        self.removed_progress_group = Group(self._match_progress, self._remove_progress)
        self.removed_files_task_id = self._remove_progress.add_task(
            "[green]Removed From Downloaded Files",
            total=None,
        )
        self._panel = Panel(
            self.hash_progress_group,
            title="Hashing",
            border_style="green",
            padding=(1, 1),
        )

        self._panel = Panel(
            self.removed_progress_group,
            border_style="green",
            padding=(1, 1),
        )


class HashingUI(LiveUI):
    """Class that keeps track of hashed files."""

    def __init__(self, base_dir: Path) -> None:
        self._progress = _generic_progress()
        self._stats: HashingStats = HashingStats()
        self._tasks_map: dict[str, TaskID] = dict(self._init_tasks())
        self._panel = Panel(
            Group("[green]Base dir: [blue]" + escape(f"{base_dir}"), self._progress),
            title="Hashing",
            border_style="green",
            padding=(1, 1),
        )

    @property
    def stats(self) -> HashingStats:
        return self._stats

    def __rich__(self) -> Panel:
        current_total = self._stats.total
        if current_total != self._total:
            for name, task_id in self._tasks_map.items():
                self._progress.update(task_id, total=current_total, completed=getattr(self._stats, name))
            self._total = current_total

        self._panel.subtitle = f"Total: [white]{current_total:,}"
        return self._panel

    def _init_tasks(self) -> Generator[tuple[str, TaskID]]:
        for algo in ("xxh128", "md5", "sha256"):
            desc = "[green]Hashed " + escape(f"[{algo}]")
            yield algo, self._progress.add_task(desc, total=None)

        yield "prev_hashed", self._progress.add_task("[green]Previously Hashed", total=None)


if __name__ == "__main__":
    panel = HashingUI(
        Path("/folder1/cdl_downloads"),
    )

    with panel(transient=False):
        time.sleep(3)
        panel.stats.md5 += 1
        time.sleep(1)
        panel.stats.sha256 += 5
        time.sleep(1)
        panel.stats.xxh128 += 15
        time.sleep(3)
