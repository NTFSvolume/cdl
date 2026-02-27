from __future__ import annotations

import asyncio
import contextlib
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from pydantic import ByteSize
from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl import config
from cyberdrop_dl.tui.common import ColumnsType, CounterPanel, TaskCounter

if TYPE_CHECKING:
    from collections.abc import Generator


def _get_enabled_hashes():
    yield "xxh128"
    if config.get().dedupe.add_md5_hash:
        yield "md5"
    if config.get().dedupe.add_sha256_hash:
        yield "sha256"


_base_dir: ContextVar[Path] = ContextVar("_base_dir")


class HashingPanel(CounterPanel):
    """Class that keeps track of hashed files."""

    _columns: ClassVar[ColumnsType] = ("{task.description}",)

    def __init__(self) -> None:
        super().__init__()
        self._hash_progress: Progress = Progress(
            "[progress.description]{task.description}", BarColumn(bar_width=None), "{task.completed:,}"
        )
        self._enabled_hashes: tuple[str, ...] = tuple(_get_enabled_hashes())
        self._computed_hashes: int = 0
        self._prev_hashed: int = 0

        for hash_type in self._enabled_hashes:
            desc = "[green]Hashed " + escape(f"[{hash_type}]")
            self._tasks_map[hash_type] = TaskCounter(self._hash_progress.add_task(desc, total=None))

        self._tasks_map.update(
            prev_hashed=TaskCounter(self._hash_progress.add_task("[green]Previously Hashed", total=None)),
            removed=TaskCounter(self._progress.add_task("", visible=False)),
            base_dir=TaskCounter(self._progress.add_task("")),
            file=TaskCounter(self._progress.add_task("")),
        )

        self._panel: Panel = Panel(
            Group(self._progress, self._hash_progress),
            title="Hashing",
            border_style="green",
            padding=(1, 1),
        )

    @property
    def hashed_files(self) -> int:
        return int(self._computed_hashes / len(self._enabled_hashes))

    @property
    def prev_hashed_files(self) -> int:
        return int(self._prev_hashed / len(self._enabled_hashes))

    @property
    def removed_files(self) -> int:
        return self._tasks_map["removed"].count

    @contextlib.contextmanager
    def __call__(self, path: Path) -> Generator[None]:
        token = _base_dir.set(path)
        desc = "[green]Base dir: [blue]" + escape(str(path))
        self._progress.update(self._tasks_map["base_dir"].id, description=desc)
        try:
            yield
        finally:
            _base_dir.reset(token)
            self._progress.update(self._tasks_map["base_dir"].id, description="")

    async def update_currently_hashing(self, file: Path | str) -> None:
        file = Path(file)
        size = await asyncio.to_thread(lambda *_: file.stat().st_size)
        size_text = ByteSize(size).human_readable(decimal=True)
        path = file.relative_to(_base_dir.get())
        self._progress.update(
            self._tasks_map["file"].id,
            description="[green]Current file: [blue]" + escape(f"{path}") + f" [green]({size_text})",
        )

    def add_hashed(self, hash_type: str) -> None:
        self._advance(hash_type)

    def add_prev_hashed(self) -> None:
        self._advance("prev_hashed")

    def add_removed(self) -> None:
        self._advance("removed")
