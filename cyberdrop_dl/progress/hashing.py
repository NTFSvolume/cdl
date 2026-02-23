from __future__ import annotations

import asyncio
import contextlib
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import ByteSize
from rich.console import Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import BarColumn, Progress

from cyberdrop_dl import config

from ._common import TaskCounter, TasksMap

if TYPE_CHECKING:
    from collections.abc import Generator


def _get_enabled_hashes():
    yield "xxh128"
    if config.get().dupe_cleanup_options.add_md5_hash:
        yield "md5"
    if config.get().dupe_cleanup_options.add_sha256_hash:
        yield "sha256"


_base_dir: ContextVar[Path] = ContextVar("_base_dir")


class HashingPanel:
    """Class that keeps track of hashed files."""

    def __init__(self) -> None:
        self._hash_progress = Progress(
            "[progress.description]{task.description}", BarColumn(bar_width=None), "{task.completed:,}"
        )
        self._progress = Progress("{task.description}")
        self._enabled_hashes: tuple[str, ...] = tuple(_get_enabled_hashes())
        self._computed_hashes: int = 0
        self._prev_hashed: int = 0
        self._tasks: TasksMap = TasksMap()

        for hash_type in self._enabled_hashes:
            desc = "[green]Hashed " + escape(f"[{hash_type}]")
            self._tasks[hash_type] = TaskCounter(self._hash_progress.add_task(desc, total=None))

        self._tasks.update(
            prev_hashed=TaskCounter(self._hash_progress.add_task("[green]Previously Hashed", total=None)),
            removed=TaskCounter(self._progress.add_task("", visible=False)),
            base_dir=TaskCounter(self._progress.add_task("")),
            file=TaskCounter(self._progress.add_task("")),
        )

        self._panel = Panel(
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
        return self._tasks["removed"].count

    def __rich__(self) -> Panel:
        return self._panel

    @contextlib.contextmanager
    def currently_hashing_dir(self, path: Path) -> Generator[None]:
        token = _base_dir.set(path)

        desc = "[green]Base dir: [blue]" + escape(str(path))
        self._progress.update(self._tasks["base_dir"].id, description=desc)
        try:
            yield
        finally:
            _base_dir.reset(token)
            self._progress.update(self._tasks["base_dir"].id, description="")

    async def update_currently_hashing(self, file: Path | str) -> None:
        file = Path(file)
        size = await asyncio.to_thread(lambda *_: file.stat().st_size)
        size_text = ByteSize(size).human_readable(decimal=True)
        path = file.relative_to(_base_dir.get())
        self._progress.update(
            self._tasks["file"].id,
            description="[green]Current file: [blue]" + escape(f"{path}") + f" [green]({size_text})",
        )

    def add_new_completed_hash(self, hash_type: str) -> None:
        self._hash_progress.advance(self._tasks[hash_type].id)
        self._tasks[hash_type].count += 1

    def add_prev_hash(self) -> None:
        self._hash_progress.advance(self._tasks["prev_hashed"].id)
        self._tasks["prev_hashed"].count += 1

    def add_removed_file(self) -> None:
        self._hash_progress.advance(self._tasks["removed"].id)
        self._tasks["removed"].count += 1
