from __future__ import annotations

import asyncio
import contextlib
from contextvars import ContextVar
from typing import TYPE_CHECKING, ClassVar, Final

from rich.live import get_console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    filesize,
)
from rich.table import Column
from rich.text import Text
from typing_extensions import override

from cyberdrop_dl.tui.common import ColumnsType, DictProgress, OverflowPanel, ProgressHook, Random, create_live

if TYPE_CHECKING:
    from collections.abc import Generator

_current_hls_task: ContextVar[TaskID] = ContextVar("_current_hls_task")
_HLS_TASK_FIELD_NAME: Final = "hls"


class AutoTransferSpeedColumn(TransferSpeedColumn):
    @override
    def render(self, task: Task) -> Text:
        real_task: Task = task.fields.get(_HLS_TASK_FIELD_NAME, task)
        return super().render(real_task)


class AutoDownloadColumn(DownloadColumn):
    """Shows `X/Y MBs` for files and `X/Y segs [Z MB]` for HLS downloads"""

    @override
    def render(self, task: Task) -> Text:
        hls_task: Task | None = task.fields.get(_HLS_TASK_FIELD_NAME)
        if hls_task is None:
            return super().render(task)

        downloaded_bytes = self._format_bytes(int(hls_task.completed))
        completed_segs = int(task.completed)
        total_segs = "?" if task.total is None else f"{int(task.total):,}"
        total_width = len(str(total_segs))
        download_status = f"{completed_segs:{total_width}d}/{total_segs} segs [{downloaded_bytes}]"
        return Text(download_status, style="progress.download", justify="right")

    def _format_bytes(self, n_bytes: int) -> str:
        multiplier, unit = self._select_bytes_units(n_bytes)
        precision = 0 if multiplier == 1 else 1
        normalized_n_bytes = n_bytes / multiplier
        n_bytes_str = f"{normalized_n_bytes:,.{precision}f}"
        return f"{n_bytes_str} {unit}"

    def _select_bytes_units(self, size: int) -> tuple[int, str]:
        if self.binary_units:
            return filesize.pick_unit_and_suffix(
                size,
                ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"],
                1024,
            )

        return filesize.pick_unit_and_suffix(
            size,
            ["bytes", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"],
            1000,
        )


class AutoTruncatedTextColumn(TextColumn):
    def render(self, task: Task) -> Text:
        text = super().render(task)
        width = get_console().width
        available_witdh = min((width * 60 // 100), (width - 65))
        desc_limit = max(available_witdh, 8)
        text.truncate(desc_limit, overflow="ellipsis")
        return text


class DownloadsPanel(OverflowPanel):
    unit: ClassVar[str] = "files"
    columns: ClassVar[ColumnsType] = (
        SpinnerColumn(),
        AutoTruncatedTextColumn(
            "[progress.description]{task.description}",
            table_column=Column(justify="left", no_wrap=True),
        ),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.1f}%",
        "━",
        AutoDownloadColumn(table_column=Column(justify="right", no_wrap=True)),
        "━",
        AutoTransferSpeedColumn(table_column=Column(justify="right", no_wrap=True)),
        "━",
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
    )

    @property
    def total_data_written(self) -> int:
        return self._total_amount

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=10)
        self._hls_progress: Final[DictProgress] = DictProgress("")

    @override
    def _clean_task_description(self, description: object, /) -> str:
        return self._escape(str(description).rsplit("/", 1)[-1])

    @contextlib.contextmanager
    def new_hls_task(self, filename: str, /, segments: float | None = None) -> Generator[None]:
        # For HLS downloads, we use 2 different tasks on 2 different progress bars
        # One (hidden) to track the downloaded bytes (with an unknown total) and one to track
        # the number of downloaded segments (with a known total)
        # We create both at the same time and smuggle the bytes task
        # as a field of the segments task to make all info available to the main progress for rendering

        task_id = self._hls_progress.add_task("", total=None, visible=False)
        segments_task = self._add_task(filename, segments)
        bytes_task = self._hls_progress[task_id]
        # The None values are not required but pyright complains cause _HLS_TASK_FIELD_NAME may overlap with a valid param
        self._progress.update(
            segments_task.id,
            total=None,
            completed=None,
            advance=None,
            description=None,
            visible=None,
            refresh=False,
            **{_HLS_TASK_FIELD_NAME: bytes_task},
        )
        token = _current_hls_task.set(segments_task.id)
        try:
            yield
        finally:
            self._remove_task(segments_task)
            self._hls_progress.remove_task(task_id)
            _current_hls_task.reset(token)

    def new_hls_seg_task(self) -> ProgressHook:
        segments_task_id = _current_hls_task.get()
        hls_task: Task = self._progress[segments_task_id].fields[_HLS_TASK_FIELD_NAME]

        def advance(amount: int) -> None:
            self._total_amount += amount
            self._hls_progress.advance(hls_task.id, amount)

        def on_exit() -> None:
            self._progress.advance(segments_task_id, 1)

        def get_speed() -> float:
            return hls_task.finished_speed or hls_task.speed or 0

        return ProgressHook(advance, get_speed, on_exit)


async def test(n_files: int = 10) -> None:
    panel = DownloadsPanel()

    async def download(hook: ProgressHook, size: int) -> None:
        with hook:
            for chunk in Random.int_until(size, max_step=1e8):
                hook.advance(chunk)
                await Random.sleep()

    async def download_file(filename: str) -> None:
        size = Random.int(1e4, 1e9)
        hook = panel.new_task(filename, size)
        await download(hook, size)

    async def download_hls(filename: str) -> None:
        sem = asyncio.BoundedSemaphore(20)

        async def download_segment() -> None:
            size = Random.int(1e2, 1e4)
            hook = panel.new_hls_seg_task()
            try:
                await download(hook, size)
            finally:
                sem.release()

        n_segments = Random.int(100, 1_000)

        with panel.new_hls_task(filename, n_segments):
            async with asyncio.TaskGroup() as tg:
                for _ in range(n_segments):
                    await sem.acquire()
                    tg.create_task(download_segment())

    with create_live(panel):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(download_file("file_X_with_a_very_long_name_and_?_#.mp4"))
            files = iter(f"file_{idx:03d}" for idx in range(n_files))
            for file in files:
                fn = Random.choice([download_hls, download_file])
                tg.create_task(fn(file))

            await Random.sleep()
            for file in files:
                fn = Random.choice([download_hls, download_file])
                tg.create_task(fn(file))


if __name__ == "__main__":  # pragma: no coverage
    asyncio.run(test(40))
