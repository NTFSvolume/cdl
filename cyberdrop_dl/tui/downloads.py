from __future__ import annotations

import contextlib
from contextvars import ContextVar
from typing import TYPE_CHECKING, ClassVar, Final

from rich.progress import (
    BarColumn,
    DownloadColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TimeRemainingColumn,
    TransferSpeedColumn,
    filesize,
)
from rich.text import Text
from typing_extensions import override

from cyberdrop_dl.tui.common import ColumnsType, OverflowingPanel, ProgressHook, ProgressProxy

if TYPE_CHECKING:
    from collections.abc import Generator

_current_hls_task: ContextVar[TaskID] = ContextVar("_current_hls_task")
_HLS_TASK_FIELD_NAME = "hls"


class AutoTransferSpeedColumn(TransferSpeedColumn):
    """Shows `X MBs/s` for files and `X segs/s` for HLS downloads"""

    @override
    def render(self, task: Task) -> Text:
        if _HLS_TASK_FIELD_NAME not in task.fields:
            return super().render(task)

        speed = task.finished_speed or task.speed
        download_status = "?" if speed is None else f"{int(speed)} seg/s"
        return Text(download_status, style="progress.data.speed")


class AutoDownloadColumn(DownloadColumn):
    """Shows `X/Y MBs` for files and `X/Y segs [Z MB]` for HLS downloads"""

    @override
    def render(self, task: Task) -> Text:
        if _HLS_TASK_FIELD_NAME not in task.fields:
            return super().render(task)

        hls_task: Task = task.fields[_HLS_TASK_FIELD_NAME]
        downloaded_bytes_str = self._format_bytes(int(hls_task.completed))
        completed_segs_str = f"{int(task.completed):,}"
        if task.total is not None:
            total_segs_str = f"{int(task.total):,}"
        else:
            total_segs_str = "?"

        download_status = f"{completed_segs_str}/{total_segs_str} segs [{downloaded_bytes_str}]"
        return Text(download_status, style="progress.download")

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


class DownloadsPanel(OverflowingPanel):
    unit: ClassVar[str] = "files"
    columns: ClassVar[ColumnsType] = (
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>6.2f}%",
        "━",
        AutoDownloadColumn(),
        "━",
        AutoTransferSpeedColumn(),
        "━",
        TimeRemainingColumn(elapsed_when_finished=True),
    )

    def __init__(self) -> None:
        self.total_data_written: int = 0
        super().__init__(visible_tasks_limit=10)
        self._hls_progress: Final[ProgressProxy] = ProgressProxy("dummy")

    @override
    def _clean_task_description(self, description: object, /) -> str:
        return self._remove_non_ascii(str(description).rsplit("/", 1)[-1])

    @contextlib.contextmanager
    def new_hls_task(self, filename: object, /, segments: float | None = None) -> Generator[None]:
        _ = self.new_task(filename, segments)
        task_id = self._hls_progress.add_task("")
        self._tasks[task_id].fields[_HLS_TASK_FIELD_NAME] = self._hls_progress.tasks_proxy[task_id]
        token = _current_hls_task.set(task_id)
        try:
            yield
        finally:
            self._remove_task(task_id)
            self._hls_progress.remove_task(task_id)
            _current_hls_task.reset(token)

    def new_hls_seg_task(self) -> ProgressHook:
        task_id = _current_hls_task.get()
        hls_task: Task = self._tasks[task_id].fields[_HLS_TASK_FIELD_NAME]

        def advance(amount: int) -> None:
            self.total_data_written += amount
            self._hls_progress.advance(task_id, amount)

        def done() -> None:
            self._progress.advance(task_id, 1)

        def speed() -> float:
            return hls_task.finished_speed or hls_task.speed or 0

        return ProgressHook(advance, done, speed)

    @override
    def _advance(self, task_id: TaskID, amount: int = 1) -> None:
        self.total_data_written += amount
        self._progress.advance(task_id, amount)
