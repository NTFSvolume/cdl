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

from cyberdrop_dl.tui.common import ColumnsType, DictProgress, OverflowPanel, ProgressHook

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

        downloaded_bytes_str = self._format_bytes(int(hls_task.completed))
        completed_segs_str = f"{int(task.completed):,}"
        total_segs_str = "?" if task.total is None else f"{int(task.total):,}"
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


class DownloadsPanel(OverflowPanel):
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

    @property
    def total_data_written(self) -> int:
        return self._total_amount

    def __init__(self) -> None:
        super().__init__(visible_tasks_limit=10)
        self._hls_progress: Final[DictProgress] = DictProgress("")

    @override
    def _clean_task_description(self, description: object, /) -> str:
        return self._remove_non_ascii(str(description).rsplit("/", 1)[-1])

    @contextlib.contextmanager
    def new_hls_task(self, filename: str, /, segments: float | None = None) -> Generator[None]:
        # For HLS downloads, we use 2 different tasks on 2 diferent progress (one hidden) to track it
        # One to track the downloaded bytes (with an unknown total) and one to track
        # the number of downloaded segments (with a known total)
        # We create both at the same time (so they have the same task_id) and smuggle the bytes task
        # as a field of the segments task to make all info available to the main progress for rendering

        task_id = self._hls_progress.add_task("", total=None, visible=False)
        _ = self.new_task(filename, segments)
        task = self._progress[task_id]
        hls_task = self._hls_progress[task_id]
        # The None values are not required but its to shut up pyright
        # in case _HLS_TASK_FIELD_NAME overlaps with a valid param
        self._progress.update(
            task_id,
            total=None,
            completed=None,
            advance=None,
            description=None,
            visible=None,
            refresh=False,
            **{_HLS_TASK_FIELD_NAME: hls_task},
        )
        token = _current_hls_task.set(task_id)
        try:
            yield
        finally:
            self._remove_task(task)
            self._hls_progress.remove_task(task_id)
            _current_hls_task.reset(token)

    def new_hls_seg_task(self) -> ProgressHook:
        task_id = _current_hls_task.get()
        hls_task = self._hls_progress[task_id]

        def advance(amount: int) -> None:
            self._total_amount += amount
            self._hls_progress.advance(task_id, amount)

        def done() -> None:
            self._progress.advance(task_id, 1)

        def speed() -> float:
            return hls_task.finished_speed or hls_task.speed or 0

        return ProgressHook(advance, done, speed)
