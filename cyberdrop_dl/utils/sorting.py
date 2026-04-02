from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import shutil
from pathlib import Path
from subprocess import CalledProcessError
from typing import TYPE_CHECKING, Self

import imagesize

from cyberdrop_dl import ffmpeg
from cyberdrop_dl.constants import FileExt, TempExt
from cyberdrop_dl.utils import strings
from cyberdrop_dl.utils.utilities import purge_dir_tree as delete_empty_files_and_folders

if TYPE_CHECKING:
    from collections.abc import Iterable

    from cyberdrop_dl.managers.live_manager import LiveManager
    from cyberdrop_dl.managers.manager import Manager
    from cyberdrop_dl.managers.progress_manager import ProgressManager


logger = logging.getLogger(__name__)


async def _get_modified_date(file: Path) -> datetime.datetime:
    stat = await asyncio.to_thread(file.stat)
    return datetime.datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0)


@dataclasses.dataclass(slots=True, frozen=True)
class Sorter:
    live_manager: LiveManager
    tui: ProgressManager
    input_dir: Path
    output_dir: Path

    incrementer_format: str
    audio_format: str | None
    image_format: str | None
    video_format: str | None
    other_format: str | None

    @classmethod
    def from_manager(cls, manager: Manager) -> Self:
        settings = manager.config_manager.settings_data.sorting
        return cls(
            manager.live_manager,
            tui=manager.progress_manager,
            input_dir=manager.config.sorting.scan_folder or manager.config.files.download_folder,
            output_dir=manager.config.sorting.sort_folder,
            incrementer_format=manager.config_manager.settings_data.sorting.sort_incrementer_format,
            audio_format=settings.sorted_audio,
            image_format=settings.sorted_image,
            video_format=settings.sorted_video,
            other_format=settings.sorted_other,
        )

    async def run(self) -> None:
        """Sorts the files in the download directory into their respective folders."""
        if not await asyncio.to_thread(self.input_dir.is_dir):
            logger.error(f"Sort directory ('{self.input_dir}' does not exist", extra={"color": "red"})
            return

        logger.info("Sorting downloads...", extra={"color": "cyan"})
        await asyncio.to_thread(self.output_dir.mkdir, parents=True, exist_ok=True)

        with self.live_manager.get_sort_live(stop=True):
            subfolders = await asyncio.to_thread(_subfolders, self.input_dir)
            await self._sort_files(subfolders)
            logger.info("DONE!", extra={"color": "green"})
            _ = delete_empty_files_and_folders(self.input_dir)

    async def _sort_files(self, folders: Iterable[Path]) -> None:
        for fut in asyncio.as_completed(asyncio.to_thread(_get_files, f) for f in folders):
            folder, files = await fut
            folder_name = folder.name
            self.tui.sort_progress.queue_length += len(files)
            task_id = self.tui.sort_progress.add_task(folder_name, len(files))
            try:

                async def sort(file: Path, name: str = folder_name, task_id=task_id) -> None:
                    try:
                        await self.__sort(name, file)
                    finally:
                        self.tui.sort_progress.advance_folder(task_id)
                        self.tui.sort_progress.queue_length -= 1

                _ = await asyncio.gather(*map(sort, files))
            finally:
                self.tui.sort_progress.remove_task(task_id)

    async def __sort(self, folder_name: str, file: Path) -> None:
        ext = file.suffix.lower()
        if ext in TempExt:
            return

        if ext in FileExt.AUDIO:
            return await self.sort_audio(file, folder_name)
        if ext in FileExt.IMAGE:
            return await self.sort_image(file, folder_name)
        if ext in FileExt.VIDEO:
            return await self.sort_video(file, folder_name)

        await self.sort_other(file, folder_name)

    async def sort_audio(self, file: Path, base_name: str) -> None:
        """Sorts an audio file into the sorted audio folder."""
        if not self.audio_format:
            return

        bitrate = duration = sample_rate = None
        probe_output = await _try_probe("audio", file)
        if probe_output and (audio := probe_output.audio):
            duration = audio.duration or probe_output.format.duration
            bitrate = audio.bitrate
            sample_rate = audio.sample_rate

        if await self._move_file(
            file,
            base_name,
            self.audio_format,
            bitrate=bitrate,
            duration=duration,
            length=duration,
            sample_rate=sample_rate,
        ):
            self.tui.sort_progress.increment_audio()

    async def sort_image(self, file: Path, base_name: str) -> None:
        """Sorts an image file into the sorted image folder."""
        if not self.image_format:
            return

        height = resolution = width = None
        try:
            width, height = await asyncio.to_thread(imagesize.get, file)
            if width > 0 and height > 0:
                resolution = f"{width}x{height}"

        except (OSError, ValueError):
            logger.exception(f"Unable to get some image properties of '{file}'")

        if await self._move_file(
            file,
            base_name,
            self.image_format,
            height=height,
            resolution=resolution,
            width=width,
        ):
            self.tui.sort_progress.increment_image()

    async def sort_video(self, file: Path, base_name: str) -> None:
        """Sorts a video file into the sorted video folder."""
        if not self.video_format:
            return

        codec = duration = framerate = height = resolution = width = None
        probe_output = await _try_probe("video", file)
        if probe_output and (video := probe_output.video):
            width = video.width
            height = video.height
            resolution = video.resolution
            codec = video.codec
            duration = video.duration or probe_output.format.duration
            framerate = video.fps

        if await self._move_file(
            file,
            base_name,
            self.video_format,
            codec=codec,
            duration=duration,
            fps=framerate,
            height=height,
            resolution=resolution,
            width=width,
        ):
            self.tui.sort_progress.increment_video()

    async def sort_other(self, file: Path, base_name: str) -> None:
        if not self.other_format:
            return

        if await self._move_file(file, base_name, self.other_format):
            self.tui.sort_progress.increment_other()

    async def _move_file(self, file: Path, base_name: str, format_str: str, /, **kwargs: object) -> bool:
        file_date = await _get_modified_date(file)
        file_date_us = file_date.strftime("%Y-%d-%m")
        file_date_iso = file_date.strftime("%Y-%m-%d")

        duration = kwargs.get("duration") or kwargs.get("length")
        if duration is not None:
            kwargs["duration"] = kwargs["length"] = duration

        dest, _ = strings.safe_format(
            format_str,
            base_dir=base_name,
            ext=file.suffix,
            file_date=file_date,
            file_date_iso=file_date_iso,
            file_date_us=file_date_us,
            filename=file.stem,
            parent_dir=file.parent.name,
            sort_dir=self.output_dir,
            **kwargs,
        )

        dest = Path(dest)
        return await asyncio.to_thread(_move_file, file, dest, self.incrementer_format)


def _subfolders(directory: Path) -> tuple[Path, ...]:
    def walk():
        for subfolder in directory.resolve().iterdir():
            if subfolder.is_dir():
                yield subfolder

    return tuple(walk())


def _get_files(directory: Path) -> tuple[Path, tuple[Path, ...]]:
    """Finds all files in a directory and returns them in a list."""

    def walk():
        for file in directory.resolve().rglob("*"):
            if file.is_file():
                yield file

    return directory, tuple(walk())


def _move_file(
    source: Path,
    dest: Path,
    incrementer_format: str = "{i}",
    *,
    max_retries: int = 10,
) -> bool:
    """Moves a file to a destination folder."""

    dest = dest.resolve()
    if source == dest:
        return True

    dest_parent = dest.parent
    dest_parent.mkdir(parents=True, exist_ok=True)
    source_size = source.stat().st_size

    try:
        _ = shutil.move(source, dest)
    except FileExistsError:
        dest_stem = dest.stem
        for auto_index in range(1, max_retries + 1):
            if source_size == dest.stat().st_size:
                source.unlink()
                logger.debug("Moved '{}' to '{}'", source, dest)
                return True

            new_filename = f"{dest_stem}{incrementer_format.format(i=auto_index)}{dest.suffix}"
            logger.warning(
                "Found name collision when moving '{}' to '{}'. Retring with '{}'",
                source,
                dest,
                dest := dest_parent / new_filename,
            )

            try:
                _ = shutil.move(source, dest)
            except FileExistsError:
                continue
            except OSError:
                logger.exception("Unable to move '{}'", source)
                return False
            else:
                return True
        else:
            logger.error("Unable to move '{}'. Giving up after {} attempts", source, max_retries)
            return False

    except OSError:
        logger.exception("Unable to move '{}'", source)
        return False

    else:
        logger.debug("Moved '{}' to '{}'", source, dest)
        return True


async def _try_probe(kind: str, file: Path) -> ffmpeg.FFprobeResult | None:
    try:
        return await ffmpeg.probe(file)

    except (RuntimeError, CalledProcessError, OSError):
        logger.exception(f"Unable to get {kind} properties of '{file}'")
