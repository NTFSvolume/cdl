from __future__ import annotations

import asyncio
import dataclasses
import datetime
import itertools
import logging
from pathlib import Path
from subprocess import CalledProcessError
from typing import TYPE_CHECKING, Any

import imagesize

from cyberdrop_dl import config, constants
from cyberdrop_dl.utils import delete_empty_files_and_folders, strings
from cyberdrop_dl.utils.ffmpeg import probe

if TYPE_CHECKING:
    from collections.abc import Iterable

    from cyberdrop_dl.tui import TUI

logger = logging.getLogger(__name__)


async def _get_modified_date(file: Path) -> datetime.datetime:
    stat = await asyncio.to_thread(file.stat)
    return datetime.datetime.fromtimestamp(stat.st_mtime, datetime.UTC).replace(microsecond=0, tzinfo=None)


@dataclasses.dataclass(slots=True)
class Sorter:
    tui: TUI
    input_dir: Path
    output_dir: Path

    incrementer_format: str
    audio_format: str | None
    image_format: str | None
    video_format: str | None
    other_format: str | None

    @classmethod
    def from_config(cls, tui: TUI, config: config.Config) -> Sorter:
        return cls(
            tui=tui,
            input_dir=config.sorting.scan_folder or config.files.download_folder,
            output_dir=config.sorting.sort_folder,
            incrementer_format=config.sorting.sort_incrementer_format,
            audio_format=config.sorting.sorted_audio,
            image_format=config.sorting.sorted_image,
            video_format=config.sorting.sorted_video,
            other_format=config.sorting.sorted_other,
        )

    async def run(self) -> None:
        """Sorts the files in the download directory into their respective folders."""
        if not await asyncio.to_thread(self.input_dir.is_dir):
            logger.error(f"Sort directory ('{self.input_dir}' does not exist", extra={"color": "red"})
            return

        logger.info("Sorting downloads...", extra={"color": "cyan"})
        await asyncio.to_thread(self.output_dir.mkdir, parents=True, exist_ok=True)

        with self.tui(screen="sorting"):
            subfolders = await asyncio.to_thread(_subfolders, self.input_dir)
            await self._sort_files(subfolders)
            logger.info("DONE!", extra={"color": "green"})
            _ = delete_empty_files_and_folders(self.input_dir)

    async def _sort_files(self, folders: Iterable[Path]) -> None:
        for fut in asyncio.as_completed(asyncio.to_thread(_get_files, f) for f in folders):
            folder, files = await fut
            folder_name = folder.name
            with self.tui.sorting(folder_name, total=len(files)) as progress:

                async def sort(file: Path, name: str = folder_name) -> None:
                    try:
                        await self.__sort(name, file)
                    finally:
                        progress.advance(1)

                _ = await asyncio.gather(*map(sort, files))

    async def __sort(self, folder_name: str, file: Path) -> None:
        ext = file.suffix.lower()
        if ext in constants.TempExt:
            return

        if ext in constants.FileFormats.AUDIO:
            await self.sort_audio(file, folder_name)
        elif ext in constants.FileFormats.IMAGE:
            await self.sort_image(file, folder_name)
        elif ext in constants.FileFormats.VIDEO:
            await self.sort_video(file, folder_name)
        else:
            await self.sort_other(file, folder_name)

    async def sort_audio(self, file: Path, base_name: str) -> None:
        """Sorts an audio file into the sorted audio folder."""
        if not self.audio_format:
            return

        bitrate = duration = sample_rate = None
        try:
            probe_output = await probe(file)
            if audio := probe_output.audio:
                duration = audio.duration or probe_output.format.duration
                bitrate = audio.bitrate
                sample_rate = audio.sample_rate

        except (RuntimeError, CalledProcessError, OSError):
            logger.exception(f"Unable to get audio properties of '{file}'")

        if await self._move_file(
            file,
            base_name,
            self.audio_format,
            bitrate=bitrate,
            duration=duration,
            length=duration,
            sample_rate=sample_rate,
        ):
            self.tui.sorting.add_audio()

    async def sort_image(self, file: Path, base_name: str) -> None:
        """Sorts an image file into the sorted image folder."""
        if not self.image_format:
            return

        height = resolution = width = None
        try:
            width, height = await asyncio.to_thread(imagesize.get, file)
            if width > 0 and height > 0:
                resolution = f"{width}x{height}"
            else:
                # imagesize returns (-1, -1) for unsupported/corrupted images
                width = height = resolution = None

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
            self.tui.sorting.add_image()

    async def sort_video(self, file: Path, base_name: str) -> None:
        """Sorts a video file into the sorted video folder."""
        if not self.video_format:
            return

        codec = duration = framerate = height = resolution = width = None

        try:
            probe_output = await probe(file)
            if video := probe_output.video:
                width = video.width
                height = video.height
                resolution = video.resolution
                codec = video.codec
                duration = video.duration or probe_output.format.duration
                framerate = video.fps

        except (RuntimeError, CalledProcessError, OSError):
            logger.exception(f"Unable to get some video properties of '{file}'")

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
            self.tui.sorting.add_video()

    async def sort_other(self, file: Path, base_name: str) -> None:
        """Sorts an other file into the sorted other folder."""
        if not self.other_format:
            return

        if await self._move_file(file, base_name, self.other_format):
            self.tui.sorting.add_other()

    async def _move_file(self, file: Path, base_name: str, format_str: str, /, **kwargs: Any) -> bool:
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


def _move_file(old_path: Path, new_path: Path, incrementer_format: str) -> bool:
    """Moves a file to a destination folder."""

    new_path = new_path.resolve()
    if old_path == new_path:
        return True

    new_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        _ = old_path.rename(new_path)
    except FileExistsError:
        if old_path.stat().st_size == new_path.stat().st_size:
            old_path.unlink()
            return True
        for auto_index in itertools.count(1):
            new_filename = f"{new_path.stem}{incrementer_format.format(i=auto_index)}{new_path.suffix}"
            possible_new_path = new_path.parent / new_filename
            try:
                _ = old_path.rename(possible_new_path)
                break
            except FileExistsError:
                continue
    except OSError:
        return False

    return True
