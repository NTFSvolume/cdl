from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
from InquirerPy.base.control import Choice
from InquirerPy.prompts.filepath import FilePathPrompt
from InquirerPy.prompts.list import ListPrompt
from InquirerPy.separator import Separator
from InquirerPy.validator import PathValidator
from rich.console import Console
from rich.markdown import Markdown
from rich.text import Text

from cyberdrop_dl import __version__
from cyberdrop_dl.clients.hash_client import hash_directory_scanner
from cyberdrop_dl.utils import text_editor
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from cyberdrop_dl.managers.manager import Manager


console = Console()
ERROR_PREFIX = Text("ERROR: ", style="bold red")
_changelog: str = ""
_CHANGELOG_URL = "https://raw.githubusercontent.com/NTFSvolume/cdl/refs/heads/main/CHANGELOG.md"
_EXIT_CHOICE = Choice("Exit")
_DONE_CHOICE = Choice("Done")


class ProgramUI:
    def __init__(self, manager: Manager, run: bool = True) -> None:
        self.manager = manager
        if run:
            self.run()

    def run(self) -> None:
        done = False
        while not done:
            done = self._run()

    def _run(self) -> Choice | bool | None:
        clear_term()
        answer = _main_prompt(self.manager)

        if answer == _EXIT_CHOICE.value:
            sys.exit(0)
        if answer == _DONE_CHOICE.value:
            return _DONE_CHOICE

        return {
            1: self._download,
            2: self._retry_failed_download,
            3: self._scan_and_create_hashes,
            4: self._sort_files,
            5: self._edit_urls,
            6: self._view_changelog,
        }[answer]()

    def _download(self) -> bool:
        return True

    def _retry_failed_download(self) -> bool:
        self.manager.parsed_args.cli_only_args.retry_failed = True
        return True

    def _scan_and_create_hashes(self) -> None:
        path = ask_dir_path(
            "Select the directory to scan",
            default=self.manager.config.files.download_folder,
        )
        asyncio.run(hash_directory_scanner(self.manager, path))
        enter_to_continue()

    def _sort_files(self) -> None:
        sorter = Sorter.from_manager(self.manager)
        console.print(
            f"You are about to sort files from '{sorter.input_dir}' to '{sorter.output_dir}'", style="bold red"
        )
        answer = input("Type 'YES' to proceed")
        if answer.strip().casefold() == "yes":
            asyncio.run(sorter.run())
            enter_to_continue()

    def _view_changelog(self) -> None:
        global _changelog

        clear_term()

        if not _changelog:
            try:
                _changelog = asyncio.run(_get_changelog())
            except Exception:
                console.print(ERROR_PREFIX, "UNABLE TO GET CHANGELOG INFORMATION")
                enter_to_continue()
                return None

        with console.pager(links=True):
            console.print(Markdown(_changelog, justify="left"))

    def _edit_urls(self) -> None:
        try:
            text_editor.open(self.manager.config.files.input_file)
        except ValueError as e:
            console.print(ERROR_PREFIX, str(e))
            enter_to_continue()


async def _get_changelog() -> str:
    async with aiohttp.request("GET", _CHANGELOG_URL, raise_for_status=True) as response:
        content = await response.text()

    lines = content.splitlines()
    # remove keep_a_changelog disclaimer
    return "\n".join(lines[:21] + lines[25:])


def _main_prompt(manager: Manager) -> int:
    _prompt_header(manager)
    enter_to_continue()
    choices = _create_choices(
        [
            [
                "Download",
                "Retry failed downloads",
                "Create file hashes",
                "Sort files in download folder",
            ],
            ["Edit URLs.txt"],
            ["View changelog"],
        ]
    )

    return _ask_choices(list(choices))


def _prompt_header(manager: Manager) -> None:
    clear_term()
    console.print(f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]")
    console.print(f"[bold]Current config:[/bold] [blue]{manager.config_manager.loaded_config}[/blue]")


def _create_choices(options_groups: Iterable[Iterable[str]]) -> Generator[Choice | Separator]:
    for group in options_groups:
        for index, option in enumerate(group, 1):
            yield Choice(index, option, enabled=True)

        yield Separator()

    yield _EXIT_CHOICE


def _ask_choices(choices: list[Choice | Separator]) -> int:
    return ListPrompt(
        message="What would you like to do:",
        choices=choices,
        long_instruction="ARROW KEYS: Navigate | ENTER: Select",
    ).execute()


def ask_dir_path(message: str = "Select dir path", default: Path = Path.home()) -> Path:  # noqa: B008
    return Path(
        FilePathPrompt(
            message=message,
            validate=PathValidator(is_dir=True, message="Input is not a directory"),
            long_instruction="ARROW KEYS: Navigate | ENTER: Select",
            default=str(default),
        ).execute(),
    )


def enter_to_continue() -> None:
    if "pytest" in sys.modules:
        return
    _ = input("Press <ENTER> to continue")
