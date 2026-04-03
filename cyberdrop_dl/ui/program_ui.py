from __future__ import annotations

import asyncio
import dataclasses
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
from cyberdrop_dl.progress import hyperlink
from cyberdrop_dl.utils import text_editor
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from cyberdrop_dl.managers.manager import Manager


console = Console()
_ERROR = Text("ERROR: ", style="bold red")
_CHANGELOG_URL = "https://raw.githubusercontent.com/NTFSvolume/cdl/refs/heads/main/CHANGELOG.md"
_EXIT = "Exit"
_changelog: str = ""


@dataclasses.dataclass(slots=True)
class ProgramUI:
    manager: Manager

    def run(self) -> None:
        while True:
            exit = self._show_prompt()
            if exit:
                break

    def _show_prompt(self) -> bool | None:
        _app_header(self.manager)
        choices = {
            "Download": lambda: True,
            "Retry failed downloads": self._retry_failed_download,
            "Create file hashes": self._scan_and_create_hashes,
            "Sort files in download folder": self._sort_files,
            None: lambda: None,
            "Edit URLs.txt": self._edit_urls,
            "View changelog": self._view_changelog,
        }

        answer = _ask_choices(_create_choices(choices))
        if answer == _EXIT:
            sys.exit(0)

        return choices[answer]()

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
                console.print(_ERROR, "UNABLE TO GET CHANGELOG INFORMATION")
                enter_to_continue()
                return None

        with console.pager(links=True):
            console.print(Markdown(_changelog, justify="left"))

    def _edit_urls(self) -> None:
        try:
            text_editor.open(self.manager.config.files.input_file)
        except ValueError as e:
            console.print(_ERROR, str(e))
            enter_to_continue()


async def _get_changelog() -> str:
    async with aiohttp.request(
        "GET",
        _CHANGELOG_URL,
        raise_for_status=True,
    ) as response:
        return await response.text()


def _app_header(manager: Manager) -> None:
    clear_term()
    console.print(f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]")
    console.print(f"Config file: [blue]{hyperlink(manager.config_manager.settings)}[/blue]\n")


def _create_choices(options_groups: Iterable[str | None]) -> Generator[Choice | Separator]:
    for option in options_groups:
        if option is None:
            yield Separator()
        else:
            yield Choice(option, option)

    yield Separator()
    yield Choice(_EXIT)


def _ask_choices(choices: Iterable[Choice | Separator]) -> str:
    return ListPrompt(
        message="What would you like to do:",
        choices=list(choices),
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
