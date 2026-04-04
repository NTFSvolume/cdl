from __future__ import annotations

import asyncio
import dataclasses
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
import inquirer
import inquirer.questions
from inquirer.themes import BlueComposure
from rich.console import Console
from rich.markdown import Markdown
from rich.text import Text

from cyberdrop_dl import __version__
from cyberdrop_dl.clients.hash_client import hash_directory_scanner
from cyberdrop_dl.progress import hyperlink
from cyberdrop_dl.utils import text_editor
from cyberdrop_dl.utils.sorting import Sorter

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from cyberdrop_dl.managers.manager import Manager


console = Console()
_ERROR = Text("ERROR: ", style="bold red")
_CHANGELOG_URL = "https://raw.githubusercontent.com/NTFSvolume/cdl/refs/heads/main/CHANGELOG.md"
_changelog: str = ""


@dataclasses.dataclass(slots=True, frozen=True)
class ProgramUI:
    manager: Manager
    choices: dict[str, Callable[[], bool | None]] = dataclasses.field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.choices.update(
            {
                "Download": lambda: True,
                "Retry failed downloads": self._retry_failed_download,
                "Create file hashes": self._scan_and_create_hashes,
                "Sort files in download folder": self._sort_files,
                "Edit URLs.txt": self._edit_urls,
                "View changelog": self._view_changelog,
                "Exit": lambda: sys.exit(0),
            }
        )

    def run(self) -> None:
        while True:
            _app_header(self.manager)
            answer = _ask_choices(self.choices)
            done = self.choices[answer]()
            if done:
                break

    def _retry_failed_download(self) -> bool:
        self.manager.parsed_args.cli_only_args.retry_failed = True
        return True

    def _scan_and_create_hashes(self) -> None:
        path = _ask_dir(
            "Select the directory to scan",
            default=self.manager.config.files.download_folder,
        )
        asyncio.run(hash_directory_scanner(self.manager, path))
        _enter_to_continue()

    def _sort_files(self) -> None:
        sorter = Sorter.from_manager(self.manager)
        console.print(
            f"You are about to sort files from '{sorter.input_dir}' to '{sorter.output_dir}'", style="bold red"
        )
        answer = input("Type 'YES' to proceed")
        if answer.strip().casefold() == "yes":
            asyncio.run(sorter.run())
            _enter_to_continue()

    def _view_changelog(self) -> None:
        global _changelog

        _clear_term()

        if not _changelog:
            try:
                _changelog = asyncio.run(_get_changelog())
            except Exception:
                console.print(_ERROR, "UNABLE TO GET CHANGELOG INFORMATION")
                _enter_to_continue()
                return None

        with console.pager(links=True):
            console.print(Markdown(_changelog, justify="left"))

    def _edit_urls(self) -> None:
        try:
            text_editor.open(self.manager.config.files.input_file)
        except ValueError as e:
            console.print(_ERROR, str(e))
            _enter_to_continue()


async def _get_changelog() -> str:
    async with aiohttp.request(
        "GET",
        _CHANGELOG_URL,
        raise_for_status=True,
    ) as response:
        return await response.text()


def _app_header(manager: Manager) -> None:
    _clear_term()
    console.print(f"[bold]cyberdrop-dl ([blue]v{__version__!s}[/blue])[/bold]")
    console.print(f"config file: [blue]{hyperlink(manager.config_manager.settings)}[/blue]\n")


def _ask_choices(choices: Iterable[str]) -> str:
    return _ask(
        inquirer.List(
            "main",
            message="What would you like to do",
            choices=list(choices),
        )
    )


def _ask(question: inquirer.questions.Question) -> str:
    answers = inquirer.prompt(
        [question],
        raise_keyboard_interrupt=True,
        theme=BlueComposure(),
    )
    assert answers
    return next(iter(answers.values()))


def _ask_dir(message: str = "Select dir path", default: Path = Path.home()) -> Path:  # noqa: B008
    while True:
        try:
            answer = _ask(
                inquirer.Text(
                    "dir",
                    message=message,
                    default=default,
                )
            )

            path = Path(answer)
            if not path.exists():
                raise FileNotFoundError(answer)
            if not path.is_dir():
                raise NotADirectoryError(answer)

        except (NotADirectoryError, FileNotFoundError) as e:
            console.print(_ERROR, repr(e))
        else:
            return path


def _enter_to_continue() -> None:
    if "pytest" in sys.modules:
        return
    _ = input("Press <ENTER> to continue")


def _clear_term() -> None:
    _ = os.system("cls" if os.name == "nt" else "clear")
