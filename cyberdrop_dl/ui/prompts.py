from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from InquirerPy.base.control import Choice
from InquirerPy.prompts.filepath import FilePathPrompt
from InquirerPy.prompts.list import ListPrompt
from InquirerPy.separator import Separator
from InquirerPy.validator import PathValidator
from rich.console import Console

from cyberdrop_dl import __version__
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from cyberdrop_dl.managers.manager import Manager


console = Console()


EXIT_CHOICE = Choice("Exit")
DONE_CHOICE = Choice("Done")


def main_prompt(manager: Manager) -> int:
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


def _prompt_header(manager: Manager, title: str | None = None) -> None:
    clear_term()
    title = title or f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]"
    console.print(title)
    console.print(f"[bold]Current config:[/bold] [blue]{manager.config_manager.loaded_config}[/blue]")


def _create_choices(options_groups: Iterable[Iterable[str]]) -> Generator[Choice | Separator]:
    for group in options_groups:
        for index, option in enumerate(group, 1):
            yield Choice(index, option, enabled=True)

        yield Separator()

    yield EXIT_CHOICE


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
