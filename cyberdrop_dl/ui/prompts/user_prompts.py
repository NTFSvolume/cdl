from __future__ import annotations

from typing import TYPE_CHECKING

from InquirerPy.base.control import Choice
from InquirerPy.prompts.list import ListPrompt
from InquirerPy.separator import Separator
from rich.console import Console

from cyberdrop_dl import __version__
from cyberdrop_dl.ui.prompts.basic_prompts import enter_to_continue
from cyberdrop_dl.ui.prompts.defaults import EXIT_CHOICE
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from cyberdrop_dl.managers.manager import Manager


console = Console()


def main_prompt(manager: Manager) -> int:
    prompt_header(manager)
    enter_to_continue()
    choices = create_choices(
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

    return ask_choice(list(choices))


def prompt_header(manager: Manager, title: str | None = None) -> None:
    clear_term()
    title = title or f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]"
    console.print(title)
    console.print(f"[bold]Current config:[/bold] [blue]{manager.config_manager.loaded_config}[/blue]")


def create_choices(options_groups: Iterable[Iterable[str]]) -> Generator[Choice | Separator]:
    for group in options_groups:
        for index, option in enumerate(group, 1):
            yield Choice(index, option, enabled=True)

        yield Separator()

    yield EXIT_CHOICE


def ask_choice(choices: list[Choice | Separator]) -> int:
    return ListPrompt(
        message="What would you like to do:",
        choices=choices,
        long_instruction="ARROW KEYS: Navigate | ENTER: Select",
    ).execute()
