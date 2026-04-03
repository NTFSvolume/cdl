# type: ignore[reportPrivateImportUsage]
from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator
from rich.console import Console

from cyberdrop_dl import __version__
from cyberdrop_dl.ui.prompts import basic_prompts
from cyberdrop_dl.ui.prompts.defaults import EXIT_CHOICE
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager

from cyberdrop_dl.ui.prompts.defaults import DONE_CHOICE

console = Console()


def main_prompt(manager: Manager) -> int:
    """Main prompt for the program."""
    prompt_header(manager)
    OPTIONS = {
        "group_1": ["Download", "Retry failed downloads", "Create file hashes", "Sort files in download folder"],
        "group_2": ["Edit URLs.txt"],
        "group_3": ["View changelog"],
    }

    choices = create_choices(OPTIONS, append_last=EXIT_CHOICE)

    return basic_prompts.ask_choice(choices)


def prompt_header(manager: Manager, title: str | None = None) -> None:
    clear_term()
    title = title or f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]"
    console.print(title)
    console.print(f"[bold]Current config:[/bold] [blue]{manager.config_manager.loaded_config}[/blue]")


def create_choices(
    options_groups: list[list[str]] | Mapping[str, list[list[str]]],
    append_last: Choice = DONE_CHOICE,
    *,
    disabled_choices: list[str] | None = None,
):
    if isinstance(options_groups, Mapping):
        options_groups = list(options_groups.values())
    disabled_choices = disabled_choices or []
    options = [option for group in options_groups for option in group]
    choices = []
    for index, option in enumerate(options, 1):
        enabled = option not in disabled_choices
        choices.append(Choice(index, option, enabled))
    choices.append(append_last)

    separator_indexes = []
    for group in options_groups:
        separator_indexes.append(len(group) + (separator_indexes[-1] if separator_indexes else 0))

    for count, index in enumerate(separator_indexes):
        choices.insert(index + count, Separator())

    return choices
