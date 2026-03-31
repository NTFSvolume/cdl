# type: ignore[reportPrivateImportUsage]
from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console

from cyberdrop_dl import __version__
from cyberdrop_dl.ui.prompts import basic_prompts
from cyberdrop_dl.ui.prompts.defaults import EXIT_CHOICE
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager

console = Console()


def main_prompt(manager: Manager) -> int:
    """Main prompt for the program."""
    prompt_header(manager)
    OPTIONS = {
        "group_1": ["Download", "Retry failed downloads", "Create file hashes", "Sort files in download folder"],
        "group_2": ["Edit URLs.txt"],
        "group_3": ["View changelog"],
    }

    choices = basic_prompts.create_choices(OPTIONS, append_last=EXIT_CHOICE)

    return basic_prompts.ask_choice(choices)


def prompt_header(manager: Manager, title: str | None = None) -> None:
    clear_term()
    title = title or f"[bold]Cyberdrop Downloader ([blue]V{__version__!s}[/blue])[/bold]"
    console.print(title)
    console.print(f"[bold]Current config:[/bold] [blue]{manager.config_manager.loaded_config}[/blue]")
