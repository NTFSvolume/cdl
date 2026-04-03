from __future__ import annotations

import asyncio
import functools
import sys
from typing import TYPE_CHECKING

from requests import request
from rich.console import Console
from rich.markdown import Markdown
from rich.text import Text

from cyberdrop_dl.clients.hash_client import hash_directory_scanner
from cyberdrop_dl.ui.prompts import DONE_CHOICE, EXIT_CHOICE, ask_dir_path, enter_to_continue, main_prompt
from cyberdrop_dl.utils import text_editor
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from pathlib import Path

    from InquirerPy.base.control import Choice

    from cyberdrop_dl.managers.manager import Manager


console = Console()
ERROR_PREFIX = Text("ERROR: ", style="bold red")


class ProgramUI:
    def __init__(self, manager: Manager, run: bool = True) -> None:
        self.manager = manager
        if run:
            self.run()

    @staticmethod
    def print_error(msg: str, critical: bool = False) -> None:
        text = ERROR_PREFIX + msg
        console.print(text, style="bold red" if critical else None)
        if critical:
            sys.exit(1)
        enter_to_continue()

    def run(self) -> None:
        done = False
        while not done:
            done = self._run()

    def _run(self) -> Choice | bool | None:
        clear_term()
        answer = main_prompt(self.manager)

        if answer == EXIT_CHOICE.value:
            sys.exit(0)
        if answer == DONE_CHOICE.value:
            return DONE_CHOICE

        return {
            1: self._download,
            2: self._retry_failed_download,
            3: self._scan_and_create_hashes,
            4: self._sort_files,
            5: self._edit_urls,
            6: self._view_changelog,
        }[answer]()

    def _download(self) -> bool:
        """Starts download process."""
        return True

    def _retry_failed_download(self) -> bool:
        """Sets retry failed and starts download process."""
        self.manager.parsed_args.cli_only_args.retry_failed = True
        return True

    def _scan_and_create_hashes(self) -> None:
        """Scans a folder and creates hashes for all of its files."""
        path = ask_dir_path("Select the directory to scan", default=self.manager.config.files.download_folder)
        asyncio.run(hash_directory_scanner(self.manager, path))
        enter_to_continue()

    def _sort_files(self) -> None:
        """Sort files in download folder"""
        sorter = Sorter.from_manager(self.manager)
        console.print(
            f"You are about to sort files from '{sorter.input_dir}' to '{sorter.output_dir}'", style="bold red"
        )
        answer = input("Type 'YES' to proceed")
        if answer.strip().casefold() == "yes":
            asyncio.run(sorter.run())
            enter_to_continue()

    def _view_changelog(self) -> None:
        clear_term()
        try:
            changelog_content = _get_changelog()
        except Exception:
            self.print_error("UNABLE TO GET CHANGELOG INFORMATION")
            return None

        with console.pager(links=True):
            console.print(Markdown(changelog_content, justify="left"))

    def _edit_urls(self) -> None:
        self._open_in_text_editor(self.manager.config.files.input_file, reload_config=False)

    def _open_in_text_editor(self, file_path: Path, *, reload_config: bool = True):
        try:
            text_editor.open(file_path)
        except ValueError as e:
            self.print_error(str(e))
            return
        if reload_config:
            console.print("Revalidating config, please wait..")
            self.manager.config_manager.reload_config()


@functools.cache
def _get_changelog() -> str:
    """Get latest changelog file from github. Returns its content."""

    url = "https://raw.githubusercontent.com/NTFSvolume/cdl/refs/heads/main/CHANGELOG.md"
    with request("GET", url, timeout=15) as response:
        response.raise_for_status()
        content = response.text

    lines = content.splitlines()
    # remove keep_a_changelog disclaimer
    return "\n".join(lines[:21] + lines[25:])
