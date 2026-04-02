from __future__ import annotations

import asyncio
import functools
import sys
from typing import TYPE_CHECKING, Any

from requests import request
from rich.console import Console
from rich.markdown import Markdown
from rich.text import Text

from cyberdrop_dl.clients.hash_client import hash_directory_scanner
from cyberdrop_dl.ui.prompts import user_prompts
from cyberdrop_dl.ui.prompts.basic_prompts import ask_dir_path, enter_to_continue
from cyberdrop_dl.ui.prompts.defaults import DONE_CHOICE, EXIT_CHOICE
from cyberdrop_dl.utils import text_editor
from cyberdrop_dl.utils.sorting import Sorter
from cyberdrop_dl.utils.utilities import clear_term

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
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

    def run(self) -> bool | None:
        done = False
        while not done:
            done = self._run()
        return done

    def _run(self) -> bool | None:
        """Program UI."""
        clear_term()
        options_map = {
            1: self._download,
            2: self._retry_failed_download,
            3: self._scan_and_create_hashes,
            4: self._sort_files,
            5: self._edit_urls,
            6: self._view_changelog,
        }

        answer = user_prompts.main_prompt(self.manager)
        result = self._process_answer(answer, options_map)
        return_to_main = result and result != DONE_CHOICE
        if return_to_main:
            clear_term()
        return return_to_main

    def _download(self) -> bool:
        """Starts download process."""
        return True

    def _retry_failed_download(self) -> bool:
        """Sets retry failed and starts download process."""
        self.manager.parsed_args.cli_only_args.retry_failed = True
        return True

    def _scan_and_create_hashes(self) -> None:
        """Scans a folder and creates hashes for all of its files."""
        path = ask_dir_path("Select the directory to scan", default=str(self.manager.config.files.download_folder))
        hash_directory_scanner(self.manager, path)

    def _sort_files(self) -> None:
        """Sort files in download folder"""
        sorter = Sorter.from_manager(self.manager)
        asyncio.run(sorter.run())

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

    def _process_answer(
        self, answer: Any, options_map: Mapping[int, Callable[[], bool | None]]
    ) -> Choice | bool | None:
        """Checks prompt answer and executes corresponding function."""
        if answer == EXIT_CHOICE.value:
            sys.exit(0)
        if answer == DONE_CHOICE.value:
            return DONE_CHOICE

        function_to_call = options_map.get(answer)
        if not function_to_call:
            self.print_error("Something went wrong. Please report it to the developer", critical=True)
            sys.exit(1)

        return function_to_call()


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
