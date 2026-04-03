# type: ignore[reportPrivateImportUsage]
import sys
from pathlib import Path

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.validator import PathValidator

from cyberdrop_dl.ui.prompts.defaults import DEFAULT_OPTIONS


def ask_choice(choices: list[Choice], *, message: str = "What would you like to do:", **kwargs):
    options = DEFAULT_OPTIONS | kwargs
    return inquirer.select(message=message, choices=choices, **options).execute()


def ask_path(message: str = "Select path", *, validator_options: dict | None = None, **kwargs) -> Path:
    options = DEFAULT_OPTIONS | {"default": str(Path.home())} | kwargs
    return Path(
        inquirer.filepath(message=message, validate=PathValidator(**(validator_options or {})), **options).execute()
    )


def ask_dir_path(message: str = "Select dir path", **kwargs) -> Path:
    options = DEFAULT_OPTIONS | kwargs
    validator_options = {"is_dir": True, "message": "Input is not a directory"}
    return ask_path(message, validator_options=validator_options, **options)


def enter_to_continue(message: str = "Press <ENTER> to continue", **kwargs):
    if "pytest" in sys.modules:
        return
    options = DEFAULT_OPTIONS | {"long_instruction": "ENTER: continue"} | kwargs
    msg = f"\n{message}"
    return inquirer.confirm(message=msg, qmark="", **options).execute()
