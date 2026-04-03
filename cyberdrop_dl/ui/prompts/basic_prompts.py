import sys
from pathlib import Path

from InquirerPy.prompts.filepath import FilePathPrompt
from InquirerPy.validator import PathValidator


def ask_dir_path(message: str = "Select dir path") -> Path:
    return Path(
        FilePathPrompt(
            message=message,
            validate=PathValidator(is_dir=True, message="Input is not a directory"),
            long_instruction="ARROW KEYS: Navigate | ENTER: Select",
            default=str(Path.home()),
        ).execute(),
    )


def enter_to_continue() -> None:
    if "pytest" in sys.modules:
        return
    _ = input("Press <ENTER> to continue")
