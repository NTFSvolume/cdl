from pathlib import Path

import pytest
from pydantic import ValidationError

from cyberdrop_dl.cli import parse_args
from cyberdrop_dl.main import run


@pytest.mark.parametrize(
    "command, text",
    [
        ("--help", "Bulk asynchronous downloader for multiple file hosts"),
        ("--show-supported-sites", "cyberdrop-dl supported sites"),
    ],
)
def test_command_by_console_output(tmp_cwd: Path, capsys: pytest.CaptureFixture[str], command: str, text: str) -> None:
    try:
        run(command.split())
    except SystemExit:
        pass
    output = capsys.readouterr().out
    assert text in output


def test_impersonate_defaults_to_true_with_no_args() -> None:
    result = parse_args(["--download"])
    assert result.cli_only_args.impersonate is None
    result = parse_args(["--impersonate"])
    assert result.cli_only_args.impersonate is True


def test_impersonate_accepts_valid_targets() -> None:
    result = parse_args(["--download", "--impersonate", "chrome"])
    assert result.cli_only_args.impersonate == "chrome"


def test_impersonate_does_not_accepts_invalid_values() -> None:
    with pytest.raises(ValidationError):
        parse_args(["--impersonate", "not_a_browser"])
