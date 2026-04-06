from pathlib import Path

import pytest

from cyberdrop_dl.__main__ import main
from cyberdrop_dl.ui import program_ui


def test_startup(tmp_cwd: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    # This is just to test that cyberdrop is able to run in the current python version
    msg = "main UI started successfully"

    def main_ui(*_) -> None:
        print(msg)

    monkeypatch.setattr(program_ui, "run", main_ui)
    main(())
    captured = capsys.readouterr()
    output = captured.out
    assert msg in output


def test_async_startup(tmp_cwd: Path, caplog: pytest.LogCaptureFixture) -> None:
    main(("--download",))
    assert "Finished downloading. Enjoy :)" in caplog.text
