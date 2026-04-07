import pytest

from cyberdrop_dl.__main__ import main
from cyberdrop_dl.ui import program_ui


def test_startup(capsys: pytest.CaptureFixture[str]) -> None:
    # This is just to test that cyberdrop is able to run in the current python version
    msg = "main UI started successfully"

    def main_ui(*_) -> None:
        print(msg)

    with pytest.MonkeyPatch.context() as m:
        m.setattr(program_ui, "run", main_ui)
        main(())
        captured = capsys.readouterr()
        output = captured.out
        assert msg in output


def test_async_startup(caplog: pytest.LogCaptureFixture) -> None:
    main(("--download",))
    assert "Finished downloading. Enjoy :)" in caplog.text
