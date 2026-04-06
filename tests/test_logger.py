import logging
from pathlib import Path

from cyberdrop_dl.utils import logger

TEXT = "\n".join(f"line {idx}" for idx in range(1, 5))


def test_logs_capture() -> None:
    with logger.capture_logs() as file:
        logger.logger.setLevel(logging.DEBUG)

        for line in TEXT.splitlines():
            logger.logger.debug(line)

    assert file.getvalue() == TEXT + "\n"


def test_export_logs(tmp_path: Path) -> None:
    log_file = tmp_path / "test-log.log"
    with logger.setup_logging(log_file):
        for line in TEXT.splitlines():
            logger.logger.debug(line)

        content = logger.export_logs().decode("utf8")

    assert "Debug log file" in content
    for line in TEXT.splitlines():
        assert line in content
