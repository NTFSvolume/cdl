import logging
from pathlib import Path

import pytest

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
        with pytest.raises(RuntimeError):
            _ = logger.export_logs(size_limit=1)

    with pytest.raises(LookupError):
        _ = logger.export_logs()

    assert "Debug log file" in content
    for line in TEXT.splitlines():
        assert line in content


class TestBorrowLogger:
    def test_handlers_swapped_temporarily(self) -> None:
        root_handler = logging.StreamHandler()
        logger.logger.addHandler(root_handler)

        try:
            other = logging.getLogger("third_party")
            original_handler = logging.NullHandler()
            other.addHandler(original_handler)

            assert other.handlers == [original_handler]

            with logger.borrow_logger("third_party"):
                assert other.handlers == [root_handler]

            assert other.handlers == [original_handler]
        finally:
            logger.logger.removeHandler(root_handler)

    def test_level_and_propagate_restored(self) -> None:
        other = logging.getLogger("third_party_restore")
        other.setLevel(logging.WARNING)
        other.propagate = True

        with logger.borrow_logger("third_party_restore", level=logging.DEBUG):
            assert other.level == logging.DEBUG
            assert other.propagate is False

        assert other.level == logging.WARNING
        assert other.propagate is True

    def test_exception_inside_block_restores_state(self) -> None:
        other = logging.getLogger("third_party_exc")
        orig_handler = logging.NullHandler()
        other.addHandler(orig_handler)
        other.setLevel(logging.CRITICAL)

        with pytest.raises(RuntimeError):
            with logger.borrow_logger("third_party_exc", level=logging.INFO):
                assert other.level == logging.INFO
                raise RuntimeError("boom")

        assert other.handlers == [orig_handler]
        assert other.level == logging.CRITICAL
