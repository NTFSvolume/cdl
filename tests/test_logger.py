import logging

from cyberdrop_dl.utils import logger

TEXT = "\n".join(f"line {idx}" for idx in range(1, 5))


def test_logs_capture() -> None:
    with logger.capture_logs() as file:
        logger.logger.setLevel(logging.DEBUG)

        for line in TEXT.splitlines():
            logger.logger.debug(line)

    assert file.getvalue() == TEXT + "\n"
