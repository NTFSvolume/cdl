from __future__ import annotations

import contextlib
import json
import logging
import queue
import sys
from collections.abc import Generator
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec

from rich._log_render import LogRender
from rich.console import Console, Group
from rich.logging import RichHandler
from rich.padding import Padding
from rich.text import Text, TextType

from cyberdrop_dl.dependencies import browser_cookie3
from cyberdrop_dl.exceptions import InvalidYamlError

logger = logging.getLogger("cyberdrop_dl")
_USER_NAME = Path.home().resolve().name
_DEFAULT_CONSOLE_WIDTH = 240
_SHOW_LOCALS = True


if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from datetime import datetime

    from rich.console import ConsoleRenderable

    _P = ParamSpec("_P")
    _ExitCode = str | int | None


class RedactedConsole(Console):
    """Custom console to remove username from logs"""

    def _render_buffer(self, buffer) -> str:
        return self._redact_message(super()._render_buffer(buffer))

    @classmethod
    def _redact_message(cls, message: object) -> str:
        redacted = str(message)
        for sep in ("\\", "\\\\", "/"):
            as_tail = sep + _USER_NAME
            as_part = _USER_NAME + sep
            redacted = redacted.replace(as_tail, f"{sep}[REDACTED]").replace(as_part, f"[REDACTED]{sep}")
        return redacted


class JsonLogRecord(logging.LogRecord):
    def getMessage(self) -> str:  # noqa: N802
        """`dicts` will be logged as json, lazily"""

        msg = str(self._proccess_msg(self.msg))
        if self.args:
            args = tuple(map(self._proccess_msg, self.args))
            try:
                return msg.format(*args)
            except Exception:
                return msg % args

        return msg

    @staticmethod
    def _proccess_msg(msg: object) -> object:
        # TODO: Use our custom decoder to support more types
        if callable(msg):
            msg = msg()
        if isinstance(msg, dict):
            return json.dumps(msg, indent=2, ensure_ascii=False, default=str)
        return msg


logging.setLogRecordFactory(JsonLogRecord)


class LogHandler(RichHandler):
    """Rich Handler with default settings, custom log render to remove padding in files and `color` extra"""

    def __init__(self, level: int = logging.DEBUG, console: Console | None = None) -> None:
        is_file = bool(console)
        super().__init__(
            level,
            console,
            show_time=is_file,
            rich_tracebacks=True,
            tracebacks_show_locals=_SHOW_LOCALS,
            locals_max_string=_DEFAULT_CONSOLE_WIDTH,
            tracebacks_extra_lines=2,
            locals_max_length=20,
        )
        if is_file:
            self._log_render = NoPaddingLogRender(show_level=True)

    def render_message(self, record: logging.LogRecord, message: str) -> ConsoleRenderable:
        """This is the same as the base class, just added the `color` parsing from the extras"""
        use_markup = getattr(record, "markup", self.markup)
        color = getattr(record, "color", "")
        message_text = Text.from_markup(message, style=color) if use_markup else Text(message, style=color)

        highlighter = getattr(record, "highlighter", self.highlighter)
        if highlighter:
            message_text = highlighter(message_text)

        if self.keywords is None:
            self.keywords = self.KEYWORDS

        if self.keywords:
            _ = message_text.highlight_words(self.keywords, "logging.keyword")

        return message_text


class BareQueueHandler(QueueHandler):
    """Sends the log record to the queue as is.

    The base class formats the record by merging the message and arguments.
    It also removes all other attributes of the record, just in case they have not pickleable objects.

    This made tracebacks render improperly because when the rich handler picks the log record from the queue, it has no traceback.
    The original traceback was being formatted as normal text and included as part of the message.

    Having not pickleable objects is only an issue in multi-processing operations (multiprocessing.Queue)
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


@contextlib.contextmanager
def enqueue_logger(log_handler: LogHandler) -> Generator[BareQueueHandler]:
    """Context-manager to process logs from this handler in another thread.

    It starts a QueueListener and yields the QueueHandler."""
    q: queue.Queue[logging.LogRecord] = queue.Queue()
    queue_handler: BareQueueHandler = BareQueueHandler(q)
    listener: QueueListener = QueueListener(q, log_handler, respect_handler_level=True)
    listener.start()
    try:
        yield queue_handler
    finally:
        try:
            queue_handler.close()
        finally:
            listener.stop()
            for handl in listener.handlers:
                handl.close()


@contextlib.contextmanager
def setup_logging(
    logs_file: Path,
    log_level: int = logging.DEBUG,
    console_log_level: int = logging.CRITICAL + 10,
) -> Generator[None]:
    logger.setLevel(log_level)
    with (
        logs_file.open("w", encoding="utf8") as file_io,
        enqueue_logger(LogHandler(level=console_log_level)) as console_handler,
        enqueue_logger(
            LogHandler(
                level=log_level,
                console=RedactedConsole(file=file_io, width=_DEFAULT_CONSOLE_WIDTH * 2),
            )
        ) as file_handler,
    ):
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        yield


@contextlib.contextmanager
def _try_open(path: Path):
    try:
        file_io = path.open("w", encoding="utf8")
    except OSError:
        yield
    else:
        with file_io:
            yield file_io


@contextlib.contextmanager
def startup_context() -> Generator[None]:
    """Temporarily log everything to the console; on exception, dump it to a file."""
    logger = logging.getLogger("cyberdrop_dl_startup")
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    handlers: list[LogHandler] = []
    if "pytest" not in sys.modules:
        console_handler = LogHandler(level=logging.DEBUG)
        logger.addHandler(console_handler)
        handlers.append(console_handler)

    path = Path.cwd() / "startup.log"
    delete = False
    with _try_open(path) as file_io:
        if file_io is not None:
            file_handler = LogHandler(
                level=logging.DEBUG,
                console=Console(file=file_io, width=_DEFAULT_CONSOLE_WIDTH),
            )
            logger.addHandler(file_handler)
            handlers.append(file_handler)
        try:
            yield

        except InvalidYamlError as e:
            logger.error(e.message)

        except browser_cookie3.BrowserCookieError:
            logger.exception("")

        except OSError as e:
            logger.exception(str(e))

        except KeyboardInterrupt:
            logger.info("Exiting...")

        except Exception:
            msg = "An error occurred, please report this to the developer with your logs file:"
            logger.exception(msg)
        else:
            delete = True

        finally:
            logger.setLevel(old_level)
            for handler in handlers:
                logger.removeHandler(handler)

    if delete and file_io is not None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


class NoPaddingLogRender(LogRender):
    _cdl_padding: int = 0
    EXCLUDE_PATH_LOGGING_FROM: tuple[str, ...] = "logger.py", "base.py", "session.py", "cache_control.py"

    def __call__(  # type: ignore[reportIncompatibleMethodOverride]  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        console: Console,
        renderables: Iterable[ConsoleRenderable],
        log_time: datetime | None = None,
        time_format: str | Callable[[datetime], Text] | None = None,
        level: TextType = "",
        path: str | None = None,
        line_no: int | None = None,
        link_path: str | None = None,
    ):
        output = Text(no_wrap=True)
        if self.show_time:
            log_time = log_time or console.get_datetime()
            time_format = time_format or self.time_format
            log_time_display = (
                time_format(log_time)
                if callable(time_format)
                else Text(log_time.strftime(time_format), style="log.time")
            )
            if log_time_display == self._last_time and self.omit_repeated_times:
                output.append(" " * len(log_time_display), style="log.time")
                output.pad_right(1)
            else:
                output.append(log_time_display)
                output.pad_right(1)
                self._last_time = log_time_display

        if self.show_level:
            output.append(level)
            output.pad_right(1)

        if not self._cdl_padding:
            self._cdl_padding = console.measure(output).maximum

        if self.show_path and path and not any(path.startswith(p) for p in self.EXCLUDE_PATH_LOGGING_FROM):
            path_text = Text(style="log.path")
            path_text.append(path, style=f"link file://{link_path}" if link_path else "")
            if line_no:
                path_text.append(":")
                path_text.append(
                    f"{line_no}",
                    style=f"link file://{link_path}#{line_no}" if link_path else "",
                )
            output.append(path_text)
            output.pad_right(1)

        padded_lines: list[ConsoleRenderable] = []

        for renderable in renderables:
            if isinstance(renderable, Text):
                renderable = _indent_text(renderable, console, self._cdl_padding)
                renderable.stylize("log.message")
                _ = output.append(renderable)
                continue

            padded_lines.append(Padding(renderable, (0, 0, 0, self._cdl_padding), expand=False))

        return Group(output, *padded_lines)


def _indent_text(text: Text, console: Console, indent: int = 30) -> Text:
    """Indents each line of a Text object except the first one."""
    padding = Text("\n" + (" " * indent))
    new_text = Text()
    first_line, *rest = text.wrap(console, width=console.width - indent)
    for line in rest:
        line.rstrip()
        new_text.append(padding + line)
    first_line.rstrip()
    return first_line.append(new_text)


def spacer(char: str = "-") -> str:
    return char * (_DEFAULT_CONSOLE_WIDTH // 2)


def catch_exceptions(func: Callable[_P, _ExitCode]) -> Callable[_P, _ExitCode]:
    """Decorator to automatically log uncaught exceptions.

    Exceptions will be logged to a file in the current working directory
    because the manager setup itself may have failed, therefore we don't know
    what the proper log file path is.
    """
    import functools

    @functools.wraps(func)
    def catch(*args: _P.args, **kwargs: _P.kwargs) -> _ExitCode | None:
        with startup_context():
            return func(*args, **kwargs)

    return catch


@contextlib.contextmanager
def enable_3p_logger(name: str, level: int | None = None) -> Generator[None]:
    logger = logging.getLogger(name)
    old_level = logger.level
    old_handlers = logger.handlers.copy()
    old_propagate = logger.propagate

    logger.handlers.clear()
    for handler in logging.getLogger("cyberdrop_dl").handlers:
        logger.addHandler(handler)

    logger.propagate = False
    if level is not None:
        logger.setLevel(level)

    try:
        yield
    finally:
        logger.handlers.clear()
        logger.handlers.extend(old_handlers)
        logger.propagate = old_propagate
        logger.setLevel(old_level)
