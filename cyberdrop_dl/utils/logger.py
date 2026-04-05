from __future__ import annotations

import contextlib
import itertools
import json
import logging
import os
import queue
from contextvars import ContextVar
from datetime import datetime
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import TYPE_CHECKING, ParamSpec, cast

from rich._log_render import LogRender
from rich.console import Console, Group
from rich.logging import RichHandler
from rich.padding import Padding
from rich.text import Text, TextType
from typing_extensions import override

from cyberdrop_dl import env

if TYPE_CHECKING:
    import threading
    from collections.abc import Generator

logger = logging.getLogger("cyberdrop_dl")
_DEFAULT_CONSOLE = Console()

_USER_NAME = Path.home().name
_NEW_ISSUE_URL = "https://github.com/NTFSvolume/cdl/issues/new/choose"
_DEFAULT_CONSOLE_WIDTH = 240
_LOCK: threading.RLock = cast("threading.RLock", logging._lock)  # pyright: ignore[ reportAttributeAccessIssue]
_MAIN_LOGGER: ContextVar[LogHandler] = ContextVar("_MAIN_LOGGER")

_capture_logs: ContextVar[bool] = ContextVar("_capture_logs", default=False)


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from rich.console import ConsoleRenderable

    from cyberdrop_dl.managers.manager import Manager

    _P = ParamSpec("_P")
    _ExitCode = str | int | None


class RedactedConsole(Console):
    """Custom console to remove username from logs"""

    @override
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
    """`dicts` will be logged as json, lazily"""

    @override
    def getMessage(self) -> str:
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
        if callable(dump := getattr(msg, "model_dump_json", None)):
            return dump()
        if isinstance(msg, dict):
            return json.dumps(msg, indent=2, ensure_ascii=False, default=str)
        return msg


logging.setLogRecordFactory(JsonLogRecord)


class LogHandler(RichHandler):
    """Rich Handler with default settings, custom log render to remove padding in files and `color` extra"""

    def __init__(self, level: int = logging.DEBUG, console: Console | None = None) -> None:
        self.is_file: bool = bool(console)
        self._buffer: list[Text] = []
        super().__init__(
            level,
            console,
            show_time=self.is_file,
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            locals_max_string=_DEFAULT_CONSOLE_WIDTH,
            tracebacks_extra_lines=2,
            locals_max_length=20,
            show_path=False,
        )
        if self.is_file:
            self._log_render = NoPaddingLogRender(
                show_level=True,
                show_path=False,
                level_width=10,
                time_format=lambda dt: Text(f"[{dt.isoformat(sep=' ', timespec='milliseconds')}]", style="log.time"),
            )

    @override
    def render_message(self, record: logging.LogRecord, message: str) -> ConsoleRenderable:
        """This is the same as the base class, just added the `color` parsing from the extras"""
        use_markup = bool(getattr(record, "markup", self.markup))
        color = getattr(record, "color", "")
        message_text = Text.from_markup(message, style=color) if use_markup else Text(message, style=color)

        highlighter = getattr(record, "highlighter", self.highlighter)
        if highlighter:
            message_text = highlighter(message_text)

        if self.keywords is None:
            self.keywords = self.KEYWORDS

        if self.keywords:
            _ = message_text.highlight_words(self.keywords, "logging.keyword")

        if self.is_file and _capture_logs.get():
            self._buffer.append(message_text)

        return message_text

    def export_text(self) -> Text:
        assert self.lock is not None
        with self.lock:
            lines = self._buffer[:]
            self._buffer.clear()

        eof = Text("\n")
        text = Text()
        for line in itertools.chain.from_iterable((line, eof) for line in lines):
            _ = text.append_text(line)
        return text


class BareQueueHandler(QueueHandler):
    """Sends the log record to the queue as is.

    The base class formats the record by merging the message and arguments.
    It also removes all other attributes of the record, just in case they have not pickleable objects.

    This made tracebacks render improperly because when the rich handler picks the log record from the queue, it has no traceback.
    The original traceback was being formatted as normal text and included as part of the message.

    We never log from other processes so we do not need that safety check
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


class QueuedLogger:
    """A helper class to setup a queue handler + listener."""

    def __init__(self, manager: Manager, split_handler: LogHandler, name: str = "main") -> None:
        assert name not in manager.loggers, f"A logger with the name '{name}' already exists"
        log_queue = queue.Queue()
        self.handler = BareQueueHandler(log_queue)
        self.log_handler = split_handler
        self.listener = QueueListener(log_queue, split_handler, respect_handler_level=True)
        self.listener.start()
        manager.loggers[name] = self

    def stop(self) -> None:
        """This asks the thread to terminate, and waits until all pending messages are processed."""
        self.listener.stop()
        self.handler.close()
        self.log_handler.console.file.close()
        self.log_handler.close()


@contextlib.contextmanager
def _threaded_logger(log_handler: LogHandler) -> Generator[BareQueueHandler]:
    """Context-manager to process logs from this handler in another thread.

    It starts a QueueListener and yields the QueueHandler."""
    q: queue.Queue[logging.LogRecord] = queue.Queue()
    q_handler: BareQueueHandler = BareQueueHandler(q)
    listener: QueueListener = QueueListener(q, log_handler, respect_handler_level=True)
    listener.start()
    try:
        yield q_handler
    finally:
        try:
            q_handler.close()
        finally:
            listener.stop()
            for handler in listener.handlers[:]:
                handler.close()


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
                _ = output.append(" " * len(log_time_display), style="log.time")
                output.pad_right(1)
            else:
                _ = output.append_text(log_time_display)
                output.pad_right(1)
                self._last_time = log_time_display

        if self.show_level:
            _ = output.append(level)
            output.pad_right(1)

        if not self._cdl_padding:
            self._cdl_padding = console.measure(output).maximum

        if self.show_path and path and not any(path.startswith(p) for p in self.EXCLUDE_PATH_LOGGING_FROM):
            path_text = Text(style="log.path")
            _ = path_text.append(path, style=f"link file://{link_path}" if link_path else "")
            if line_no:
                _ = path_text.append(":")
                _ = path_text.append(
                    f"{line_no}",
                    style=f"link file://{link_path}#{line_no}" if link_path else "",
                )
            _ = output.append_text(path_text)
            output.pad_right(1)

        padded_lines: list[ConsoleRenderable] = []

        for renderable in renderables:
            if isinstance(renderable, Text):
                renderable = _indent_text(renderable, console, self._cdl_padding)
                renderable.stylize("log.message")
                _ = output.append_text(renderable)
                continue

            padded_lines.append(Padding(renderable, (0, 0, 0, self._cdl_padding), expand=False))

        return Group(output, *padded_lines)


def _indent_text(text: Text, console: Console, indent: int) -> Text:
    """Indents each line of a Text object except the first one."""
    padding = Text("\n" + (" " * indent))
    new_text = Text()
    first_line, *rest = text.wrap(console, width=console.width - indent)
    for line in rest:
        line.rstrip()
        _ = new_text.append_text(padding + line)
    first_line.rstrip()
    return first_line.append_text(new_text)


def log_spacer(char: str = "-") -> None:
    logger.info(char * (_DEFAULT_CONSOLE_WIDTH // 2), stacklevel=2)


@contextlib.contextmanager
def setup_logging(file: Path, /, level: int = logging.DEBUG) -> Generator[None]:
    logger.setLevel(level)
    file.parent.mkdir(parents=True, exist_ok=True)
    with (
        file.open("w+" if os.name == "nt" else "w", encoding="utf8") as fp,
        _threaded_logger(LogHandler(level=level)) as console_out,
        _threaded_logger(
            main_logger := LogHandler(
                level=level,
                console=RedactedConsole(file=fp, width=_DEFAULT_CONSOLE_WIDTH * 2),
            )
        ) as file_out,
        _setup_debug_logger() as debug_log_file,
    ):
        token = _MAIN_LOGGER.set(main_logger)
        logger.addHandler(console_out)
        logger.addHandler(file_out)
        try:
            logger.info(f"Debug log file: '{debug_log_file}'")
            yield
        finally:
            _MAIN_LOGGER.reset(token)


@contextlib.contextmanager
def _setup_debug_logger() -> Generator[Path | None]:
    if not env.DEBUG_VAR:
        yield
        return

    debug_log_file = Path(__file__).parent.parent.parent / "cyberdrop_dl_debug.log"

    if env.DEBUG_LOG_FOLDER:
        debug_log_folder = Path(env.DEBUG_LOG_FOLDER)

        if not debug_log_folder.is_dir():
            msg = f"Value of env var 'CDL_DEBUG_LOG_FOLDER' is invalid. Folder '{debug_log_folder}' does not exists"
            raise FileNotFoundError(None, msg, env.DEBUG_LOG_FOLDER)

        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_log_file = debug_log_folder / f"cyberdrop_dl_debug_{now}.log"

    debug_log_file = debug_log_file.expanduser().resolve().absolute()

    with (
        debug_log_file.open("w", encoding="utf8") as fp,
        _threaded_logger(
            LogHandler(level=logging.DEBUG, console=Console(file=fp, width=_DEFAULT_CONSOLE_WIDTH * 2))
        ) as debug_handler,
    ):
        logger.addHandler(debug_handler)
        yield debug_log_file
