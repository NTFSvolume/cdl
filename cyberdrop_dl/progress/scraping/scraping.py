from __future__ import annotations

import contextlib
import dataclasses
import itertools
import time
from typing import TYPE_CHECKING, ClassVar, final

from rich.columns import Columns
from rich.markup import escape
from rich.progress import SpinnerColumn
from rich.spinner import Spinner
from rich.text import Text

from cyberdrop_dl import __version__
from cyberdrop_dl.progress import create_live
from cyberdrop_dl.progress.overflow import OverflowPanel

if TYPE_CHECKING:
    from collections.abc import Generator


_generate_unique_id = itertools.count(1).__next__


@final
@dataclasses.dataclass(slots=True, frozen=True)
class StatusMessage:
    description: Text | str = f"Running cyberdrop-dl [blue]v{__version__}[/blue]"
    _messages: dict[int, tuple[Spinner, Text]] = dataclasses.field(init=False, default_factory=dict)
    _cols: Columns = dataclasses.field(init=False, default_factory=Columns)

    def __post_init__(self) -> None:
        self._cols.renderables.extend([Spinner("dots", style="green"), self.description])

    def __rich__(self) -> Columns:
        return self._cols

    @contextlib.contextmanager
    def __call__(self, msg: object) -> Generator[None]:
        msg_id = _generate_unique_id()
        try:
            self._messages[msg_id] = new_msg = Spinner("dots", style="green"), Text(escape(str(msg)))
            self._cols.renderables.extend(new_msg)
            yield
        finally:
            _ = self._messages.pop(msg_id)
            self._cols.renderables[2:] = itertools.chain.from_iterable(self._messages.values())


@final
class ScrapingPanel(OverflowPanel):
    unit: ClassVar[str] = "URLs"

    def __init__(self) -> None:
        super().__init__(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            visible_tasks_limit=3,
            expand=False,
        )

    @contextlib.contextmanager
    def new(self, url: object) -> Generator[None]:
        task = self._add_task(str(url))
        try:
            yield
        finally:
            self._remove_task(task)


if __name__ == "__main__":
    panel = ScrapingPanel()
    status = StatusMessage()
    with create_live(status):
        time.sleep(2)
        with status("test 1"):
            time.sleep(2)
            with status("test 2"):
                time.sleep(2)
                with status("test 3"):
                    time.sleep(2)
                time.sleep(2)
                with status("test 4"):
                    time.sleep(2)
                time.sleep(2)
        time.sleep(2)

        with status("test 5"):
            time.sleep(2)

    with create_live(panel):
        a = panel._add_task("url_a")
        b = panel._add_task("url_b")
        c = panel._add_task("url_c")
        time.sleep(5)
        d = panel._add_task("url_d")
        _ = panel._add_task("url_e")
        time.sleep(5)
        panel._remove_task(a)
        panel._remove_task(b)
        panel._remove_task(c)
        panel._remove_task(d)
        time.sleep(2)
        with panel.new("http://github.com"):
            time.sleep(2)
            with panel.new("http://github2.com"):
                time.sleep(2)
            with panel.new("http://github3.com"):
                time.sleep(2)
