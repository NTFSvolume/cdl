from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import itertools
import random
from typing import TYPE_CHECKING, ClassVar, final

from rich.columns import Columns
from rich.markup import escape
from rich.progress import SpinnerColumn
from rich.spinner import Spinner
from rich.text import Text

from cyberdrop_dl import __version__
from cyberdrop_dl.progress import create_test_live
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
        self._cols.renderables.extend([self.description, Spinner("point", style="green"), "|"])

    def __rich__(self) -> Columns:
        return self._cols

    @contextlib.contextmanager
    def __call__(self, msg: object) -> Generator[None]:
        msg_id = _generate_unique_id()
        try:
            self._messages[msg_id] = new_msg = Spinner("dots3", style="green"), Text(escape(str(msg)))
            self._cols.renderables.extend(new_msg)
            yield
        finally:
            _ = self._messages.pop(msg_id)
            self._cols.renderables[3:] = itertools.chain.from_iterable(self._messages.values())

    async def simulate(self) -> None:
        await asyncio.sleep(2)

        async def show(msg: str) -> None:
            with self(msg):
                await asyncio.sleep(random.random() * 5)

        async with asyncio.TaskGroup() as tg:
            for idx in range(1, 10):
                tg.create_task(show(f"test msg {idx}"))


@final
class ScrapingPanel(OverflowPanel):
    unit: ClassVar[str] = "URL"

    def __init__(self) -> None:
        super().__init__(
            SpinnerColumn("dots3"),
            "[progress.description]{task.description}",
            max_rows=3,
            expand=False,
        )

    @contextlib.contextmanager
    def new(self, url: object) -> Generator[None]:
        task = self._add_task(str(url))
        try:
            yield
        finally:
            self._remove_task(task)

    async def simulate(self) -> None:
        a = self._add_task("url_a")
        b = self._add_task("url_b")
        c = self._add_task("url_c")
        await asyncio.sleep(5)
        d = self._add_task("url_d")
        _ = self._add_task("url_e")
        await asyncio.sleep(5)
        self._remove_task(a)
        self._remove_task(b)
        self._remove_task(c)
        self._remove_task(d)
        await asyncio.sleep(2)
        with self.new("http://github.com"):
            await asyncio.sleep(2)
            with self.new("http://github2.com"):
                await asyncio.sleep(2)
            with self.new("http://github3.com"):
                await asyncio.sleep(2)


if __name__ == "__main__":
    panel = ScrapingPanel()
    status = StatusMessage()
    with create_test_live(status):
        asyncio.run(status.simulate())

    with create_test_live(panel):
        asyncio.run(panel.simulate())
