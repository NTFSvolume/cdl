from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Literal, Self

from rich.console import Group, RenderableType
from rich.layout import Layout

from cyberdrop_dl.cli import is_terminal_in_portrait
from cyberdrop_dl.progress._common import RichProxy

if TYPE_CHECKING:
    from cyberdrop_dl.progress import ProgressManager


@dataclasses.dataclass(slots=True)
class Screen(RichProxy):
    _renderable_vertical: RenderableType = ""

    def __rich__(self) -> RenderableType:
        return (
            self._renderable_vertical if (self._renderable_vertical and is_terminal_in_portrait()) else self._renderable
        )


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class AppScreens:
    scraping: Screen
    simple: Screen
    hashing: Screen
    sorting: Screen

    @classmethod
    def build(cls, progress: ProgressManager) -> Self:
        horizontal = Layout()
        vertical = Layout()
        top = (
            Layout(progress.files, ratio=1, minimum_size=9),
            Layout(progress.scrape_errors, ratio=1),
            Layout(progress.download_errors, ratio=1),
        )

        bottom = (
            Layout(progress.scrape, ratio=20),
            Layout(progress.downloads, ratio=20),
            Layout(progress.status, ratio=2),
        )

        horizontal.split_column(Layout(name="top", ratio=20), *bottom)
        vertical.split_column(Layout(name="top", ratio=60), *bottom)

        horizontal["top"].split_row(*top)
        vertical["top"].split_column(*top)

        return cls(
            scraping=Screen(horizontal, vertical),
            simple=Screen(Group(progress.status.activity, progress.files.simple)),
            hashing=Screen(progress.hashing),
            sorting=Screen(progress.sorting),
        )

    def __getitem__(self, name: Literal["hashing", "sorting", "scraping", "simple"]) -> Screen:
        return {
            "hashing": self.hashing,
            "sorting": self.sorting,
            "simple": self.simple,
            "scraping": self.scraping,
        }[name]
