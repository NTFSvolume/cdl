from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Literal, Self

from rich.console import Group, RenderableType
from rich.layout import Layout

from cyberdrop_dl.cli import is_terminal_in_portrait

if TYPE_CHECKING:
    from cyberdrop_dl.progress import ProgressManager


@dataclasses.dataclass(slots=True, frozen=True)
class Screen:
    _renderable: RenderableType

    def __rich__(self) -> RenderableType:
        return self._renderable


@dataclasses.dataclass(slots=True, frozen=True)
class RotatingScreen(Screen):
    _renderable_vertical: RenderableType

    def __rich__(self) -> RenderableType:
        return self._renderable_vertical if is_terminal_in_portrait() else self._renderable


class ScrapingScreen(RotatingScreen):
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
        return cls(horizontal, vertical)


@dataclasses.dataclass(slots=True, frozen=True)
class AppScreens:
    scraping: ScrapingScreen
    simple: Screen
    hashing: Screen
    sorting: Screen

    @classmethod
    def build(cls, progress: ProgressManager) -> Self:
        return cls(
            ScrapingScreen.build(progress),
            Screen(Group(progress.status.activity, progress.files.simple)),
            Screen(progress.hashing),
            Screen(progress.sorting),
        )

    def __getitem__(self, name: Literal["hashing", "sorting", "scrape"]) -> Screen:
        if name == "hashing":
            return self.hashing
        if name == "sorting":
            return self.sorting
        if name == "simple":
            return self.simple
        if name == "scrape":
            return self.scraping
        raise KeyError
