from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Literal

from cyberdrop_dl.cli import is_terminal_in_portrait
from cyberdrop_dl.progress.common import RichProxy

if TYPE_CHECKING:
    from rich.console import RenderableType


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

    def __getitem__(self, name: Literal["hashing", "sorting", "scraping", "simple"]) -> Screen:
        return {
            "hashing": self.hashing,
            "sorting": self.sorting,
            "simple": self.simple,
            "scraping": self.scraping,
        }[name]
