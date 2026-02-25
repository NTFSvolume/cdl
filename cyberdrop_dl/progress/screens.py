from __future__ import annotations

import dataclasses
import shutil
from typing import TYPE_CHECKING, Literal

from cyberdrop_dl.progress.common import RichProxy

if TYPE_CHECKING:
    from rich.console import RenderableType


from cyberdrop_dl import env


def is_terminal_in_portrait() -> bool:
    """Check if CDL is being run in portrait mode based on a few conditions."""

    if env.FORCE_PORTRAIT:
        return True

    terminal_size = shutil.get_terminal_size()
    width, height = terminal_size.columns, terminal_size.lines
    aspect_ratio = width / height

    # High aspect ratios are likely to be in landscape mode
    if aspect_ratio >= 3.2:
        return False

    # Check for mobile device in portrait mode
    if (aspect_ratio < 1.5 and height >= 40) or (aspect_ratio < 2.3 and width <= 85):
        return True

    # Assume landscape mode for other cases
    return False


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
