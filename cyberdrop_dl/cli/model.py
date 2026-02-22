import datetime
from enum import StrEnum, auto
from pathlib import Path
from typing import Literal

from cyclopts import Parameter
from pydantic import Field

from cyberdrop_dl.config import Config
from cyberdrop_dl.models import Settings


class UIOptions(StrEnum):
    DISABLED = auto()
    ACTIVITY = auto()
    SIMPLE = auto()
    FULLSCREEN = auto()


@Parameter(name="*", negative_bool=[])
class CLIargs(Settings):
    appdata_folder: Path | None = Field(
        default=None,
        description="AppData folder path",
    )

    config_file: Path | None = Field(
        default=None,
        description="path to the CDL settings.yaml file to load",
    )

    impersonate: (
        Literal[
            "chrome",
            "edge",
            "safari",
            "safari_ios",
            "chrome_android",
            "firefox",
        ]
        | None
    ) = Field(
        default=None,
        description="Use this target as impersonation for all scrape requests",
    )

    portrait: bool = Field(
        default=False,
        description="force CDL to run with a vertical layout",
    )
    print_stats: bool = Field(
        default=True,
        description="show stats report at the end of a run",
    )


@Parameter(name="*", negative_bool="")
class RetryArgs(Settings):
    completed_after: datetime.date | None = Field(
        default=None,
        description="only retry downloads that were completed on or after this date",
    )
    completed_before: datetime.date | None = Field(
        default=None,
        description="only retry downloads that were completed on or before this date",
    )

    max_items_retry: int = Field(
        default=0,
        description="max number of links to retry",
    )


class ParsedArgs(Settings):
    cli_only_args: CLIargs = CLIargs()
    config: Config = Config()
