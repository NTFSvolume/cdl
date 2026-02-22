from __future__ import annotations

from datetime import date, timedelta
from enum import Enum
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import BaseModel, ValidationError
from yarl import URL

from cyberdrop_dl.exceptions import InvalidYamlError

if TYPE_CHECKING:
    from pydantic_core import ErrorDetails


class TimedeltaSerializer(BaseModel):
    duration: timedelta


def _save_as_str(dumper: yaml.Dumper, value: object):
    if isinstance(value, Enum):
        return dumper.represent_str(value.name)
    return dumper.represent_str(str(value))


def _save_date(dumper: yaml.Dumper, value: date):
    return dumper.represent_str(value.isoformat())


def _save_timedelta(dumper: yaml.Dumper, value: timedelta):
    timespan = TimedeltaSerializer(duration=value).model_dump(mode="json")
    return dumper.represent_str(timespan["duration"])


yaml.add_multi_representer(PurePath, _save_as_str)
yaml.add_multi_representer(Enum, _save_as_str)
yaml.add_multi_representer(date, _save_date)
yaml.add_representer(timedelta, _save_timedelta)
yaml.add_representer(URL, _save_as_str)


def save(file: Path, data: BaseModel | dict[str, Any]) -> None:
    """Saves a dict to a yaml file."""
    if isinstance(data, BaseModel):
        data = data.model_dump()
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(
        yaml.dump(data, default_flow_style=False),
        encoding="utf8",
    )


def load(file: Path, *, create: bool = False) -> dict[str, Any]:
    """Loads a yaml file and returns it as a dict."""
    if create:
        file.parent.mkdir(parents=True, exist_ok=True)
        if not file.is_file():
            file.touch()
    try:
        yaml_values = yaml.safe_load(file.read_text(encoding="utf8"))
        return yaml_values or {}
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise InvalidYamlError(file, e) from None


def format_validation_error(e: ValidationError, *, title: str = "", file: Path | None = None):
    """Logs the validation error details and exits the program."""

    error_count = e.error_count()
    msg = ""
    if file:
        msg += f"File '{file}' has an invalid config\n\n"

    show_title = title or e.title
    msg += f"Found {error_count} error{'s' if error_count > 1 else ''} [{show_title}]:"
    from_cli = title == "CLI arguments"

    for error in e.errors(include_url=False):
        option_name = get_field_name(error, from_cli)
        msg += f"\n\nOption '{option_name}' with value '{error['input']}' is invalid:\n"
        msg += f"  {error['msg']}"

    if not from_cli:
        msg += "\n\n" + """Please delete the file or fix the errors"""

    return msg


def get_field_name(error: ErrorDetails, from_cli: bool = False) -> str:
    """Get a human readable representation of the field that raised this error"""

    if not from_cli:
        return ".".join(map(str, error["loc"]))

    option_name: str | int = error["loc"][-1]
    if isinstance(option_name, int):
        option_name = ".".join(map(str, error["loc"][-2:]))
    option_name = option_name.replace("_", "-")
    return f"--{option_name}"
