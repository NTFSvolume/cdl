from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from ._base import AliasModel, AppriseURLModel, Settings, SettingsGroup

if TYPE_CHECKING:
    from pathlib import Path

    import pydantic_core
    from pydantic import BaseModel, ValidationError

    _M = TypeVar("_M", bound=BaseModel)


def get_model_fields(model: BaseModel, *, exclude_unset: bool = True) -> set[str]:
    fields = set()
    for submodel_name, submodel in model.model_dump(exclude_unset=exclude_unset).items():
        for field_name in submodel:
            fields.add(f"{submodel_name}.{field_name}")

    return fields


def merge_dicts(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    for key, val in old.items():
        if isinstance(val, dict):
            if key in new and isinstance(new[key], dict):
                merge_dicts(old[key], new[key])
        else:
            if key in new:
                old[key] = new[key]

    for key, val in new.items():
        if key not in old:
            old[key] = val

    return old


def merge_models(old: _M, new: _M) -> _M:
    old.model_copy()
    old_values = old.model_dump()
    new_values = new.model_dump(exclude_unset=True)
    updated_dict = merge_dicts(old_values, new_values)
    return old.model_validate(updated_dict)


def format_validation_error(e: ValidationError, /, file: Path | None = None, title: str | None = None) -> str:
    error_count = e.error_count()

    def get_name(error: pydantic_core.ErrorDetails) -> str:
        """Get a human readable representation of the field that raised this error"""

        if file:
            return ".".join(map(str, error["loc"]))

        option_name: str | int = error["loc"][-1]
        if isinstance(option_name, int):
            option_name = ".".join(map(str, error["loc"][-2:]))
        option_name = option_name.replace("_", "-")
        return f"--{option_name}"

    def parse():
        if file:
            yield f"File '{file}' has an invalid config\n"
        yield f"Found {error_count} error{'s' if error_count > 1 else ''} [{title or e.title}]:"

        for error in e.errors(include_url=False):
            option_name = get_name(error)
            yield f"\nOption '{option_name}' with value '{error['input']}' is invalid:"
            yield f"  {error['msg']}"

        if file:
            yield "\nPlease delete the file or fix the errors"

    return "\n".join(parse())


__all__ = [
    "AliasModel",
    "AppriseURLModel",
    "AppriseURLModel",
    "Settings",
    "SettingsGroup",
    "format_validation_error",
    "get_model_fields",
]
