from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel

from ._base import AliasModel, AppriseURLModel, Settings, SettingsGroup

M = TypeVar("M", bound=BaseModel)


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


def merge_models(old: M, new: M) -> M:
    old.model_copy()
    old_values = old.model_dump()
    new_values = new.model_dump(exclude_unset=True)
    updated_dict = merge_dicts(old_values, new_values)
    return old.model_validate(updated_dict)


__all__ = [
    "AliasModel",
    "AppriseURLModel",
    "AppriseURLModel",
    "Settings",
    "SettingsGroup",
    "get_model_fields",
]
