from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel

from .base import AliasModel, AppriseURLModel, FlatNamespace, FrozenModel, HttpAppriseURL, Settings, SettingsGroup

M = TypeVar("M", bound=BaseModel)


def get_model_fields(model: BaseModel, *, exclude_unset: bool = True) -> set[str]:
    fields = set()
    for submodel_name, submodel in model.model_dump(exclude_unset=exclude_unset).items():
        for field_name in submodel:
            fields.add(f"{submodel_name}.{field_name}")

    return fields


def merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    for key, val in dict1.items():
        if isinstance(val, dict):
            if key in dict2 and isinstance(dict2[key], dict):
                merge_dicts(dict1[key], dict2[key])
        else:
            if key in dict2:
                dict1[key] = dict2[key]

    for key, val in dict2.items():
        if key not in dict1:
            dict1[key] = val

    return dict1


def merge_models(old: M, new: M) -> M:
    old.model_copy()
    old_values = old.model_dump()
    new_values = new.model_dump(exclude_unset=True)
    updated_dict = merge_dicts(old_values, new_values)
    return old.model_validate(updated_dict)


__all__ = [
    "AliasModel",
    "AppriseURLModel",
    "FlatNamespace",
    "FrozenModel",
    "HttpAppriseURL",
    "Settings",
    "SettingsGroup",
    "get_model_fields",
]
