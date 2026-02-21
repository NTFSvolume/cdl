from pydantic import BaseModel

from .base_models import AliasModel, AppriseURLModel, FrozenModel, HttpAppriseURL


def get_model_fields(model: BaseModel, *, exclude_unset: bool = True) -> set[str]:
    fields = set()
    for submodel_name, submodel in model.model_dump(exclude_unset=exclude_unset).items():
        for field_name in submodel:
            fields.add(f"{submodel_name}.{field_name}")

    return fields


__all__ = ["AliasModel", "AppriseURLModel", "FrozenModel", "HttpAppriseURL", "get_model_fields"]
