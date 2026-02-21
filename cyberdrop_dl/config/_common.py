from pathlib import Path
from typing import Self

from pydantic import BaseModel

from cyberdrop_dl.exceptions import InvalidYamlError
from cyberdrop_dl.models import AliasModel, get_model_fields
from cyberdrop_dl.utils import yaml


class ConfigModel(AliasModel):
    @classmethod
    def load_file(cls, file: Path, update_if_has_string: str) -> Self:
        default = cls()
        if not file.is_file():
            config = default
            needs_update = True

        else:
            all_fields = get_model_fields(default, exclude_unset=False)
            config = cls.model_validate(yaml.load(file))
            set_fields = get_model_fields(config)
            needs_update = all_fields != set_fields or _is_in_file(update_if_has_string, file)

        if needs_update:
            config.save_to_file(file)

        return config

    def save_to_file(self, file: Path) -> None:
        yaml.save(file, self)

    def resolve_paths(self) -> None:
        self._resolve_paths(self)

    @classmethod
    def _resolve_paths(cls, model: BaseModel) -> None:
        for name, value in vars(model).items():
            if isinstance(value, Path):
                setattr(model, name, value.resolve())

            elif isinstance(value, BaseModel):
                cls._resolve_paths(value)


def _is_in_file(search_value: str, file: Path) -> bool:
    try:
        return search_value.casefold() in file.read_text().casefold()
    except FileNotFoundError:
        return False
    except Exception as e:
        raise InvalidYamlError(file, e) from e
