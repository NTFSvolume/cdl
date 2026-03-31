from pathlib import Path
from typing import Self

from cyberdrop_dl import yaml
from cyberdrop_dl.exceptions import InvalidYamlError
from cyberdrop_dl.models import AliasModel, get_model_fields


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


def _is_in_file(search_value: str, file: Path) -> bool:
    try:
        return search_value.casefold() in file.read_text().casefold()
    except FileNotFoundError:
        return False
    except Exception as e:
        raise InvalidYamlError(file, e) from e
