from pathlib import Path
from typing import Any, ClassVar, Final, TypeVar

import pydantic_core
from cyclopts import Parameter
from pydantic import BaseModel, Secret, SerializationInfo, ValidationError, model_serializer, model_validator

from .types import HttpURL

_ModelT = TypeVar("_ModelT", bound=BaseModel)


class AliasModel(BaseModel, populate_by_name=True, defer_build=True): ...


@Parameter(name="*")
class Settings(AliasModel): ...


class SettingsGroup(Settings):
    def __init_subclass__(cls, name: str | None = None, **kwargs: Any) -> None:
        _ = Parameter(group=name or cls.__name__)(cls)
        return super().__init_subclass__(**kwargs)


class AppriseURL(AliasModel):
    url: Secret[HttpURL]
    tags: set[str] = set()

    _OS_URLS: ClassVar[Final] = "windows://", "macosx://", "dbus://", "qt://", "glib://", "kde://"
    _VALID_TAGS: ClassVar[set[str]] = {"no_logs", "attach_logs", "simplified"}

    def model_post_init(self, *_) -> None:
        if not self.tags.intersection(self._VALID_TAGS):
            self.tags = self.tags | {"no_logs"}

        if self.is_os_url:
            self.tags = (self.tags - self._VALID_TAGS) | {"simplified"}

    def __str__(self) -> str:
        return self._format(dump_secret=True)

    @property
    def is_os_url(self) -> bool:
        return any(scheme in str(self).casefold() for scheme in self._OS_URLS)

    @property
    def attach_logs(self) -> bool:
        return "attach_logs" in self.tags

    @model_serializer()
    def serialize(self, info: SerializationInfo) -> str:
        return self._format(dump_secret=info.mode != "json")

    def _format(self, dump_secret: bool) -> str:
        url = str(self.url.get_secret_value() if dump_secret else self.url)
        if not self.tags:
            return url
        return f"{','.join(sorted(self.tags))}={url}"

    @model_validator(mode="before")
    @staticmethod
    def parse(value: str) -> dict[str, Any]:
        match value.split("://", 1)[0].split("=", 1):
            case [tags_, _scheme]:
                tags = set(tags_.split(","))
                url = value.split("=", 1)[-1]
            case _:
                tags: set[str] = set()
                url: str = value

        return {"url": url, "tags": tags}


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


def merge_models(old: _ModelT, new: _ModelT) -> _ModelT:
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
