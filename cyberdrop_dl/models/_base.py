from typing import Any, TypeVar

from cyclopts import Parameter
from pydantic import BaseModel, Secret, SerializationInfo, model_serializer, model_validator

from cyberdrop_dl.models.types import HttpURL

_ModelT = TypeVar("_ModelT", bound=BaseModel)


class AliasModel(BaseModel, populate_by_name=True, defer_build=True): ...


class FrozenModel(BaseModel, frozen=True, defer_build=True): ...


@Parameter(name="*")
class Settings(AliasModel): ...


class SettingsGroup(Settings):
    def __init_subclass__(cls, name: str | None = None, **kwargs: Any) -> None:
        _ = Parameter(group=name or cls.__name__)(cls)
        return super().__init_subclass__(**kwargs)


class AppriseURLModel(AliasModel):
    url: Secret[HttpURL]
    tags: set[str] = set()

    @model_serializer()
    def serialize(self, info: SerializationInfo) -> str:
        return self._format(dump_secret=info.mode != "json")

    def _format(self, dump_secret: bool) -> str:
        url = self.url.get_secret_value() if dump_secret else self.url
        tags = self.tags - set("no_logs")
        tags = sorted(tags)
        return f"{','.join(tags)}{'=' if tags else ''}{url}"

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
