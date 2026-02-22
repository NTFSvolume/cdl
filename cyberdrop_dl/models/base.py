"""Pydantic models"""

from collections.abc import Iterator, Mapping, Sequence
from typing import TypeVar, Unpack

import yarl
from cyclopts import Parameter
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    RootModel,
    Secret,
    SerializationInfo,
    model_serializer,
    model_validator,
)

from cyberdrop_dl.models.types import HttpURL
from cyberdrop_dl.models.validators import to_apprise_url_dict

_ModelT = TypeVar("_ModelT", bound=BaseModel)


class AliasModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, defer_build=True)


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, defer_build=True)


@Parameter(name="*")
class FlatNamespace: ...


class Settings(FlatNamespace, AliasModel): ...


class SettingsGroup(Settings):
    def __init_subclass__(cls, name: str | None = None, **kwargs: Unpack[ConfigDict]) -> None:
        _ = Parameter(group=name or cls.__name__)(cls)
        return super().__init_subclass__(**kwargs)


class AppriseURLModel(FrozenModel):
    url: Secret[AnyUrl]
    tags: set[str] = set()

    @model_serializer()
    def serialize(self, info: SerializationInfo) -> str:
        dump_secret = info.mode != "json"
        url = self.url.get_secret_value() if dump_secret else self.url
        tags = self.tags - set("no_logs")
        tags = sorted(tags)
        return f"{','.join(tags)}{'=' if tags else ''}{url}"

    @model_validator(mode="before")
    @staticmethod
    def parse_input(value: yarl.URL | dict | str) -> Mapping:
        return to_apprise_url_dict(value)


class HttpAppriseURL(AppriseURLModel):
    url: Secret[HttpURL]


class SequenceModel(RootModel[list[_ModelT]], Sequence[_ModelT]):
    def __len__(self) -> int:
        return len(self.root)

    def __iter__(self) -> Iterator[_ModelT]:
        yield from self.root

    def __getitem__(self, index: int) -> _ModelT:
        return self.root[index]

    def __bool__(self) -> bool:
        return bool(len(self))
