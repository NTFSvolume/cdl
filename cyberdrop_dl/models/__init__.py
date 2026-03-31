"""Pydantic models"""

from collections.abc import Iterator, Sequence
from typing import ClassVar, TypedDict, TypeVar

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


def get_model_fields(model: BaseModel, *, exclude_unset: bool = True) -> set[str]:
    fields = set()
    for submodel_name, submodel in model.model_dump(exclude_unset=exclude_unset).items():
        for field_name in submodel:
            fields.add(f"{submodel_name}.{field_name}")

    return fields


_ModelT = TypeVar("_ModelT", bound=BaseModel)


class AliasModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class SequenceModel(RootModel[list[_ModelT]], Sequence[_ModelT]):
    def __len__(self) -> int:
        return len(self.root)

    def __iter__(self) -> Iterator[_ModelT]:
        yield from self.root

    def __getitem__(self, index: int) -> _ModelT:
        return self.root[index]

    def __bool__(self) -> bool:
        return bool(len(self))


class _AppriseURLDict(TypedDict):
    url: str
    tags: set[str]


class AppriseURL(AliasModel):
    url: Secret[AnyUrl]
    tags: set[str] = set()

    _OS_URLS: ClassVar[tuple[str, ...]] = "windows://", "macosx://", "dbus://", "qt://", "glib://", "kde://"
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
    @classmethod
    def parse(cls, value: object) -> _AppriseURLDict:
        match value:
            case str():
                return cls._parse_url(value)

            case dict():
                tags = value.get("tags") or set()
                url = str(value.get("url", ""))
                if not tags:
                    return cls._parse_url(url)

                return {"url": url, "tags": tags}

            case _:
                return {"url": str(value), "tags": set()}

    @staticmethod
    def _parse_url(value: str) -> _AppriseURLDict:
        match value.split("://", 1)[0].split("=", 1):
            case [tags_, _scheme]:
                tags = set(tags_.split(","))
                url = value.split("=", 1)[-1]
            case _:
                tags: set[str] = set()
                url: str = value

        return {"url": url, "tags": tags}
