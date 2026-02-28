from typing import Any, ClassVar, Final, TypeVar

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
