import dataclasses
from argparse import BooleanOptionalAction
from collections.abc import Generator, Iterable
from typing import Any, Literal, TypedDict

from pydantic import BaseModel

_NOT_SET: Any = object()


class _ArgumentParams(TypedDict, total=False):
    action: str
    nargs: int | str | None
    const: Any
    default: Any
    choices: Iterable[Any] | None
    required: bool
    help: str | None
    metavar: str | tuple[str, ...] | None
    dest: str | None


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class ArgumentParams:
    positional_only: bool = dataclasses.field(default=False, metadata={"exclude": True})
    nargs: Literal["?", "*", "+"] | str | None = _NOT_SET
    const: Any = _NOT_SET
    dest: str = _NOT_SET
    choices: Iterable[Any] | None = _NOT_SET
    metavar: str | tuple[str, ...] | None = _NOT_SET

    def as_dict(self) -> _ArgumentParams:
        return {name: v for name in _params if (v := getattr(self, name)) is not _NOT_SET}  # pyright: ignore[reportReturnType]


_params = tuple(f.name for f in dataclasses.fields(ArgumentParams) if not f.metadata.get("exclude"))


@dataclasses.dataclass(slots=True, kw_only=True)
class Argument:
    name_or_flags: list[str] = dataclasses.field(init=False)
    cli_name: str
    aliases: tuple[str, ...]
    required: bool
    default: Any
    annotation: Any
    help: str | None
    metadata: list[Any]
    arg_type: type = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.arg_type = type(self.default)

        if self.arg_type not in (list, set, bool):
            self.arg_type = str

        self.name_or_flags = [f"{'' if self.required else '--'}{self.cli_name}"]

        for alias in self.aliases:
            if alias and len(alias) == 1:
                self.name_or_flags.insert(0, f"-{alias}")
            else:
                self.name_or_flags.append(alias)

    def compose_options(self) -> _ArgumentParams:
        options = self._options()
        if override := self._overrides():
            return options | override.as_dict()

        return options

    def _overrides(self) -> ArgumentParams | None:
        for meta in self.metadata:
            if isinstance(meta, ArgumentParams):
                return meta

    def _options(self) -> _ArgumentParams:
        default = dict(  # noqa: C408
            default=self.default,
            help=self.help,
            action="store",
        )
        if not self.required:
            default["dest"] = self.cli_name

        if self.arg_type is bool:
            default["action"] = BooleanOptionalAction

        elif self.arg_type in (list, set):
            default.update(nargs="*", action="extend")

        else:
            default["type"] = self.arg_type

        return default  # pyright: ignore[reportReturnType]


def parse(model: type[BaseModel]) -> Generator[Argument]:
    for python_name, field in model.model_fields.items():
        aliases = filter(
            None,
            (
                field.alias,
                field.validation_alias,
                field.serialization_alias,
            ),
        )

        yield Argument(
            cli_name=python_name.replace("_", "-"),
            aliases=tuple(map(str, aliases)),
            annotation=field.annotation,
            default=field.default,
            required=field.is_required(),
            metadata=field.metadata,
            help=field.description or None,
        )
