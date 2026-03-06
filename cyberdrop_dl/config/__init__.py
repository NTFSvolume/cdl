from __future__ import annotations

import dataclasses
from contextvars import ContextVar, Token
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, ClassVar, Self

from cyclopts.bind import normalize_tokens
from cyclopts.core import App
from cyclopts.parameter import Parameter
from pydantic import BaseModel

from cyberdrop_dl import yaml
from cyberdrop_dl.config.auth import AuthSettings
from cyberdrop_dl.config.settings import ConfigSettings
from cyberdrop_dl.models import get_model_fields, merge_models

if TYPE_CHECKING:
    from collections.abc import Iterable

    from cyclopts.argument import ArgumentCollection

_config: ContextVar[Config] = ContextVar("_config")


class Config(ConfigSettings):
    auth: Annotated[AuthSettings, Parameter(show=False)] = AuthSettings()
    _source: Path | None = None

    _token: Token[Config] | None = None
    _resolved: bool = False

    @property
    def source(self) -> Path | None:
        return self._source

    def __enter__(self) -> Self:
        self._token = _config.set(self)
        return self

    def __exit__(self, *_) -> None:
        assert self._token is not None
        _config.reset(self._token)

    def save(self, file: Path) -> None:
        yaml.dump(file, self)

    def resolve_paths(self) -> None:
        if self._resolved:
            return
        self._resolve_paths(self)
        self.logs.delete_old_logs_and_folders()
        self._resolved = True

    @classmethod
    def _resolve_paths(cls, model: BaseModel) -> None:
        for name, value in vars(model).items():
            if isinstance(value, Path):
                setattr(model, name, value.expanduser().resolve().absolute())

            elif isinstance(value, BaseModel):
                cls._resolve_paths(value)

    def update(self, other: Self) -> Self:
        return merge_models(self, other)

    @classmethod
    def load(cls, file: Path) -> Config:
        default = cls()
        if not file.is_file():
            config = default
            overwrite = True

        else:
            all_fields = get_model_fields(default, exclude_unset=False)
            config = cls.model_validate(yaml.load(file))
            set_fields = get_model_fields(config)
            overwrite = all_fields != set_fields

        if overwrite:
            config.save(file)

        config._source = file
        return config


def get() -> Config:
    return _config.get()


def add_or_remove_lists(cli_values: list[str], config_values: list[str]) -> None:
    exclude = {"+", "-"}
    if cli_values:
        if cli_values[0] == "+":
            new_values_set = set(config_values + cli_values)
            cli_values.clear()
            cli_values.extend(sorted(new_values_set - exclude))
        elif cli_values[0] == "-":
            new_values_set = set(config_values) - set(cli_values)
            cli_values.clear()
            cli_values.extend(sorted(new_values_set - exclude))


def _coerce(*, config: Config) -> Config:
    return config


@dataclasses.dataclass(slots=True)
class _ConfigParser:
    app: App = dataclasses.field(init=False)
    args: ArgumentCollection = dataclasses.field(init=False)

    _instance: ClassVar[_ConfigParser | None] = None

    def __new__(cls) -> _ConfigParser:
        if cls._instance is None:
            cls._instance = self = super().__new__(cls)
            self.app = App(print_error=False, exit_on_error=False)
            _ = self.app.command(name="coerce")(_coerce)
            self.args = self.app.assemble_argument_collection()
        return cls._instance

    def __call__(self, tokens: str | Iterable[str]) -> Config:
        fn, bound, *_ = self.app.parse_args(["coerce", *normalize_tokens(tokens)])  # pyright: ignore[reportUnknownMemberType]
        assert fn is _coerce
        return _coerce(*bound.args, **bound.kwargs)


def parse_args(tokens: str | Iterable[str]) -> Config:
    return _ConfigParser()(tokens)


def generate_cli_command() -> tuple[str, ...]:
    return tuple(a.name for a in _ConfigParser().args)
