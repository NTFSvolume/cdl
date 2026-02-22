from __future__ import annotations

import dataclasses
import sys
from argparse import SUPPRESS, ArgumentParser, RawDescriptionHelpFormatter
from shutil import get_terminal_size
from typing import TYPE_CHECKING, Any, Final, NoReturn

from pydantic import BaseModel, ValidationError

from cyberdrop_dl import __version__, env
from cyberdrop_dl.cli import arguments
from cyberdrop_dl.cli.model import CLIargs, ParsedArgs
from cyberdrop_dl.config import ConfigSettings, GlobalSettings

if TYPE_CHECKING:
    from argparse import _ArgumentGroup as ArgGroup  # pyright: ignore[reportPrivateUsage]
    from collections.abc import Sequence


def is_terminal_in_portrait() -> bool:
    """Check if CDL is being run in portrait mode based on a few conditions."""

    if env.PORTRAIT_MODE:
        return True

    terminal_size = get_terminal_size()
    width, height = terminal_size.columns, terminal_size.lines
    aspect_ratio = width / height

    # High aspect ratios are likely to be in landscape mode
    if aspect_ratio >= 3.2:
        return False

    # Check for mobile device in portrait mode
    if (aspect_ratio < 1.5 and height >= 40) or (width <= 85 and aspect_ratio < 2.3):
        return True

    # Assume landscape mode for other cases
    return False


class CustomHelpFormatter(RawDescriptionHelpFormatter):
    MAX_HELP_POS: Final = 80
    INDENT_INCREMENT: Final = 2

    def __init__(self, prog: str, width: int | None = None) -> None:
        super().__init__(prog, self.INDENT_INCREMENT, self.MAX_HELP_POS, width)

    def _get_help_string(self, action) -> str | None:
        if action.help:
            return action.help.replace("program's", "CDL")  # The ' messes up the markdown formatting
        return action.help


@dataclasses.dataclass(slots=True)
class CLIParser:
    parser: ArgumentParser
    groups: dict[str, list[ArgGroup]]

    def parse_args(self, args: Sequence[str] | None = None) -> dict[str, dict[str, Any]]:
        return self._unflatten(self._parse_args(args))

    def _parse_args(self, args: Sequence[str] | None = None) -> dict[str, Any]:
        return dict(sorted(vars(self.parser.parse_intermixed_args(args)).items()))

    def _unflatten(self, namespace: dict[str, Any]) -> dict[str, dict[str, Any]]:
        parsed_args: dict[str, dict[str, Any]] = {}

        for name, groups in self.groups.items():
            parsed_args[name] = {}
            for group in groups:
                group_dict = {arg.dest: v for arg in group._group_actions if (v := namespace.get(arg.dest)) is not None}
                if group_dict:
                    assert group.title
                    parsed_args[name][group.title] = _unflatten_nested_args(group_dict)

        parsed_args["cli_only_args"] = parsed_args["cli_only_args"]["CLI-only options"]
        return parsed_args


def make_parser() -> CLIParser:
    kwargs: dict[str, Any] = {"color": True} if sys.version_info > (3, 14) else {}
    parser = ArgumentParser(
        description="Bulk asynchronous downloader for multiple file hosts",
        usage="cyberdrop-dl [OPTIONS] URL [URL...]",
        allow_abbrev=False,
        formatter_class=CustomHelpFormatter,
        **kwargs,
    )
    _ = parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")

    cli_only = parser.add_argument_group("CLI-only options")
    _add_args_from_model(cli_only, CLIargs)

    groups = {
        "config_settings": _create_groups_from_nested_models(parser, ConfigSettings),
        "global_settings": _create_groups_from_nested_models(parser, GlobalSettings),
        "cli_only_args": [cli_only],
    }

    return CLIParser(parser, groups)


def parse_args(args: Sequence[str] | None = None) -> ParsedArgs:
    """Parses the command line arguments passed into the program."""
    from cyberdrop_dl.utils.yaml import handle_validation_error

    parsed_args = make_parser().parse_args(args)
    try:
        model = ParsedArgs.model_validate(parsed_args, extra="forbid")

    except ValidationError as e:
        handle_validation_error(e, title="CLI arguments")
        sys.exit(1)

    if model.cli_only_args.show_supported_sites:
        show_supported_sites()

    return model


def show_supported_sites() -> NoReturn:
    from rich import print

    from cyberdrop_dl.utils.markdown import get_crawlers_info_as_rich_table

    table = get_crawlers_info_as_rich_table()
    print(table)
    sys.exit(0)


def _unflatten_nested_args(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for command_name, value in data.items():
        inner_names = command_name.split(".")
        current_level = result
        for index, key in enumerate(inner_names):
            if index < len(inner_names) - 1:
                if key not in current_level:
                    current_level[key] = {}
                current_level = current_level[key]
            else:
                current_level[key] = value
    return result


def _add_args_from_model(parser: ArgumentParser | ArgGroup, model: type[BaseModel]) -> None:
    cli_args = model is CLIargs

    for arg in arguments.parse(model):
        options = arg.compose_options()

        if cli_args and arg.arg_type is bool and not (arg.cli_name == "portrait" and env.RUNNING_IN_TERMUX):
            default = arg.default if cli_args else SUPPRESS
            options["action"] = "store_false" if default else "store_true"

        _ = parser.add_argument(*arg.name_or_flags, **options)


def _create_groups_from_nested_models(parser: ArgumentParser, model: type[BaseModel]) -> list[ArgGroup]:
    groups: list[ArgGroup] = []
    for name, field in model.model_fields.items():
        submodel = field.annotation
        assert submodel and issubclass(submodel, BaseModel)
        submodel_group = parser.add_argument_group(name)
        _add_args_from_model(submodel_group, submodel)
        groups.append(submodel_group)

    return groups
