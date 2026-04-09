from typing import Any, TypeVar

from pydantic import BaseModel

from cyberdrop_dl.cli.model import ParsedArgs

from . import Config


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


def merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    for key, val in dict1.items():
        if isinstance(val, dict):
            if key in dict2 and isinstance(dict2[key], dict):
                merge_dicts(dict1[key], dict2[key])
        else:
            if key in dict2:
                dict1[key] = dict2[key]

    for key, val in dict2.items():
        if key not in dict1:
            dict1[key] = val

    return dict1


def _merge_additive_args(config: Config, parsed_args: ParsedArgs) -> None:
    cli_general_options = parsed_args.global_settings.general
    cli_ignore_options = parsed_args.config_settings.ignore_options
    config_ignore_options = config.settings.ignore_options
    config_general_options = config.global_settings.general

    add_or_remove_lists(cli_ignore_options.skip_hosts, config_ignore_options.skip_hosts)
    add_or_remove_lists(cli_ignore_options.only_hosts, config_ignore_options.only_hosts)
    add_or_remove_lists(cli_general_options.disable_crawlers, config_general_options.disable_crawlers)


def merge_cli_and_config_args(config: Config, parsed_args: ParsedArgs) -> None:
    _merge_additive_args(config, parsed_args)

    conf = merge_models(config.settings, parsed_args.config_settings)
    global_conf = merge_models(config.global_settings, parsed_args.global_settings)
    deep_scrape = parsed_args.config_settings.runtime_options.deep_scrape or config.deep_scrape

    config.settings = conf
    config.global_settings = global_conf
    config.deep_scrape = deep_scrape


M = TypeVar("M", bound=BaseModel)


def merge_models(default: M, new: M) -> M:
    default_dict = default.model_dump()
    new_dict = new.model_dump(exclude_unset=True)

    updated_dict = merge_dicts(default_dict, new_dict)
    return default.model_validate(updated_dict)
