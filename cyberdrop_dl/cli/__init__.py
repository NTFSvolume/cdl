import shutil
from typing import Annotated

import cyclopts
import pydantic
from cyclopts import Parameter

from cyberdrop_dl import __version__, env, signature
from cyberdrop_dl.cli.model import CLIargs, ParsedArgs
from cyberdrop_dl.models.types import HttpURL


def is_terminal_in_portrait() -> bool:
    """Check if CDL is being run in portrait mode based on a few conditions."""

    if env.PORTRAIT_MODE:
        return True

    terminal_size = shutil.get_terminal_size()
    width, height = terminal_size.columns, terminal_size.lines
    aspect_ratio = width / height

    # High aspect ratios are likely to be in landscape mode
    if aspect_ratio >= 3.2:
        return False

    # Check for mobile device in portrait mode
    if (aspect_ratio < 1.5 and height >= 40) or (aspect_ratio < 2.3 and width <= 85):
        return True

    # Assume landscape mode for other cases
    return False


class App(cyclopts.App):
    @signature.copy(cyclopts.App._parse_known_args)
    def _parse_known_args(self, *args, **kwargs):
        from cyberdrop_dl.utils.yaml import format_validation_error

        try:
            return super()._parse_known_args(*args, **kwargs)
        except cyclopts.ValidationError as e:
            if isinstance(e.__cause__, pydantic.ValidationError):
                e.exception_message = format_validation_error(e.__cause__, title="CLI arguments")
            raise


app = App(
    help="Bulk asynchronous downloader for multiple file hosts",
    version=f"{__version__}.NTFS",
    default_parameter=Parameter(negative_iterable=[]),
)


@app.command()
def download(
    links: Annotated[
        list[HttpURL] | None,
        Parameter(
            name="links",
            negative=[],
            help="link(s) to content to download",
        ),
    ] = None,
    /,
    *,
    cli_args: CLIargs = CLIargs(),  # noqa: B008  # pyright: ignore[reportCallInDefaultInitializer]
    parsed_settings: ParsedArgs = ParsedArgs(),  # pyright: ignore[reportCallInDefaultInitializer]  # noqa: B008
):
    return links, cli_args, parsed_settings


@app.command()
def show_supported_sites() -> None:
    from cyberdrop_dl.utils.markdown import get_crawlers_info_as_rich_table

    table = get_crawlers_info_as_rich_table()
    app.console.print(table)


if __name__ == "__main__":
    app()
