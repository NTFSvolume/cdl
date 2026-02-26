from typing import Annotated, Literal

import cyclopts
import pydantic
from cyclopts import Parameter

from cyberdrop_dl import __version__
from cyberdrop_dl.annotations import copy_signature
from cyberdrop_dl.cli.model import CLIargs, ParsedArgs, RetryArgs
from cyberdrop_dl.models.types import HttpURL
from cyberdrop_dl.utils.yaml import format_validation_error


class App(cyclopts.App):
    @copy_signature(cyclopts.App._parse_known_args)
    def _parse_known_args(self, *args, **kwargs):
        try:
            return super()._parse_known_args(*args, **kwargs)
        except cyclopts.ValidationError as e:
            if isinstance(e.__cause__, pydantic.ValidationError):
                e.exception_message = format_validation_error(e.__cause__, title="CLI arguments")
            raise


app = App(
    name="cyberdrop-dl",
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
    """Scrape and download files from a list of URLs (from a file or stdin)"""
    return links, cli_args, parsed_settings


@app.command()
def show_supported_sites() -> None:
    """Show a list of all supported sites"""
    from cyberdrop_dl.utils.markdown import get_crawlers_info_as_rich_table

    table = get_crawlers_info_as_rich_table()
    app.console.print(table)


@app.command()
def retry(choice: Literal["all", "failed", "maintenance"], /, *, retry: RetryArgs | None = None):
    """Retry failed downloads"""
    return choice, retry or RetryArgs()


if __name__ == "__main__":
    app()
