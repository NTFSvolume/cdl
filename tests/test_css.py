import pytest

from cyberdrop_dl.utils import css


@pytest.mark.parametrize(
    "raw, domain, expected",
    [
        ("cyberdrop-dl | GitHub", "github.com", "cyberdrop-dl"),
        ("cyberdrop-dl - GitHub", "github.com", "cyberdrop-dl"),
        ("cyberdrop-dl | GitHub - Bar", "github.com", "cyberdrop-dl"),
        ("", "github.com", ""),
        ("   ", "github.com", "   "),
        # case-insensitive
        ("cyberdrop-dl | GITHUB", "GitHub.com", "cyberdrop-dl"),
        ("cyberdrop-dl - GiThUb", "GITHUB.io", "cyberdrop-dl"),
        # sub-domains
        ("News | www.github.co", "www.github.co.uk", "News"),
        # no match -> unchanged
        ("cyberdrop-dl | Foo", "github.com", "cyberdrop-dl | Foo"),
        ("cyberdrop-dl - Foo", "github.com", "cyberdrop-dl - Foo"),
        # clean up once
        ("cyberdrop-dl | Foo - GitHub", "github.com", "cyberdrop-dl | Foo"),
        ("A | B | GitHub", "github.com", "A | B"),
        ("A - B - GitHub", "github.com", "A - B"),
    ],
)
def test_rstrip_domain(raw: str, domain: str, expected: str) -> None:
    assert css.Title(raw).rstrip_domain(domain) == expected


def test_no_domain_raise_error() -> None:
    with pytest.raises(AssertionError):
        css.Title("cyberdrop-dl | Foo").rstrip_domain("")


def test_return_type_is_title() -> None:
    title = css.Title("cyberdrop-dl | GitHub")
    assert type(title.rstrip_domain("github.com")) is css.Title
