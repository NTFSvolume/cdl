from __future__ import annotations

import functools
import json
from typing import TYPE_CHECKING, Any, NamedTuple, ParamSpec, TypeVar, overload

import bs4.css
from bs4 import BeautifulSoup

from cyberdrop_dl.exceptions import ScrapeError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from bs4.element import Tag

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


class SelectorError(ScrapeError):
    def __init__(self, message: str | None = None) -> None:
        super().__init__(422, message)


class CssAttributeSelector(NamedTuple):
    element: str
    attribute: str = ""

    def __call__(self, soup: Tag) -> str:
        return select(soup, self.element, self.attribute)

    def text(self, tag: Tag) -> str:
        return select_text(tag, self.element)


def _not_none(func: Callable[_P, _R | None]) -> Callable[_P, _R]:
    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        result = func(*args, **kwargs)
        if result is None:
            raise SelectorError
        return result

    return wrapper


@_not_none
def _select_one(tag: Tag, selector: str) -> Tag | None:
    """Same as `tag.select_one` but asserts the result is not `None`"""
    return tag.select_one(selector)


def select_text(tag: Tag, selector: str, strip: bool = True, *, decompose: str | None = None) -> str:
    """Same as `tag.select_one.get_text(strip=strip)` but asserts the result is not `None`"""
    inner_tag = select(tag, selector)
    if decompose:
        for trash in iselect(inner_tag, decompose):
            trash.decompose()
    return get_text(inner_tag, strip)


def _get_attr(tag: Tag, attribute: str) -> str | None:
    """Same as `tag.get(attribute)` but asserts the result is a single str"""
    attribute_ = attribute
    if attribute_ == "srcset":
        if (srcset := tag.get(attribute_)) and isinstance(srcset, str):
            return _parse_srcset(srcset)
        attribute_ = "src"

    if attribute_ == "src":
        value = tag.get("data-src") or tag.get(attribute_)
    else:
        value = tag.get(attribute_)
    if isinstance(value, list):
        raise SelectorError(f"Expected a single value for {attribute = !r}, got multiple")
    return value


def get_text(tag: Tag, strip: bool = True) -> str:
    return tag.get_text(strip=strip)


@_not_none
def get_attr(tag: Tag, attribute: str) -> str | None:
    """Same as `tag.get(attribute)` but asserts the result is not `None` and is a single string"""
    return _get_attr(tag, attribute)


@overload
def select(tag: Tag, selector: str) -> Tag: ...


@overload
def select(tag: Tag, selector: str, attribute: str) -> str: ...


def select(tag: Tag, selector: str, attribute: str | None = None) -> Tag | str:
    inner_tag = _select_one(tag, selector)
    if not attribute:
        return inner_tag
    return get_attr(inner_tag, attribute)


@overload
def iselect(tag: Tag, selector: str) -> Generator[Tag]: ...


@overload
def iselect(tag: Tag, selector: str, attribute: str) -> Generator[str]: ...


def iselect(tag: Tag, selector: str, attribute: str | None = None) -> Generator[Tag] | Generator[str]:
    """Same as `tag.select(selector)`, but it returns a generator instead of a list."""
    tags = bs4.css.CSS(tag).iselect(selector)
    if not attribute:
        yield from tags

    else:
        for inner_tag in tags:
            if attr := _get_attr(inner_tag, attribute):
                yield attr


def _parse_srcset(srcset: str) -> str:
    # The best src is the last one (usually)
    return [src.split(" ")[0] for src in srcset.split(", ")][-1]


def decompose(tag: Tag, selector: str) -> None:
    for inner_tag in tag.select(selector):
        inner_tag.decompose()


def sanitize_page_title(title: str, domain: str) -> str:
    sld = domain.rsplit(".", 1)[0].casefold()

    def clean(string: str, char: str):
        if char in string:
            front, _, tail = string.rpartition(char)
            if sld in tail.casefold():
                string = front.strip()
        return string

    return clean(clean(title, "|"), " - ")


def page_title(soup: Tag, domain: str | None = None) -> str:
    title = select_text(soup, "title")
    if domain:
        return sanitize_page_title(title, domain)
    return title


def json_ld(soup: Tag, /, contains: str | None = None) -> dict[str, Any]:
    selector = "script[type='application/ld+json']"
    if contains:
        selector += f":-soup-contains('{contains}')"

    ld_json = json.loads(select_text(soup, selector)) or {}
    if isinstance(ld_json, list):
        return ld_json[0]

    return ld_json


def json_ld_date(soup: Tag) -> str:
    return json_ld(soup)["uploadDate"]


def unescape(html: str) -> str:
    return make_soup(html).get_text()


def make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


iframes = CssAttributeSelector("iframe", "src")
images = CssAttributeSelector("img", "srcset")
links = CssAttributeSelector(":any-link", "href")
