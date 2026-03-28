from __future__ import annotations

import dataclasses
import datetime
import email.utils
from collections.abc import Generator
from types import MappingProxyType
from typing import TYPE_CHECKING
from xml.etree import ElementTree

from yarl import URL

if TYPE_CHECKING:
    from collections.abc import Generator


_PROPERTIES = (
    "creationdate",
    "displayname",
    "getcontentlength",
    "getcontenttype",
    "getetag",
    "getlastmodified",
    "resourcetype",
    "status",
)


@dataclasses.dataclass(slots=True)
class Node:
    display_name: str
    content_type: str
    etag: str
    last_modified: datetime.datetime
    creation_date: datetime.datetime
    status: str
    href: str

    resource_type: str | None = None
    content_length: int | None = None

    @property
    def is_dir(self) -> bool:
        return self.resource_type == "collection"


_NODE_PROPERTIES_MAP: MappingProxyType[str, str] = MappingProxyType(
    {f.name.replace("_", ""): f.name for f in dataclasses.fields(Node)}
)


def parse_resp(xml_resp: str) -> Generator[Node]:
    root = ElementTree.fromstring(xml_resp)

    for response in root.iterfind(".//{DAV:}response"):
        href = response.find("{DAV:}href")
        assert href is not None
        assert href.text is not None

        node = dict(_parse_node(response))
        yield Node(
            etag=node.pop("etag").strip('"'),
            creation_date=datetime.datetime.fromisoformat(node.pop("creation_date")),
            last_modified=email.utils.parsedate_to_datetime(node.pop("last_modified")),
            content_length=int(node.pop("content_length", 0)) or None,
            **node,
            href=URL(href.text, encoded="%" in href.text).path,
        )


def _parse_node(response: ElementTree.Element[str]) -> Generator[tuple[str, str]]:
    for prop in _PROPERTIES:
        value = response.findtext(".//{DAV:}" + prop)
        if value is not None:
            name = _NODE_PROPERTIES_MAP[prop.removeprefix("get")]
            yield name, value


def prepare_request(*properties: str) -> bytes:
    root = ElementTree.Element(
        "d:propfind",
        attrib={"xmlns:d": "DAV:"},
    )

    prop_tag = ElementTree.SubElement(root, "d:prop")
    for prop in properties:
        if prop not in ("status",):
            ElementTree.SubElement(prop_tag, "d:" + prop)

    ElementTree.indent(root, space="  ")

    return ElementTree.tostring(root, xml_declaration=True, encoding="utf-8")


PROPERTIES_XML = prepare_request(*_PROPERTIES)
