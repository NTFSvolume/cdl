from __future__ import annotations

import dataclasses
import datetime
import email.utils
from collections.abc import Generator
from types import MappingProxyType
from typing import TYPE_CHECKING, Final
from xml.etree import ElementTree

from yarl import URL

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping


_PROPERTIES: Final = (
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


_NODE_FIELDS_MAP: dict[str, str] = {f.name.replace("_", ""): f.name for f in dataclasses.fields(Node)}


_PROPERTY_TO_NODE_ATTR_MAP: MappingProxyType[str, str] = MappingProxyType(
    {prop: _NODE_FIELDS_MAP[prop.removeprefix("get")] for prop in _PROPERTIES}
)

del _NODE_FIELDS_MAP


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
            name = _PROPERTY_TO_NODE_ATTR_MAP[prop]
            yield name, value


def prepare_request(*properties: str, namespaces: Mapping[str, str] | None = None) -> bytes:
    root = ElementTree.Element(
        "d:propfind",
        attrib={f"xmlns:{name}": uri for name, uri in {"d": "DAV:", **(namespaces or {})}.items()},
    )

    prop_tag = ElementTree.SubElement(root, "d:prop")
    for prop in properties:
        if prop not in ("status",):
            _ = ElementTree.SubElement(prop_tag, f"d:{prop}" if ":" not in prop else prop)

    ElementTree.indent(root, space="  ")

    return ElementTree.tostring(root, xml_declaration=True, encoding="utf-8")


PROPERTIES_XML = prepare_request(*_PROPERTIES)
