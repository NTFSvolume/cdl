from __future__ import annotations

import dataclasses
import datetime
import email.utils
from types import MappingProxyType
from typing import TYPE_CHECKING, Final
from xml.etree import ElementTree

from yarl import URL

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Mapping


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
_NAMESPACE = ("d", "DAV:")

ElementTree.register_namespace(*_NAMESPACE)


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


def prepare_request(
    *properties: str,
    namespaces: Mapping[str, str] | Iterable[tuple[str, str]] | None = None,
) -> ElementTree.Element[str]:
    ns: dict[str, str] = dict([_NAMESPACE])
    if namespaces is not None:
        ns.update(namespaces)

    root = ElementTree.Element(
        "d:propfind",
        attrib={f"xmlns:{prefix}": uri for prefix, uri in ns.items()},
    )

    prop_tag = ElementTree.SubElement(root, "d:prop")
    for prop in properties:
        if prop not in ("status",):
            _ = ElementTree.SubElement(prop_tag, f"d:{prop}" if ":" not in prop else prop)

    ElementTree.indent(root, space="  ")
    return root


def update_tags_from_ns(root: ElementTree.Element[str]) -> ElementTree.Element[str]:
    return ElementTree.fromstring(xml_to_bytes(root))


def xml_to_bytes(root: ElementTree.Element[str]) -> bytes:
    return ElementTree.tostring(root, xml_declaration=True, encoding="utf-8")


PROPERTIES_XML = xml_to_bytes(prepare_request(*_PROPERTIES))
