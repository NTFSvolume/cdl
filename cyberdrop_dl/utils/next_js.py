from __future__ import annotations

import dataclasses
import itertools
import json
import re
from collections.abc import Generator
from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

    from bs4 import BeautifulSoup


class FlightDataType(IntEnum):
    BOOTSTRAP = 0
    PAYLOAD = 1
    FORM_STATE = 2
    BINARY = 3


@dataclasses.dataclass(slots=True, order=True)
class FlightChunk:
    index: int = dataclasses.field(init=False)
    id: str
    marker: str | None
    data: Any = dataclasses.field(init=False)
    raw_data: str
    resolved: bool = False
    hints: list[str] = dataclasses.field(default_factory=list)

    def __post_init__(self) -> None:
        self.index = int(self.id, base=16)
        self.data, *self.hints = self.raw_data.split("\n:H")


# Map of chunk_id (hex index of the chunk) -> list of all the objects (components) created from that chunk
NextJSFlight = dict[str, list[dict[str, Any]]]


_find_chunks = re.compile(r'^(?<![0-9A-Za-z:"])([0-9a-f]+):([A-Z]{0,2})(["\[\{]|null)', re.MULTILINE).finditer


def ifind(next_flight: NextJSFlight, *attrs: str) -> Generator[dict[str, Any]]:
    """Yield every object within `next_flight` that have the required `attrs`."""
    needed = frozenset(attrs)

    def walk(obj: Any) -> Generator[dict[str, Any]]:
        if isinstance(obj, dict):
            if needed.issubset(obj):
                yield obj
            else:
                for v in obj.values():
                    yield from walk(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from walk(v)

    for ele in next_flight.values():
        yield from walk(ele)


def find(next_flight: NextJSFlight, *attrs: str) -> dict[str, Any]:
    """Get the first object within `next_flight` that have the required `attrs`."""
    return next(ifind(next_flight, *attrs))


def extract(soup: BeautifulSoup) -> NextJSFlight:
    return parse(_extract(soup))


def parse(flight_data: str) -> NextJSFlight:
    return {chunk.id: chunk.data for chunk in _parse_chunks(flight_data)}


def _extract(soup: BeautifulSoup) -> str:
    return "".join((data for _, data in _get_flight_chunks(soup)))


def _get_flight_chunks(soup: BeautifulSoup) -> Generator[tuple[FlightDataType, str]]:
    for script in soup.select("script:-soup-contains('self.__next_f.push(')"):
        js_text = script.get_text()
        raw_data = js_text[js_text.find("(") + 1 : js_text.rfind(")")]
        type, data = json.loads(raw_data)
        type = FlightDataType(type)
        if type is FlightDataType.PAYLOAD:
            yield type, data


def _parse_chunks(flight_data: str) -> Generator[FlightChunk]:
    chunks: dict[str, FlightChunk] = {}

    def revive(value: Any) -> Any:
        if not value:
            return value

        if isinstance(value, str):
            if value[0] != "$":
                return value

            if value == "$":
                return ""

            match value[1]:
                case "$":
                    return value[2:]
                case "@" | "L":
                    chunk_id = value[2:]
                    return revive(chunks[chunk_id])
                case "u":
                    return None
                case "D":
                    return value[2:]
                case "n":
                    return int(value[2:])
                case _:
                    return value[1:]

        elif isinstance(value, list):
            for idx, obj in enumerate(value):
                value[idx] = revive(obj)

        elif isinstance(value, dict):
            for key, obj in value.items():
                value[key] = revive(obj)

        elif isinstance(value, FlightChunk):
            initialize(value)
            return revive(value.data)

        return value

    def initialize(chunk: FlightChunk) -> None:
        if chunk.resolved:
            return
        try:
            raw_value = json.loads(chunk.data) if isinstance(chunk.data, str) else chunk.data
        except json.decoder.JSONDecodeError:
            chunk.data = "ERROR"
        else:
            chunk.data = revive(raw_value)
        finally:
            chunk.resolved = True

    flight_data = flight_data.replace('"$undefined"', "null")
    indices = list(_find_chunks(flight_data))

    for m, next_m in itertools.zip_longest(indices, indices[1:]):
        chunk_id, marker, delimiter = m.groups()
        end = next_m.start() if next_m is not None else -1
        payload = flight_data[m.end() - 1 : end].strip()
        chunks[chunk_id] = chunk = FlightChunk(chunk_id, marker=marker or None, raw_data=payload)
        if delimiter == "null":
            chunk.data = None

    for chunk in sorted(chunks.values()):
        initialize(chunk)
        yield chunk
