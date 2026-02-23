from __future__ import annotations

import asyncio
import csv
import dataclasses
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from cyberdrop_dl.exceptions import get_origin
from cyberdrop_dl.utils import json

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from yarl import URL

    from cyberdrop_dl.config import Config
    from cyberdrop_dl.data_structures.url_objects import MediaItem, ScrapeItem

_CSV_DELIMITER = ","

_file_locks: defaultdict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)


@dataclasses.dataclass(slots=True, frozen=True)
class LogManager:
    config: Config
    task_group: asyncio.TaskGroup = dataclasses.field(default_factory=asyncio.TaskGroup, repr=False)
    _has_headers: set[Path] = dataclasses.field(init=False, default_factory=set)

    async def write_jsonl(self, data: Iterable[dict[str, Any]]) -> None:
        async with _file_locks[self.config.logs.jsonl_file]:
            await asyncio.to_thread(json.dump_jsonl, data, self.config.logs.jsonl_file)

    async def _write_to_csv(self, file: Path, **kwargs: Any) -> None:
        """Write to the specified csv file. kwargs are columns for the CSV."""
        async with _file_locks[file]:
            is_first_write = file not in self._has_headers
            self._has_headers.add(file)

            def write():
                if is_first_write:
                    file.parent.mkdir(parents=True, exist_ok=True)
                    file.unlink(missing_ok=True)

                with file.open("a", encoding="utf8", newline="") as csv_file:
                    writer = csv.DictWriter(
                        csv_file, fieldnames=kwargs, delimiter=_CSV_DELIMITER, quoting=csv.QUOTE_ALL
                    )
                    if is_first_write:
                        writer.writeheader()
                    writer.writerow(kwargs)

            await asyncio.to_thread(write)

    def write_last_post_log(self, url: URL) -> None:
        _ = self.task_group.create_task(self._write_to_csv(self.config.logs.last_forum_post, url=url))

    def write_unsupported(self, url: URL, origin: ScrapeItem | URL | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(self.config.logs.unsupported_urls, url=url, origin=get_origin(origin))
        )

    def write_download_error_log(self, media_item: MediaItem, error_message: str) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.download_error_urls,
                url=media_item.url,
                error=error_message,
                referer=media_item.referer,
                origin=get_origin(media_item),
            )
        )

    def write_scrape_error_log(self, url: URL | str, error_message: str, origin: URL | Path | None = None) -> None:
        _ = self.task_group.create_task(
            self._write_to_csv(
                self.config.logs.scrape_error_urls,
                url=url,
                error=error_message,
                origin=origin,
            )
        )
