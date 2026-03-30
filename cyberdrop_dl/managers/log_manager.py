from __future__ import annotations

import asyncio
import csv
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cyberdrop_dl.constants import CSV_DELIMITER
from cyberdrop_dl.exceptions import get_origin
from cyberdrop_dl.utils import json
from cyberdrop_dl.utils.logger import log_spacer

if TYPE_CHECKING:
    from collections.abc import Iterable

    from yarl import URL

    from cyberdrop_dl.data_structures.url_objects import MediaItem
    from cyberdrop_dl.managers.manager import Manager


logger = logging.getLogger(__name__)


class LogManager:
    def __init__(self, manager: Manager) -> None:
        self.manager = manager
        self.main_log: Path = manager.path_manager.main_log
        self.last_post_log: Path = manager.path_manager.last_forum_post_log
        self.unsupported_urls_log: Path = manager.path_manager.unsupported_urls_log
        self.download_error_log: Path = manager.path_manager.download_error_urls_log
        self.scrape_error_log: Path = manager.path_manager.scrape_error_urls_log
        self.jsonl_file = self.main_log.with_suffix(".results.jsonl")
        self._file_locks: dict[Path, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._has_headers: set[Path] = set()

    def startup(self) -> None:
        """Startup process for the file manager."""
        for var in vars(self).values():
            if isinstance(var, Path):
                var.unlink(missing_ok=True)

    async def write_jsonl(self, data: Iterable[dict[str, Any]]):
        async with self._file_locks[self.jsonl_file]:
            await json.dump_jsonl(data, self.jsonl_file)

    async def _write_to_csv(self, file: Path, **kwargs) -> None:
        """Write to the specified csv file. kwargs are columns for the CSV."""
        async with self._file_locks[file]:
            write_headers = file not in self._has_headers
            self._has_headers.add(file)

            def write():
                with file.open("a", encoding="utf8", newline="") as csv_file:
                    writer = csv.DictWriter(
                        csv_file, fieldnames=kwargs.keys(), delimiter=CSV_DELIMITER, quoting=csv.QUOTE_ALL
                    )
                    if write_headers:
                        writer.writeheader()
                    writer.writerow(kwargs)

            await asyncio.to_thread(write)

    def write_last_post_log(self, url: URL) -> None:
        """Writes to the last post log."""
        self.manager.task_group.create_task(self._write_to_csv(self.last_post_log, url=url))

    def write_unsupported_urls_log(self, url: URL, origin: URL | None = None) -> None:
        """Writes to the unsupported urls log."""
        self.manager.task_group.create_task(self._write_to_csv(self.unsupported_urls_log, url=url, origin=origin))

    def write_download_error_log(self, media_item: MediaItem, error_message: str) -> None:
        """Writes to the download error log."""
        origin = get_origin(media_item)
        self.manager.task_group.create_task(
            self._write_to_csv(
                self.download_error_log,
                url=media_item.url,
                error=error_message,
                referer=media_item.referer,
                origin=origin,
            )
        )

    def write_scrape_error_log(self, url: URL | str, error_message: str, origin: URL | Path | None = None) -> None:
        """Writes to the scrape error log."""
        self.manager.task_group.create_task(
            self._write_to_csv(self.scrape_error_log, url=url, error=error_message, origin=origin)
        )

    async def update_last_forum_post(self) -> None:
        """Updates the last forum post."""
        input_file = self.manager.path_manager.input_file

        def proceed():
            return input_file.is_file() and self.last_post_log.is_file()

        if await asyncio.to_thread(proceed):
            await asyncio.to_thread(_update_last_forum_post, input_file, self.last_post_log)


def _update_last_forum_post(input_file: Path, last_post_log: Path) -> None:
    log_spacer(20)
    logger.info("Updating Last Forum Posts...\n")

    current_urls, current_base_urls, new_urls, new_base_urls = [], [], [], []
    try:
        with input_file.open(encoding="utf8") as f:
            for line in f:
                url = base_url = line.strip().removesuffix("/")

                if "https" in url and "/post-" in url:
                    base_url = url.rsplit("/post", 1)[0]

                # only keep 1 url of the same thread
                if base_url not in current_base_urls:
                    current_urls.append(url)
                    current_base_urls.append(base_url)
    except UnicodeDecodeError:
        logger.exception("Unable to read input file, skipping update_last_forum_post")
        return

    with last_post_log.open(encoding="utf8") as f:
        reader = csv.DictReader(f.readlines())
        for row in reader:
            new_url = base_url = row["url"].strip().removesuffix("/")  # type: ignore

            if "https" in new_url and "/post-" in new_url:
                base_url = new_url.rsplit("/post", 1)[0]

            # only keep 1 url of the same thread
            if base_url not in new_base_urls:
                new_urls.append(new_url)
                new_base_urls.append(base_url)

    updated_urls = current_urls.copy()
    for new_url, base in zip(new_urls, new_base_urls, strict=False):
        if base in current_base_urls:
            index = current_base_urls.index(base)
            old_url = current_urls[index]
            if old_url == new_url:
                continue
            logger.info(f"Updating {base}\n  {old_url = }\n  {new_url = }")
            updated_urls[index] = new_url

    if updated_urls == current_urls:
        logger.info("No URLs updated")
        return

    with input_file.open("w", encoding="utf8") as f:
        f.write("\n".join(updated_urls))
