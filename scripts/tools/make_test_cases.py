from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cyberdrop_dl.__main__ import main as run_cdl
from cyberdrop_dl.crawlers.crawler import SKIP_DOWNLOAD

if TYPE_CHECKING:
    from collections.abc import Generator

KEYS = ("url", "filename", "debrid_link", "original_filename", "referer", "album_id", "uploaded_at", "download_folder")
ROOT = Path(__file__).resolve().parents[2]
TEST_FOLDER = ROOT / "tests/crawlers/test_cases"

TestCase = dict[str, Any]


def parse(file: Path) -> Generator[tuple[str, str, TestCase]]:
    for line in file.read_text().splitlines():
        media = json.loads(line)
        url = media["parents"][0] if media["parents"] else media["referer"]

        media["download_folder"] = "re:" + media["download_folder"].split("/")[-1]
        yield media["domain"], url, {key: media[key] for key in KEYS}


def run(url_txt: Path, main_log: Path) -> None:
    with tempfile.TemporaryDirectory() as temp:
        _ = SKIP_DOWNLOAD.set(True)
        _ = run_cdl(
            [
                "--download",
                "--appdata-folder",
                temp,
                "--input-file",
                str(url_txt),
                "--main-log",
                str(main_log),
                "--dump-json",
                "--ui",
                "simple",
            ]
        )


def create_test_files(file: Path) -> None:
    all_results: dict[str, dict[str, list[TestCase]]] = {}
    for domain, url, results in parse(file):
        site2 = all_results.setdefault(domain, {})

        site2.setdefault(url, []).append(results)

    test_files: list[Path] = []
    for site, cases in all_results.items():
        domain = site.replace(".", "_")
        test_cases = [(url, results) for url, results in cases.items()]
        content = f"DOMAIN = {site!r}\nTEST_CASES = {test_cases}"

        test_file = TEST_FOLDER / f"test_case_{domain}.py"
        _ = test_file.write_text(content)
        test_files.append(test_file)

    _ = subprocess.run(["ruff", "format", *test_files], check=False)


if __name__ == "__main__":
    main_log = Path("test_run.log").resolve()
    url_txt = Path("URLs.txt").resolve()
    main_log, url_txt = [Path(f).resolve() for f in (*sys.argv[1:], "test_run.log", "URLs.txt")[:2]]
    run(url_txt, main_log)
    json_l = main_log.with_suffix(".results.jsonl")
    create_test_files(json_l)
