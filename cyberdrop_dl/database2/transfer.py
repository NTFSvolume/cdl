import datetime
import shutil
import sqlite3
import sys
from pathlib import Path

from cyberdrop_dl.database2.tables import Downloads, Files, Hash, History

create_downloads = f"""
{Downloads.to_sql_schema()}

INSERT INTO downloads (media_id, folder, file_name, original_file_name, created_at, completed_at)
SELECT m.id,
        old.download_path AS folder,
        COALESCE(NULLIF(old.download_filename, ''), old.original_filename) AS file_name,
        old.original_filename AS original_file_name,
        COALESCE(old.created_at, datetime('now')) AS created_at,
        old.completed_at AS completed_at
FROM old.media AS old
JOIN media AS m
    ON m.domain = old.domain AND m.url_path = old.url_path
WHERE old.download_filename IS NOT NULL
    AND old.download_path IS NOT NULL
ORDER BY m.id, COALESCE(old.created_at, ''), old.rowid;
"""

transfer_media = f"""
{History.to_sql_schema()}

INSERT INTO media (domain, url_path, referer, name, album_id, size, duration, created_at)
SELECT domain,
        url_path,
        COALESCE(referer, '') AS referer,
        COALESCE(original_filename, '') AS name,
        album_id,
        file_size AS size,
        duration,
        COALESCE(created_at, datetime('now')) AS created_at
FROM (
    SELECT *,
            ROW_NUMBER() OVER (
            PARTITION BY domain, url_path
            ORDER BY
                CASE WHEN created_at IS NULL THEN 0 ELSE 1 END DESC,
                created_at DESC,
                rowid DESC
            ) AS rn
    FROM old.media
)
WHERE rn = 1;
"""

transfer_files = f"""
{Files.to_sql_schema()}

INSERT INTO files (folder, name, size, modtime)
    SELECT folder,
            COALESCE(NULLIF(download_filename, ''), original_filename) AS name,
            file_size AS size,
            CASE WHEN date IS NOT NULL THEN datetime(date, 'unixepoch') ELSE NULL END AS modtime
    FROM (
        SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY folder,
                            COALESCE(NULLIF(download_filename, ''), original_filename)
                ORDER BY
                    CASE WHEN date IS NULL THEN 0 ELSE 1 END DESC,
                    date DESC,
                    rowid DESC
                ) AS rn
        FROM old.files
    )
    WHERE rn = 1
        AND COALESCE(NULLIF(download_filename, ''), original_filename) IS NOT NULL
        AND COALESCE(NULLIF(download_filename, ''), original_filename) <> '';
"""

transfer_hash = f"""
{Hash.to_sql_schema()}

INSERT INTO hash (file_id, algorithm, hash)
SELECT f.id AS file_id,
       ohb.hash_type AS algorithm,
       ohb.hash AS hash
FROM old.hash AS ohb
JOIN files AS f
  ON f.folder = ohb.folder
 AND f.name = ohb.download_filename
ORDER BY f.id;
"""


def migrate(old_db: Path, new_db: Path) -> None:
    if not old_db.is_file():
        raise FileNotFoundError(f"old_path not found: {old_db}")

    now = datetime.datetime.now(datetime.UTC).replace(microsecond=0).strftime("%Y%m%d_%H%M%S")
    bak = old_db.parent / f"{old_db.stem}_{now}.bak{old_db.suffix}"
    print(f"Created backup at {bak}")  # noqa: T201
    __ = shutil.copy2(old_db, bak)

    if new_db.exists():
        raise FileExistsError(new_db)

    new_db.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(new_db)

    try:
        with conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA foreign_keys = OFF;")
            conn.execute("ATTACH DATABASE ? AS old;", (str(old_db),))
            conn.executescript(transfer_media)
            conn.executescript(create_downloads)
            conn.executescript(transfer_files)
            conn.executescript(transfer_hash)
            conn.execute("DETACH DATABASE old;")
            conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        conn.close()
        new_db.unlink()
        raise
    else:
        conn.close()

    def count(conn: sqlite3.Connection, table: str) -> int:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table};")
        return cur.fetchone()[0]

    tables = "media", "files", "hash"
    with sqlite3.connect(new_db) as new_conn:
        rows_copied = {name: count(new_conn, name) for name in tables}

    with sqlite3.connect(old_db) as old_conn:
        rows_old = {name: count(old_conn, name) for name in tables}

    for table in tables:
        print(f"Copied {rows_copied[table]:,} {table} rows into {new_db} (old had {rows_old[table]:,} rows).")  # noqa: T201


if __name__ == "__main__":
    old_db = Path(sys.argv[1])
    new_db = Path("new.db")
    migrate(old_db, new_db)
