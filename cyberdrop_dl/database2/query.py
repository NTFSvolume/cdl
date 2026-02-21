from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, NewType

if TYPE_CHECKING:
    from cyberdrop_dl.database2.tables import Table

Command = NewType("Command", str)


def create(table: Table, **foreign: tuple[str, ...]) -> Command:
    params = ", ".join(f"{key} {' '.join(map(str.upper, props))}" for key, props in table.columns.items())
    for name, keys in foreign.items():
        f_params = ", ".join(keys)
        params += f", FOREIGN KEY ({f_params}) REFERENCES {name}({f_params})"

    return Command(f"CREATE TABLE IF NOT EXISTS {table.name} ({params})")


def exists(table: Table, **where: Any) -> tuple[Command, tuple[Any, ...]]:
    table.check_columns(where.keys())
    conditions = " AND ".join(f"{key}=?" for key in where.keys())
    command = f"SELECT EXISTS(SELECT 1 FROM {table.name} WHERE {conditions})"
    return Command(command), tuple(where.values())


def insert(table: Table, **values: Any) -> tuple[Command, tuple[Any, ...]]:
    return _insert(table, "INSERT", **values)


def insert_or_ignore(table: Table, **values: Any) -> tuple[Command, tuple[Any, ...]]:
    return _insert(table, "INSERT OR IGNORE", **values)


def _insert(table: Table, exc: str = "INSERT", **values: Any) -> tuple[Command, tuple[Any, ...]]:
    table.check_columns(values.keys())
    assert len(values) == len(table.column_names)
    columns = ", ".join(values.keys())
    placeholders = ", ".join("?" for _ in values)
    command = f"{exc} INTO {table.name} ({columns}) VALUES ({placeholders})"
    return Command(command), tuple(values.values())


def select(table: Table, *columns: str, limit: int | None = None, **where: Any) -> tuple[Command, tuple[Any, ...]]:
    assert columns
    table.check_columns(columns)
    wanted = ", ".join(columns)
    command = f"SELECT {wanted} FROM {table.name}"
    if where:
        table.check_columns(where.keys())
        conditions = " AND ".join(f"{key}=?" for key in where.keys())
        command += f" WHERE {conditions}"
    if limit:
        command += f" LIMIT {limit}"

    return Command(command), tuple(where.values())


def update(table: Table, **row: Any) -> tuple[Command, tuple[Any, ...]]:
    table.check_columns(row.keys())

    p_keys: dict[str, Any] = {}
    other_keys: dict[str, Any] = {}
    for key, value in row.items():
        if key in table.primary_keys:
            p_keys[key] = value
        else:
            other_keys[key] = value

    assert p_keys
    assert other_keys

    new = ", ".join(f"{key}={_placeholder(v)}" for key, v in other_keys.items())
    conditions = " AND ".join(f"{key}=?" for key in p_keys)
    command = f"UPDATE {table.name} SET {new} WHERE {conditions}"
    values = *(v for v in other_keys.values() if v != "CURRENT_TIMESTAMP"), *p_keys.values()

    return Command(command), values


def _placeholder(v: Any) -> Literal["CURRENT_TIMESTAMP", "?"]:
    if v == "CURRENT_TIMESTAMP":
        return v
    return "?"
