"""Normalize staging-table rows into analytics-friendly tables."""

from __future__ import annotations

import datetime as _dt
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Mapping, Sequence

from app.prep_excel import _default_metadata_column_definitions, TableMissingError

# Metadata fields that should always be preserved for downstream joins.
_METADATA_COLUMNS = ["raw_id", "file_hash", "batch_id", "source_year", "ingested_at"]

# Source-side columns that should never be copied directly into the normalized
# payload because they are handled separately (or represent bookkeeping data).
_RESERVED_SOURCE_COLUMNS = {"id", "processed_at"}

# Certain columns require stronger typing than the default VARCHAR fallback.
_COLUMN_TYPE_OVERRIDES = {
    "日期": "DATE NULL",
    "上課時數": "DECIMAL(6,2) NULL",
}


@dataclass(frozen=True)
class TableConfig:
    """Subset of sheet configuration required for normalization."""

    staging_table: str
    normalized_table: str
    column_mappings: Mapping[str, str]


def _coerce_date(value) -> _dt.date | None:
    if value is None:
        return None
    if isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
        return value
    if isinstance(value, _dt.datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    # Normalise common date delimiters.
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    text = text.replace("/", "-").replace(".", "-")
    try:
        return _dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        pass
    for fmt in ("%d-%m-%Y", "%Y-%d-%m", "%m-%d-%Y"):
        try:
            return _dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return _dt.date.fromisoformat(text)
    except ValueError:
        return None


def _coerce_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _coerce_source_year(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _coerce_ingested_at(value) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return _dt.datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return _dt.datetime.fromisoformat(text)
    except ValueError:
        return None


_COERCERS: Mapping[str, Callable[[object], object | None]] = {
    "日期": _coerce_date,
    "上課時數": _coerce_decimal,
}


def _coerce_business_value(column: str, value):
    coercer = _COERCERS.get(column)
    if coercer is None:
        return value if value != "" else None
    return coercer(value)


def _normalise_metadata(column: str, row: Mapping[str, object]):
    if column == "raw_id":
        return row.get("id")
    if column == "source_year":
        return _coerce_source_year(row.get(column))
    if column == "ingested_at":
        return _coerce_ingested_at(row.get(column))
    return row.get(column)


def resolve_column_mappings(
    rows: Sequence[Mapping[str, object]],
    column_mappings: Mapping[str, str] | None,
) -> "OrderedDict[str, str]":
    """Expand configured mappings with any new staging columns."""

    resolved: "OrderedDict[str, str]" = OrderedDict()
    if column_mappings:
        for normalized_column, source_column in column_mappings.items():
            resolved[normalized_column] = source_column

    if rows:
        staging_columns = list(rows[0].keys())
        for column in staging_columns:
            if column in _RESERVED_SOURCE_COLUMNS:
                continue
            if column in _METADATA_COLUMNS:
                continue
            if column in resolved:
                continue
            if column in resolved.values():
                # Avoid mapping the same source column twice when explicit
                # configuration already redirected it.
                continue
            resolved[column] = column

    return resolved


def _build_ordered_columns(column_mappings: Mapping[str, str]) -> list[str]:
    ordered = list(_METADATA_COLUMNS)
    for column in column_mappings:
        if column in ordered:
            continue
        ordered.append(column)
    return ordered


def _build_row(row: Mapping[str, object], column_mappings: Mapping[str, str]) -> tuple[object, ...]:
    values: list[object] = []
    ordered_columns = _build_ordered_columns(column_mappings)
    for column in ordered_columns:
        if column in _METADATA_COLUMNS:
            values.append(_normalise_metadata(column, row))
            continue
        source_column = column_mappings.get(column)
        value = row.get(source_column) if source_column else None
        values.append(_coerce_business_value(column, value))
    return tuple(values)


def build_insert_statement(
    table: str, column_mappings: Mapping[str, str]
) -> tuple[str, list[str]]:
    ordered_columns = _build_ordered_columns(column_mappings)
    column_sql = ", ".join(f"`{name}`" for name in ordered_columns)
    placeholders = ", ".join(["%s"] * len(ordered_columns))
    sql = f"INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders})"
    return sql, ordered_columns


def prepare_rows(
    rows: Iterable[Mapping[str, object]],
    column_mappings: Mapping[str, str],
) -> list[tuple[object, ...]]:
    prepared: list[tuple[object, ...]] = []
    for row in rows:
        prepared.append(_build_row(row, column_mappings))
    return prepared


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _normalise_sql_type(type_text: str, *, default_nullability: str | None = None) -> str:
    text = " ".join(str(type_text).strip().upper().split())
    if not text:
        return text
    if default_nullability:
        has_nullability = " NULL" in text or " NOT NULL" in text
        if not has_nullability:
            text = f"{text} {default_nullability}"
    return text


def _fetch_existing_columns(connection, table: str) -> list[dict[str, object]]:
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW COLUMNS FROM {_quote_identifier(table)}")
        rows = cursor.fetchall()

    columns: list[dict[str, object]] = []
    for row in rows:
        if isinstance(row, Mapping):
            name = row.get("Field")
            col_type = row.get("Type")
            nullable = str(row.get("Null", "")).upper() == "YES"
        else:
            name = row[0]
            col_type = row[1]
            nullable = str(row[2]).upper() == "YES"
        columns.append({"name": name, "type": col_type, "is_nullable": nullable})
    return columns


def _normalized_metadata_column_definitions() -> "OrderedDict[str, str]":
    defaults = _default_metadata_column_definitions()
    definitions: "OrderedDict[str, str]" = OrderedDict()

    id_definition = defaults.get("id")
    if id_definition:
        definitions["id"] = id_definition

    raw_id_definition = id_definition or "BIGINT UNSIGNED NOT NULL"
    for phrase in ("AUTO_INCREMENT", "PRIMARY KEY"):
        raw_id_definition = raw_id_definition.replace(phrase, "")
    raw_id_definition = " ".join(raw_id_definition.split())
    definitions["raw_id"] = raw_id_definition if raw_id_definition else "BIGINT UNSIGNED NOT NULL"

    for column in _METADATA_COLUMNS:
        if column == "raw_id":
            continue
        default = defaults.get(column)
        if default:
            definitions[column] = default

    return definitions


def _resolve_normalized_column_type(
    column: str, column_types: Mapping[str, str] | None
) -> str:
    if column_types:
        override = column_types.get(column)
        if override is not None:
            override = str(override).strip()
            if override:
                return override
    override = _COLUMN_TYPE_OVERRIDES.get(column)
    if override:
        return override
    return "VARCHAR(255) NULL"


def _build_create_table_sql(
    table: str,
    *,
    column_mappings: Mapping[str, str],
    column_types: Mapping[str, str],
) -> str:
    metadata_definitions = _normalized_metadata_column_definitions()
    added: set[str] = set()
    column_sql: list[str] = []

    def append_column(name: str, type_sql: str) -> None:
        if name in added:
            return
        column_sql.append(f"{_quote_identifier(name)} {type_sql}")
        added.add(name)

    for name, type_sql in metadata_definitions.items():
        append_column(name, type_sql)

    for name in column_mappings:
        if name in added or name in _METADATA_COLUMNS:
            continue
        append_column(name, _resolve_normalized_column_type(name, column_types))

    columns_joined = ",\n  ".join(column_sql)
    return (
        f"CREATE TABLE {_quote_identifier(table)} (\n  {columns_joined}\n) "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )


def _create_normalized_table(
    connection,
    table: str,
    *,
    column_mappings: Mapping[str, str],
    column_types: Mapping[str, str],
) -> bool:
    create_sql = _build_create_table_sql(
        table, column_mappings=column_mappings, column_types=column_types
    )
    with connection.cursor() as cursor:
        cursor.execute(create_sql)
    return True


def _is_table_missing_error(exc: Exception) -> bool:
    if isinstance(exc, TableMissingError):
        return True

    errno = getattr(exc, "errno", None)
    if errno == 1146:
        return True

    args = getattr(exc, "args", ())
    if args:
        first = args[0]
        if isinstance(first, int) and first == 1146:
            return True
        if isinstance(first, str):
            try:
                if int(first) == 1146:
                    return True
            except ValueError:
                pass

    message = str(exc).lower()
    return "does not exist" in message and "table" in message


def ensure_normalized_schema(
    connection,
    table: str,
    column_mappings: Mapping[str, str],
    column_types: Mapping[str, str] | None = None,
) -> bool:
    """Ensure the normalized table contains columns for every mapping key."""

    if not column_mappings:
        return False

    try:
        existing_columns = {
            column["name"]: column for column in _fetch_existing_columns(connection, table)
        }
    except Exception as exc:  # pragma: no cover - thin wrapper around DB driver
        if _is_table_missing_error(exc):
            return _create_normalized_table(
                connection,
                table,
                column_mappings=column_mappings,
                column_types=column_types or {},
            )
        raise
    additions: list[tuple[str, str]] = []
    modifications: list[tuple[str, str]] = []
    for column in column_mappings:
        if column in _METADATA_COLUMNS:
            continue
        override_type = None
        if column_types:
            override_type = column_types.get(column)
            if override_type is not None:
                override_type = str(override_type).strip()
        if not override_type:
            override_type = _COLUMN_TYPE_OVERRIDES.get(column)
        if not override_type:
            override_type = "VARCHAR(255) NULL"

        existing = existing_columns.get(column)
        if existing is None:
            additions.append((column, override_type))
            continue

        actual_type = _normalise_sql_type(
            existing.get("type"),
            default_nullability="NULL"
            if existing.get("is_nullable", True)
            else "NOT NULL",
        )
        desired_type = _normalise_sql_type(override_type, default_nullability="NULL")
        if actual_type != desired_type:
            modifications.append((column, override_type))

    if not additions and not modifications:
        return False

    with connection.cursor() as cursor:
        for column, column_type in additions:
            cursor.execute(
                f"ALTER TABLE {_quote_identifier(table)} "
                f"ADD COLUMN {_quote_identifier(column)} {column_type}"
            )
        for column, column_type in modifications:
            cursor.execute(
                f"ALTER TABLE {_quote_identifier(table)} "
                f"MODIFY COLUMN {_quote_identifier(column)} {column_type}"
            )
    return True


def insert_normalized_rows(
    connection,
    table: str,
    rows: Sequence[Mapping[str, object]],
    column_mappings: Mapping[str, str] | None = None,
) -> int:
    if not rows:
        return 0
    resolved_mappings = resolve_column_mappings(rows, column_mappings)
    sql, _ = build_insert_statement(table, resolved_mappings)
    prepared = prepare_rows(rows, resolved_mappings)
    with connection.cursor() as cursor:
        cursor.executemany(sql, prepared)
        if getattr(cursor, "rowcount", None) not in (None, -1):
            return cursor.rowcount
    return len(prepared)


def mark_staging_rows_processed(
    connection,
    staging_table: str,
    row_ids: Sequence[int],
    *,
    file_hash: str,
    processed_at: _dt.datetime | None = None,
) -> _dt.datetime | None:
    """Mark staging rows as processed for the given file hash.

    The previous implementation built an ``IN`` clause for every processed row
    and passed each identifier as an update parameter. This version reuses the
    ``file_hash`` that scoped the batch fetch, letting the database locate the
    relevant rows directly.
    """

    if not row_ids:
        return None

    if processed_at is None:
        processed_at = _dt.datetime.now(_dt.timezone.utc)

    sql = (
        f"UPDATE `{staging_table}` "
        "SET processed_at = %s "
        "WHERE file_hash = %s "
        "AND (processed_at IS NULL OR processed_at = '0000-00-00 00:00:00')"
    )
    params = (processed_at, file_hash)

    with connection.cursor() as cursor:
        cursor.execute(sql, params)

    return processed_at


__all__ = [
    "ensure_normalized_schema",
    "resolve_column_mappings",
    "TableConfig",
    "build_insert_statement",
    "insert_normalized_rows",
    "mark_staging_rows_processed",
    "prepare_rows",
]
