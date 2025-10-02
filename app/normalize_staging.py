"""Normalize staging-table rows into analytics-friendly tables."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Mapping, Sequence

# Metadata fields that should always be preserved for downstream joins.
_METADATA_COLUMNS = ["raw_id", "file_hash", "batch_id", "source_year", "ingested_at"]

# Ordered list of business columns mirrored from the teaching-record spreadsheet.
BUSINESS_COLUMNS: Sequence[str] = (
    "記錄狀態",
    "日期",
    "任教老師",
    "學生編號",
    "姓名",
    "英文姓名",
    "性別",
    "學生級別",
    "病房",
    "病床",
    "出勤 (來自出勤記錄輸入)",
    "出勤",
    "教學組別",
    "科目",
    "取代科目",
    "教授科目",
    "課程級別",
    "教材",
    "課題",
    "教學重點1",
    "教學重點2",
    "教學重點3",
    "教學重點4",
    "自定課題",
    "自定教學重點",
    "練習",
    "上課時數",
    "備註",
    "教學跟進/回饋",
)


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


def _build_ordered_columns(column_mappings: Mapping[str, str]) -> list[str]:
    ordered = list(_METADATA_COLUMNS)
    for column in BUSINESS_COLUMNS:
        if column in column_mappings:
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


def build_insert_statement(table: str, column_mappings: Mapping[str, str]) -> tuple[str, list[str]]:
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


def insert_normalized_rows(
    connection,
    table: str,
    rows: Sequence[Mapping[str, object]],
    column_mappings: Mapping[str, str] | None = None,
) -> int:
    if not rows:
        return 0
    if column_mappings is None:
        column_mappings = {column: column for column in BUSINESS_COLUMNS}
    sql, _ = build_insert_statement(table, column_mappings)
    prepared = prepare_rows(rows, column_mappings)
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
        "WHERE file_hash = %s AND processed_at IS NULL"
    )
    params = (processed_at, file_hash)

    with connection.cursor() as cursor:
        cursor.execute(sql, params)

    return processed_at


__all__ = [
    "BUSINESS_COLUMNS",
    "TableConfig",
    "build_insert_statement",
    "insert_normalized_rows",
    "mark_staging_rows_processed",
    "prepare_rows",
]
