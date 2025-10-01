import json
import os
import re
import sys
import hashlib

from functools import lru_cache
from typing import Iterable

import pandas as pd
import pymysql

try:
    from .config import get_db_settings
except ImportError:  # pragma: no cover - fallback when executed as a script
    # Ensure the repository root is on sys.path when running ``python app/prep_excel.py``.
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from app.config import get_db_settings

DB = get_db_settings()

UNNAMED_PAT = re.compile(r"^Unnamed(?::\s*\d+)?$", re.IGNORECASE)

DEFAULT_SHEET = "TEACH_RECORD"

CONFIG_TABLE = "sheet_ingest_config"


class MissingColumnsError(RuntimeError):
    """Raised when required spreadsheet columns are missing after normalization."""

    def __init__(self, missing_columns: Iterable[str]):
        self.missing_columns = tuple(missing_columns)
        if self.missing_columns:
            message = "Missing required column(s): " + ", ".join(self.missing_columns)
        else:
            message = "Missing required column(s)."
        super().__init__(message)


def normalize_headers_and_subject(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(axis=1, how="all")
    df.columns = [("" if pd.isna(c) else str(c)).strip() for c in df.columns]

    def is_unnamed(c: str) -> bool:
        return c == "" or UNNAMED_PAT.match(c) is not None

    flags = [is_unnamed(c) for c in df.columns]
    last_idx = None
    for i in range(len(df.columns) - 1, -1, -1):
        if flags[i]:
            ser = df.iloc[:, i].astype(str).str.strip().replace({"nan": ""})
            if ser.ne("").any():
                last_idx = i
                break
    if last_idx is not None:
        cols = list(df.columns)
        cols[last_idx] = "教授科目"
        df.columns = cols
    # drop any other unnamed columns that are fully empty
    drop = []
    for i, f in enumerate(flags):
        if i == last_idx:
            continue
        if f:
            ser = df.iloc[:, i].astype(str).str.strip().replace({"nan": ""})
            if not ser.ne("").any():
                drop.append(df.columns[i])
    if drop:
        df = df.drop(columns=drop)
    return df


def validate_required_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    required_list = list(required)
    missing = [column for column in required_list if column not in df.columns]
    return missing


def _loads_json(value):
    if value in (None, ""):
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
        return json.loads(value)
    return value


@lru_cache(maxsize=1)
def _get_sheet_config() -> dict[str, dict[str, object]]:
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                f"""
                SELECT sheet_name, staging_table, metadata_columns, options
                  FROM {CONFIG_TABLE}
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    config: dict[str, dict[str, object]] = {}
    for row in rows:
        metadata_columns = _loads_json(row.get("metadata_columns")) or []
        options = _loads_json(row.get("options")) or {}
        config[row["sheet_name"]] = {
            "table": row["staging_table"],
            "metadata_columns": frozenset(metadata_columns),
            "options": options,
        }
    return config


def _get_table_config(sheet: str):
    config = _get_sheet_config()
    try:
        return config[sheet]
    except KeyError as exc:
        raise ValueError(f"Unsupported sheet name: {sheet!r}") from exc


def _fetch_table_columns(table_name: str) -> list[dict[str, object]]:
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              ORDER BY ORDINAL_POSITION
                """,
                (DB["database"], table_name),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "name": row["COLUMN_NAME"],
            "is_nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            "default": row["COLUMN_DEFAULT"],
        }
        for row in rows
    ]


def get_schema_details(sheet: str = DEFAULT_SHEET) -> dict[str, list[str]]:
    config = _get_table_config(sheet)
    metadata_columns = set(config.get("metadata_columns", ()))
    columns = _fetch_table_columns(config["table"])

    ordered_columns = [
        column["name"] for column in columns if column["name"] not in metadata_columns
    ]
    required_columns = [
        column["name"]
        for column in columns
        if column["name"] not in metadata_columns
        and not column["is_nullable"]
        and column["default"] is None
    ]
    return {"order": ordered_columns, "required": required_columns}


def get_table_order(sheet: str = DEFAULT_SHEET) -> list[str]:
    schema = get_schema_details(sheet)
    return schema["order"]


def main(xlsx_path, sheet=DEFAULT_SHEET):
    df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str)
    df = normalize_headers_and_subject(df)

    schema = get_schema_details(sheet)
    missing = validate_required_columns(df, schema["required"])
    if missing:
        raise MissingColumnsError(missing)

    # Reorder to match table order; add missing columns as empty while preserving
    # any additional headers by appending them after the schema-aligned block.
    order = schema["order"]
    extra_columns = [c for c in df.columns if c not in order]
    for c in order:
        if c not in df.columns:
            df[c] = None
    final_columns = order + extra_columns
    df = df.reindex(columns=final_columns)

    # write CSV next to the xlsx
    csv_path = os.path.splitext(xlsx_path)[0] + ".csv"
    df.to_csv(csv_path, index=False)

    # metadata
    with open(xlsx_path, "rb") as source:
        file_hash = hashlib.sha256(source.read()).hexdigest()
    print(csv_path)
    print(file_hash)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: prep_excel.py /path/to/file.xlsx [SheetName]", file=sys.stderr)
        sys.exit(1)
    xlsx = sys.argv[1]
    sheet = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SHEET
    try:
        main(xlsx, sheet)
    except MissingColumnsError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
