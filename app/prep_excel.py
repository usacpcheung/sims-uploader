import os
import re
import sys
import hashlib

from typing import Iterable, Sequence

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

# Columns that must be present in the spreadsheet after header normalization.
# The order matches the teaching record export shared by the school and is a
# subset of the staging table schema to allow future optional columns.
REQUIRED_HEADERS: Sequence[str] = (
    "日期",
    "任教老師",
    "學生編號",
    "姓名",
    "教授科目",
)


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


def get_table_order():
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW COLUMNS FROM teach_record_raw")
            cols = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    # Exclude metadata columns; we will set them via SET in LOAD DATA
    for meta in ("id", "file_hash", "batch_id", "source_year", "ingested_at"):
        if meta in cols:
            cols.remove(meta)
    return cols


def main(xlsx_path, sheet="TEACH_RECORD"):
    df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str)
    df = normalize_headers_and_subject(df)

    missing = validate_required_columns(df, REQUIRED_HEADERS)
    if missing:
        raise MissingColumnsError(missing)

    # Reorder to match table order; add missing columns as empty while preserving
    # any additional headers by appending them after the schema-aligned block.
    order = get_table_order()
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
    sheet = sys.argv[2] if len(sys.argv) > 2 else "TEACH_RECORD"
    try:
        main(xlsx, sheet)
    except MissingColumnsError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
