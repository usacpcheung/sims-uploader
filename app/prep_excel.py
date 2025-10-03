import json
import os
import re
import sys
import hashlib
import warnings

from functools import lru_cache
from typing import Iterable, Mapping, Sequence

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


def _series_has_data(series: pd.Series) -> bool:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace({
        "nan": "",
        "None": "",
        "NONE": "",
        "<NA>": "",
        "NaT": "",
    })
    return cleaned.ne("").any()


def normalize_headers_and_subject(
    df: pd.DataFrame, *, rename_last_subject: bool = True
) -> pd.DataFrame:
    df.columns = [("" if pd.isna(c) else str(c)).strip() for c in df.columns]

    keep_mask: list[bool] = []
    for i, column_name in enumerate(df.columns):
        if column_name == "" and not _series_has_data(df.iloc[:, i]):
            keep_mask.append(False)
        else:
            keep_mask.append(True)

    if not all(keep_mask):
        df = df.loc[:, keep_mask]

    if not rename_last_subject:
        return df

    def is_unnamed(c: str) -> bool:
        return c == "" or UNNAMED_PAT.match(c) is not None

    flags = [is_unnamed(c) for c in df.columns]
    last_idx = None
    for i in range(len(df.columns) - 1, -1, -1):
        if flags[i]:
            if _series_has_data(df.iloc[:, i]):
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
            if not _series_has_data(df.iloc[:, i]):
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


def _normalise_db_settings(db_settings: Mapping[str, object] | None) -> dict[str, object]:
    if db_settings is None:
        return dict(DB)
    return dict(db_settings)


def _freeze_db_settings(settings: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    def _freeze(value):
        if isinstance(value, Mapping):
            return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
        if isinstance(value, (list, tuple, set)):
            return tuple(_freeze(v) for v in value)
        return value

    return tuple(sorted((key, _freeze(val)) for key, val in settings.items()))


def _parse_sheet_config_rows(rows: Sequence[Mapping[str, object]]) -> dict[str, dict[str, object]]:
    config: dict[str, dict[str, object]] = {}
    for row in rows:
        metadata_columns = _loads_json(row.get("metadata_columns")) or []
        required_columns = _loads_json(row.get("required_columns")) or []
        options = _loads_json(row.get("options")) or {}
        column_mappings = _loads_json(row.get("column_mappings"))
        normalized_table = row.get("normalized_table")
        if normalized_table is None and isinstance(options, Mapping):
            normalized_table = options.get("normalized_table")
        config[row["sheet_name"]] = {
            "table": row["staging_table"],
            "metadata_columns": frozenset(metadata_columns),
            "required_columns": frozenset(required_columns),
            "options": options,
            "column_mappings": column_mappings,
            "normalized_table": normalized_table,
        }
    return config


def _load_sheet_config(connection) -> dict[str, dict[str, object]]:
    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        try:
            cur.execute(
                f"""
                SELECT sheet_name,
                       staging_table,
                       metadata_columns,
                       required_columns,
                       column_mappings,
                       options
                  FROM {CONFIG_TABLE}
                """
            )
        except pymysql.err.MySQLError as exc:
            error_code = exc.args[0] if exc.args else None
            if error_code != 1054:
                raise
            cur.execute(
                f"""
                SELECT sheet_name,
                       staging_table,
                       metadata_columns,
                       required_columns,
                       options
                  FROM {CONFIG_TABLE}
                """
            )
            rows = cur.fetchall()
            for row in rows:
                row.setdefault("column_mappings", None)
        else:
            rows = cur.fetchall()
    return _parse_sheet_config_rows(rows)


@lru_cache(maxsize=8)
def _get_sheet_config_cached(settings_items: tuple[tuple[str, object], ...]):
    settings = dict(settings_items)
    conn = pymysql.connect(**settings)
    try:
        return _load_sheet_config(conn)
    finally:
        conn.close()


def _get_sheet_config(
    *, connection=None, db_settings: Mapping[str, object] | None = None
) -> dict[str, dict[str, object]]:
    if connection is not None:
        return _load_sheet_config(connection)

    settings = _normalise_db_settings(db_settings)
    settings_key = _freeze_db_settings(settings)
    return _get_sheet_config_cached(settings_key)


_get_sheet_config.cache_clear = _get_sheet_config_cached.cache_clear  # type: ignore[attr-defined]
_get_sheet_config.cache_info = _get_sheet_config_cached.cache_info  # type: ignore[attr-defined]


def _get_table_config(
    sheet: str, *, connection=None, db_settings: Mapping[str, object] | None = None
):
    config = _get_sheet_config(connection=connection, db_settings=db_settings)
    try:
        return config[sheet]
    except KeyError as exc:
        raise ValueError(f"Unsupported sheet name: {sheet!r}") from exc
def _fetch_table_columns(
    table_name: str, *, connection=None, db_settings: Mapping[str, object] | None = None
) -> list[dict[str, object]]:
    owns_connection = connection is None
    settings = None
    if owns_connection:
        settings = _normalise_db_settings(db_settings)
        connection = pymysql.connect(**settings)
    db_settings_for_query = (
        settings
        if settings is not None
        else (_normalise_db_settings(db_settings) if db_settings is not None else dict(DB))
    )
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              ORDER BY ORDINAL_POSITION
                """,
                (db_settings_for_query["database"], table_name),
            )
            rows = cur.fetchall()
    finally:
        if owns_connection:
            connection.close()

    return [
        {
            "name": row["COLUMN_NAME"],
            "is_nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            "default": row["COLUMN_DEFAULT"],
        }
        for row in rows
    ]


def get_schema_details(
    sheet: str = DEFAULT_SHEET,
    *,
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> dict[str, list[str]]:
    config = _get_table_config(sheet, connection=connection, db_settings=db_settings)
    metadata_columns = set(config.get("metadata_columns", ()))
    required_columns_config = set(config.get("required_columns", ()))
    columns = _fetch_table_columns(
        config["table"], connection=connection, db_settings=db_settings
    )

    ordered_columns = [
        column["name"] for column in columns if column["name"] not in metadata_columns
    ]
    if required_columns_config:
        required_columns = [
            column["name"]
            for column in columns
            if column["name"] in required_columns_config
        ]
    else:
        required_columns = [
            column["name"]
            for column in columns
            if column["name"] not in metadata_columns
        ]
    return {"order": ordered_columns, "required": required_columns}


def get_table_order(
    sheet: str = DEFAULT_SHEET,
    *,
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> list[str]:
    schema = get_schema_details(
        sheet, connection=connection, db_settings=db_settings
    )
    return schema["order"]


def _derive_csv_output_path(xlsx_path: str, file_hash: str) -> str:
    base, _ = os.path.splitext(xlsx_path)
    return f"{base}.{file_hash}.csv"


def _staging_file_hash_exists(
    table_name: str,
    file_hash: str,
    *,
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> bool:
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unsafe table name: {table_name!r}")
    owns_connection = connection is None
    if owns_connection:
        settings = _normalise_db_settings(db_settings)
        connection = pymysql.connect(**settings)
    try:
        with connection.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM `{table_name}` WHERE file_hash = %s LIMIT 1",
                (file_hash,),
            )
            return cur.fetchone() is not None
    finally:
        if owns_connection:
            connection.close()


def main(
    xlsx_path,
    sheet=DEFAULT_SHEET,
    *,
    emit_stdout: bool = True,
    connection=None,
    db_settings: Mapping[str, object] | None = None,
):
    """Preprocess an Excel worksheet into a CSV aligned with the staging schema.

    Parameters
    ----------
    xlsx_path:
        Path to the workbook on disk.
    sheet:
        Name of the worksheet to ingest. Defaults to ``TEACH_RECORD``.
    emit_stdout:
        When ``True`` (the default), status messages are printed to stdout/stderr
        to preserve the current CLI behaviour. UI callers can set this to
        ``False`` to suppress printing while still receiving the return values.
    connection:
        Optional existing database connection to reuse for configuration lookups
        and duplicate-hash checks. The caller remains responsible for its
        lifecycle.
    db_settings:
        Override the default database settings when a connection is not
        supplied. Accepts any mapping supported by :func:`pymysql.connect`.

    Returns
    -------
    tuple[str | None, str]
        A tuple of ``(csv_path, file_hash)``. ``csv_path`` is ``None`` when the
        file has already been processed (duplicate hash).
    """

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning,
            module="openpyxl.styles.stylesheet",
        )
        df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str)

    config = _get_table_config(
        sheet, connection=connection, db_settings=db_settings
    )
    options = config.get("options") or {}
    rename_last_subject = bool(options.get("rename_last_subject"))
    df = normalize_headers_and_subject(df, rename_last_subject=rename_last_subject)

    schema = get_schema_details(
        sheet, connection=connection, db_settings=db_settings
    )
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

    with open(xlsx_path, "rb") as source:
        file_hash = hashlib.sha256(source.read()).hexdigest()

    table_name = config["table"]
    if _staging_file_hash_exists(
        table_name,
        file_hash,
        connection=connection,
        db_settings=db_settings,
    ):
        if emit_stdout:
            print(
                f"File hash {file_hash} already exists in staging table {table_name}; skipping.",
                file=sys.stderr,
            )
        return None, file_hash

    csv_path = _derive_csv_output_path(xlsx_path, file_hash)
    df.to_csv(csv_path, index=False)

    if emit_stdout:
        print(csv_path)
        print(file_hash)
    return csv_path, file_hash


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
