import json
import os
import re
import sys
import hashlib
import warnings
from collections import OrderedDict

from functools import lru_cache
from typing import Iterable, Mapping, Sequence

import pandas as pd
import pymysql

try:
    from .config import get_db_settings
    from .identifiers import sanitize_identifier
except ImportError:  # pragma: no cover - fallback when executed as a script
    # Ensure the repository root is on sys.path when running ``python app/prep_excel.py``.
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from app.config import get_db_settings
    from app.identifiers import sanitize_identifier

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


class TableMissingError(RuntimeError):
    """Raised when a staging table is missing from the database."""

    def __init__(self, table_name: str):
        super().__init__(f"Table does not exist: {table_name}")
        self.table_name = table_name


def _default_metadata_column_definitions() -> "OrderedDict[str, str]":
    """Return the default column definitions for staging table metadata."""

    return OrderedDict(
        (
            ("id", "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"),
            ("file_hash", "CHAR(64) NOT NULL"),
            ("batch_id", "CHAR(36) NULL"),
            ("source_year", "INT NULL"),
            ("ingested_at", "DATETIME NOT NULL"),
            ("processed_at", "DATETIME NULL DEFAULT NULL"),
        )
    )


def _resolve_column_type(
    column_name: str,
    *,
    column_types: Mapping[str, str],
    metadata_defaults: Mapping[str, str],
) -> str:
    override = column_types.get(column_name)
    if override is not None:
        override = str(override).strip()
        if override:
            return override
    return metadata_defaults.get(column_name, "VARCHAR(255) NULL")


def _build_create_table_statement(
    table_name: str,
    *,
    metadata_columns: Iterable[str],
    metadata_order: Iterable[str],
    required_columns: Iterable[str],
    column_types: Mapping[str, str],
) -> str:
    metadata_defaults = _default_metadata_column_definitions()
    metadata_columns_set = set(metadata_columns)

    if metadata_columns_set:
        ordered_metadata = [
            column
            for column in metadata_order
            if column in metadata_columns_set
        ]
        # Ensure default ordering for known metadata columns that were not
        # explicitly included in the configuration order.
        ordered_metadata.extend(
            column
            for column in metadata_defaults
            if column in metadata_columns_set and column not in ordered_metadata
        )
        ordered_metadata.extend(
            column
            for column in metadata_columns_set
            if column not in ordered_metadata
        )
    else:
        ordered_metadata = list(metadata_defaults.keys())

    required_order = list(required_columns)
    if not required_order:
        required_order = []

    added: set[str] = set()
    column_defs: list[str] = []

    def append_column(name: str) -> None:
        if name in added:
            return
        column_defs.append(
            f"{_quote_identifier(name)} "
            f"{_resolve_column_type(name, column_types=column_types, metadata_defaults=metadata_defaults)}"
        )
        added.add(name)

    for name in ordered_metadata:
        append_column(name)

    for name in required_order:
        append_column(name)

    for name in column_types:
        append_column(name)

    if not column_defs:
        append_column("id")

    columns_sql = ",\n  ".join(column_defs)
    return (
        f"CREATE TABLE {_quote_identifier(table_name)} (\n  {columns_sql}\n) "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )


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


def _normalise_configured_columns(values: Iterable[object]) -> list[str]:
    """Coerce configured column names into a unique, ordered list of strings."""

    cleaned: list[str] = []
    seen: set[str] = set()

    def _iter_values(items: Iterable[object]):
        for item in items:
            if isinstance(item, str):
                yield item
            elif isinstance(item, (list, tuple, set)):
                # Flatten one level of nested iterables that may appear in
                # configuration JSON. Any deeper nesting is unlikely and would
                # still be coerced via ``str`` below.
                for sub_item in item:
                    yield sub_item
            else:
                yield item

    for raw_value in _iter_values(values):
        text = str(raw_value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)

    return cleaned


def _parse_sheet_config_rows(
    rows: Sequence[Mapping[str, object]]
) -> dict[str, dict[str, dict[str, object]]]:
    config: dict[str, dict[str, dict[str, object]]] = {}
    normalized_sources: dict[tuple[str, str], str] = {}
    for row in rows:
        workbook_type = row.get("workbook_type")
        if isinstance(workbook_type, str):
            workbook_type = workbook_type.strip() or "default"
        else:
            workbook_type = "default"
        metadata_columns = _normalise_configured_columns(
            _loads_json(row.get("metadata_columns")) or []
        )
        required_columns = _normalise_configured_columns(
            _loads_json(row.get("required_columns")) or []
        )
        options = _loads_json(row.get("options")) or {}
        column_mappings = _loads_json(row.get("column_mappings"))
        normalized_table_source = "none"
        normalized_table = row.get("normalized_table")
        if isinstance(normalized_table, str):
            normalized_table = normalized_table.strip() or None
        if normalized_table is not None:
            normalized_table_source = "explicit"
        if normalized_table is None and isinstance(options, Mapping):
            option_value = options.get("normalized_table")
            if isinstance(option_value, str):
                option_value = option_value.strip() or None
            if option_value is not None:
                normalized_table = option_value
                normalized_table_source = "explicit"
        column_types: dict[str, str] = {}
        normalized_metadata_columns: tuple[str, ...] | None = None
        reserved_source_columns: frozenset[str] | None = None
        normalized_column_type_overrides: dict[str, str] | None = None
        if isinstance(options, Mapping):
            raw_column_types = options.get("column_types")
            if isinstance(raw_column_types, Mapping):
                for key, value in raw_column_types.items():
                    key_text = str(key).strip()
                    if not key_text:
                        continue
                    if value is None:
                        continue
                    type_text = str(value).strip()
                    if not type_text:
                        continue
                    column_types[key_text] = type_text

            raw_metadata_columns = options.get("normalized_metadata_columns")
            if isinstance(raw_metadata_columns, (list, tuple, set)):
                cleaned_metadata: list[str] = []
                seen_metadata: set[str] = set()
                for value in raw_metadata_columns:
                    text = str(value).strip()
                    if not text or text in seen_metadata:
                        continue
                    seen_metadata.add(text)
                    cleaned_metadata.append(text)
                if cleaned_metadata:
                    normalized_metadata_columns = tuple(cleaned_metadata)

            raw_reserved_columns = options.get("reserved_source_columns")
            if isinstance(raw_reserved_columns, (list, tuple, set)):
                cleaned_reserved: list[str] = []
                seen_reserved: set[str] = set()
                for value in raw_reserved_columns:
                    text = str(value).strip()
                    if not text or text in seen_reserved:
                        continue
                    seen_reserved.add(text)
                    cleaned_reserved.append(text)
                if cleaned_reserved:
                    reserved_source_columns = frozenset(cleaned_reserved)

            raw_normalized_overrides = options.get(
                "normalized_column_type_overrides"
            )
            if isinstance(raw_normalized_overrides, Mapping):
                cleaned_overrides: dict[str, str] = {}
                for key, value in raw_normalized_overrides.items():
                    key_text = str(key).strip()
                    if not key_text:
                        continue
                    if value is None:
                        continue
                    type_text = str(value).strip()
                    if not type_text:
                        continue
                    cleaned_overrides[key_text] = type_text
                if cleaned_overrides:
                    normalized_column_type_overrides = cleaned_overrides
        if normalized_table is None:
            staging_table = row.get("staging_table")
            if isinstance(staging_table, str):
                base_table = staging_table.strip()
                if base_table:
                    if base_table.endswith("_raw"):
                        base_table = base_table[: -len("_raw")]
                    normalized_table = f"{base_table}_normalized"
                    normalized_table_source = "derived"
        sheet_name = row["sheet_name"]
        workbook_config = config.setdefault(workbook_type, {})
        key = (workbook_type, sheet_name)
        existing = workbook_config.get(sheet_name)
        if existing is not None:
            existing_source = normalized_sources.get(key, "none")
            if existing_source == "explicit" and normalized_table_source != "explicit":
                # Preserve workbook-specific configuration that already defines the
                # normalization target when a more generic row lacks it.
                continue
        workbook_config[sheet_name] = {
            "table": row["staging_table"],
            "metadata_columns": frozenset(metadata_columns),
            "metadata_column_order": tuple(metadata_columns),
            "required_columns": frozenset(required_columns),
            "required_column_order": tuple(required_columns),
            "options": options,
            "column_mappings": column_mappings,
            "normalized_table": normalized_table,
            "column_types": column_types,
            "normalized_metadata_columns": normalized_metadata_columns,
            "reserved_source_columns": reserved_source_columns,
            "normalized_column_type_overrides": normalized_column_type_overrides,
        }
        normalized_sources[key] = normalized_table_source
    return config


def _load_sheet_config(connection) -> dict[str, dict[str, dict[str, object]]]:
    with connection.cursor(pymysql.cursors.DictCursor) as cur:
        try:
            cur.execute(
                f"""
                SELECT workbook_type,
                       sheet_name,
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
                row["workbook_type"] = "default"
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
) -> dict[str, dict[str, dict[str, object]]]:
    if connection is not None:
        return _load_sheet_config(connection)

    settings = _normalise_db_settings(db_settings)
    settings_key = _freeze_db_settings(settings)
    return _get_sheet_config_cached(settings_key)


_get_sheet_config.cache_clear = _get_sheet_config_cached.cache_clear  # type: ignore[attr-defined]
_get_sheet_config.cache_info = _get_sheet_config_cached.cache_info  # type: ignore[attr-defined]


def _get_table_config(
    sheet: str,
    *,
    workbook_type: str = "default",
    connection=None,
    db_settings: Mapping[str, object] | None = None,
):
    config_by_type = _get_sheet_config(connection=connection, db_settings=db_settings)
    workbook_config = config_by_type.get(workbook_type)
    if workbook_config is not None and sheet in workbook_config:
        return workbook_config[sheet]

    default_config = config_by_type.get("default", {})
    if workbook_type != "default" and sheet in default_config:
        return default_config[sheet]

    if workbook_config is None and workbook_type != "default":
        raise ValueError(f"Unsupported workbook type: {workbook_type!r}")

    raise ValueError(f"Unsupported sheet name: {sheet!r}")
def _normalise_sql_type(type_text: str, *, default_nullability: str | None = None) -> str:
    text = " ".join(str(type_text).strip().upper().split())
    if not text:
        return text
    if default_nullability:
        has_nullability = " NULL" in text or " NOT NULL" in text
        if not has_nullability:
            text = f"{text} {default_nullability}"
    return text


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
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              ORDER BY ORDINAL_POSITION
                """,
                (db_settings_for_query["database"], table_name),
            )
            rows = cur.fetchall()
    except pymysql.err.ProgrammingError as exc:  # type: ignore[attr-defined]
        if exc.args and exc.args[0] == 1146:
            raise TableMissingError(table_name) from exc
        raise
    finally:
        if owns_connection:
            connection.close()

    return [
        {
            "name": row["COLUMN_NAME"],
            "is_nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            "default": row["COLUMN_DEFAULT"],
            "type": row["COLUMN_TYPE"],
        }
        for row in rows
    ]


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_staging_columns(
    *,
    headers: Sequence[str],
    config: Mapping[str, object],
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> bool:
    table_name = config["table"]
    metadata_columns = set(config.get("metadata_columns", ()))
    metadata_column_order = tuple(config.get("metadata_column_order", ()))
    column_types: Mapping[str, str] = config.get("column_types") or {}
    required_column_order = tuple(config.get("required_column_order", ()))
    if not required_column_order and config.get("required_columns"):
        required_column_order = tuple(sorted(config.get("required_columns", ())))

    metadata_defaults = _default_metadata_column_definitions()

    owns_connection = connection is None
    if owns_connection:
        settings = _normalise_db_settings(db_settings)
        connection = pymysql.connect(**settings)

    schema_changed = False

    try:
        try:
            columns = _fetch_table_columns(
                table_name, connection=connection, db_settings=db_settings
            )
        except TableMissingError:
            create_sql = _build_create_table_statement(
                table_name,
                metadata_columns=metadata_columns,
                metadata_order=metadata_column_order,
                required_columns=required_column_order,
                column_types=column_types,
            )
            with connection.cursor() as cursor:
                cursor.execute(create_sql)
            connection.commit()
            schema_changed = True
            columns = _fetch_table_columns(
                table_name, connection=connection, db_settings=db_settings
            )

        column_details = {column["name"]: column for column in columns}

        missing_columns: list[tuple[str, str]] = []
        modify_columns: list[tuple[str, str]] = []
        metadata_targets = (
            metadata_columns if metadata_columns else set(metadata_defaults.keys())
        )

        for column_name in metadata_targets:
            if column_name in column_details:
                continue
            column_type_sql = _resolve_column_type(
                column_name,
                column_types=column_types,
                metadata_defaults=metadata_defaults,
            )
            missing_columns.append((column_name, column_type_sql))

        seen: set[str] = set()
        for header in headers:
            if header in seen:
                continue
            seen.add(header)
            if not header:
                continue
            if header in metadata_columns:
                continue
            column_type_override = column_types.get(header)
            if column_type_override is not None:
                column_type_override = str(column_type_override).strip()
            if header in column_details:
                if column_type_override:
                    existing = column_details[header]
                    actual = _normalise_sql_type(
                        existing.get("type"),
                        default_nullability="NULL"
                        if existing.get("is_nullable", True)
                        else "NOT NULL",
                    )
                    desired = _normalise_sql_type(
                        column_type_override, default_nullability="NULL"
                    )
                    if actual != desired:
                        modify_columns.append((header, column_type_override))
                continue
            column_type_sql = column_type_override or "VARCHAR(255) NULL"
            missing_columns.append((header, column_type_sql))

        if not missing_columns and not modify_columns:
            return schema_changed

        with connection.cursor() as cursor:
            for column, column_type in missing_columns:
                cursor.execute(
                    f"ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN {_quote_identifier(column)} {column_type}"
                )
            for column, column_type in modify_columns:
                cursor.execute(
                    f"ALTER TABLE {_quote_identifier(table_name)} MODIFY COLUMN {_quote_identifier(column)} {column_type}"
                )
        connection.commit()
        return True
    except TableMissingError:
        raise
    finally:
        if owns_connection and connection is not None:
            connection.close()


def get_schema_details(
    sheet: str = DEFAULT_SHEET,
    *,
    workbook_type: str = "default",
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> dict[str, list[str]]:
    config = _get_table_config(
        sheet,
        workbook_type=workbook_type,
        connection=connection,
        db_settings=db_settings,
    )
    metadata_columns = set(config.get("metadata_columns", ()))
    required_columns_config = set(config.get("required_columns", ()))
    try:
        columns = _fetch_table_columns(
            config["table"], connection=connection, db_settings=db_settings
        )
    except TableMissingError as exc:
        raise RuntimeError(
            f"Staging table {config['table']!r} is missing; run prep_excel.main first."
        ) from exc

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
    workbook_type: str = "default",
    connection=None,
    db_settings: Mapping[str, object] | None = None,
) -> list[str]:
    schema = get_schema_details(
        sheet,
        workbook_type=workbook_type,
        connection=connection,
        db_settings=db_settings,
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
    workbook_type: str = "default",
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
    workbook_type:
        Configuration grouping used to select workbook-specific overrides.
        Defaults to ``"default"``.
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
        sheet,
        workbook_type=workbook_type,
        connection=connection,
        db_settings=db_settings,
    )
    options = config.get("options") or {}
    rename_last_subject = bool(options.get("rename_last_subject"))
    df = normalize_headers_and_subject(df, rename_last_subject=rename_last_subject)

    schema_changed = _ensure_staging_columns(
        headers=list(df.columns),
        config=config,
        connection=connection,
        db_settings=db_settings,
    )
    if schema_changed:
        _get_sheet_config.cache_clear()

    schema = get_schema_details(
        sheet,
        workbook_type=workbook_type,
        connection=connection,
        db_settings=db_settings,
    )
    missing = validate_required_columns(df, schema["required"])
    if missing:
        raise MissingColumnsError(missing)

    # Reorder to match table order; add missing columns as empty while preserving
    # any additional headers by appending them after the schema-aligned block.
    order = schema["order"]
    extra_columns = [c for c in df.columns if c not in order]
    if extra_columns:
        existing_identifiers = set(order)
        sanitized_mapping: dict[str, str] = {}
        for column in extra_columns:
            sanitized = sanitize_identifier(column, existing=existing_identifiers)
            sanitized_mapping[column] = sanitized
        df = df.rename(columns=sanitized_mapping)
        extra_columns = [sanitized_mapping[column] for column in extra_columns]
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
