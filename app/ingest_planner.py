"""CLI helper to summarize Excel headers for ingest configuration planning.

Usage examples:

    python app/ingest_planner.py uploads/sample.xlsx
    python app/ingest_planner.py uploads/sample.xlsx --sheets Sheet1 Sheet2
    python app/ingest_planner.py uploads/sample.xlsx --output uploads/plan.json

The script reads the provided workbook without modifying it, identifies the first
non-empty header row per sheet, and writes a JSON summary containing the sheet
name, cleaned header values, suggested snake_case staging column names, and
inferred column types. With ``--emit-sql`` it also writes a reviewable
``sheet_ingest_config`` INSERT script that mirrors the JSON payload so operators
can paste entries directly into the configuration table.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

DEFAULT_OUTPUT_SUFFIX = "_ingest_plan.json"
DEFAULT_STAGING_TABLE_TEMPLATE = "{workbook}_{sheet}_raw"
DEFAULT_NORMALIZED_TABLE_TEMPLATE = "{workbook}_{sheet}"
DEFAULT_SAMPLE_ROWS = 50
METADATA_COLUMNS = [
    "id",
    "file_hash",
    "batch_id",
    "source_year",
    "ingested_at",
    "processed_at",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize workbook headers to prefill sheet_ingest_config entries."
        )
    )
    parser.add_argument(
        "workbook",
        type=Path,
        help="Path to the XLSX workbook to analyze (e.g., uploads/sample.xlsx).",
    )
    parser.add_argument(
        "--sheets",
        nargs="*",
        help=(
            "Optional list of sheet names to include. If omitted, all sheets are"
            " analyzed."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help=(
            "Where to write the JSON summary. Defaults to <workbook>_ingest_plan.json"
            " next to the input file."
        ),
    )
    parser.add_argument(
        "--staging-table-template",
        default=DEFAULT_STAGING_TABLE_TEMPLATE,
        help=(
            "Pattern for suggested staging table names. Use {workbook} and {sheet}"
            " placeholders; defaults to '{workbook}_{sheet}_raw'."
        ),
    )
    parser.add_argument(
        "--normalized-table-template",
        default=DEFAULT_NORMALIZED_TABLE_TEMPLATE,
        help=(
            "Pattern for suggested normalized table names stored in options. Use"
            " {workbook} and {sheet} placeholders; defaults to"
            " '{workbook}_{sheet}'."
        ),
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=DEFAULT_SAMPLE_ROWS,
        help=(
            "Number of data rows to scan for column-type inference. Defaults to"
            f" {DEFAULT_SAMPLE_ROWS}."
        ),
    )
    parser.add_argument(
        "--column-type-override",
        action="append",
        default=[],
        metavar="COL:TYPE",
        help=(
            "Override inferred column types (e.g., --column-type-override "
            "出生日期:'DATE NULL'). Can be provided multiple times."
        ),
    )
    parser.add_argument(
        "--workbook-type",
        default="default",
        help="Workbook type to use when emitting SQL inserts (defaults to 'default').",
    )
    parser.add_argument(
        "--emit-sql",
        action="store_true",
        help=(
            "Also write a sheet_ingest_config INSERT snippet next to the JSON plan "
            "for review."
        ),
    )
    parser.add_argument(
        "--sql-output",
        type=Path,
        help=(
            "Explicit path for the generated SQL (used with --emit-sql). Defaults to "
            "<workbook>_sheet_ingest_config.sql beside the workbook."
        ),
    )
    return parser.parse_args(argv)


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _first_non_empty_row(df: pd.DataFrame) -> tuple[int | None, list[str]]:
    for idx, row in df.iterrows():
        values = [_cell_text(v) for v in row.tolist()]
        if any(values):
            return int(idx), values
    return None, []


def _trim_trailing_empty(values: Iterable[str]) -> list[str]:
    trimmed = list(values)
    while trimmed and trimmed[-1] == "":
        trimmed.pop()
    return trimmed


def _normalize_header(value: str, index: int) -> str:
    if not value:
        return f"column_{index + 1}"
    value = value.strip().lower()
    # Preserve Unicode word characters so non-Latin headers remain readable
    # (e.g., Chinese column names). Replace any non-word character groups with
    # underscores to keep the output snake_case friendly.
    value = re.sub(r"[^\w]+", "_", value, flags=re.UNICODE)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or f"column_{index + 1}"


def _normalize_table_component(value: str) -> str:
    normalized = re.sub(r"[^\w]+", "_", value.lower(), flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "table"


def _parse_overrides(overrides: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in overrides:
        if ":" not in item:
            continue
        key, value = item.split(":", 1)
        parsed[key.strip()] = value.strip().strip("'\"")
    return parsed


def _looks_like_date(header: str, series: pd.Series) -> bool:
    header_lower = header.lower()
    date_keywords = ["date", "日期", "日", "年", "月"]
    if any(keyword in header_lower for keyword in date_keywords):
        return True

    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    sample = series.dropna().astype(str).head(10)
    date_pattern = re.compile(
        r"^(\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})$"
    )
    return any(date_pattern.match(value.strip()) for value in sample)


def _is_long_text(series: pd.Series) -> bool:
    sample = series.dropna().astype(str).head(10)
    return any(len(value) > 255 for value in sample)


def _infer_column_type(header: str, series: pd.Series) -> str:
    if _looks_like_date(header, series):
        return "DATE NULL"

    if _is_long_text(series):
        return "TEXT NULL"

    numeric_series = pd.to_numeric(series, errors="coerce")
    if numeric_series.notna().any():
        if (numeric_series.dropna() % 1 == 0).all():
            return "INTEGER NULL"
        return "NUMERIC NULL"

    return "VARCHAR(255) NULL"


def _dedupe(headers: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique: list[str] = []
    for header in headers:
        count = seen.get(header, 0)
        if count:
            candidate = f"{header}_{count + 1}"
            seen[header] = count + 1
            unique.append(candidate)
        else:
            seen[header] = 1
            unique.append(header)
    return unique


def _format_table_name(template: str, workbook_component: str, sheet_component: str) -> str:
    return template.format(workbook=workbook_component, sheet=sheet_component)


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _clean_optional_text(value: object) -> str | None:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _nullable_text_sql(value: str | None) -> str:
    if value is None:
        return "NULL"
    return f"'{_sql_escape(value)}'"


def _json_array_sql(values: Iterable[str]) -> str:
    escaped = ", ".join(f"'{_sql_escape(v)}'" for v in values)
    return f"JSON_ARRAY({escaped})"


def _json_object_sql(pairs: Iterable[tuple[str, str, bool]]) -> str:
    parts: list[str] = []
    for key, value, is_raw in pairs:
        key_sql = f"'{_sql_escape(key)}'"
        value_sql = value if is_raw else f"'{_sql_escape(value)}'"
        parts.append(f"{key_sql}, {value_sql}")
    joined = ", ".join(parts)
    return f"JSON_OBJECT({joined})"


def summarize_sheet(
    sheet_name: str,
    df: pd.DataFrame,
    workbook_component: str,
    staging_template: str,
    normalized_template: str,
    sample_rows: int,
    overrides: dict[str, str],
) -> dict[str, object]:
    workbook_component = _normalize_table_component(workbook_component)
    header_row, raw_headers = _first_non_empty_row(df)
    cleaned_headers = _trim_trailing_empty(raw_headers)
    staged = _dedupe(
        _normalize_header(value, idx) for idx, value in enumerate(cleaned_headers)
    )
    sheet_component = _normalize_table_component(sheet_name)

    data_start = header_row + 1 if header_row is not None else 0
    data_frame = df.iloc[data_start : data_start + sample_rows]
    column_types: dict[str, str] = {}
    for idx, column in enumerate(staged):
        series = data_frame.iloc[:, idx] if idx < data_frame.shape[1] else pd.Series()
        header_for_inference = cleaned_headers[idx] if idx < len(cleaned_headers) else column
        inferred = _infer_column_type(header_for_inference, series)
        column_types[column] = overrides.get(column, inferred)

    return {
        "sheet_name": sheet_name,
        "header_row_index": header_row,
        "clean_headers": cleaned_headers,
        "suggested_staging_columns": staged,
        "staging_table": _format_table_name(
            staging_template, workbook_component, sheet_component
        ),
        "metadata_columns": METADATA_COLUMNS,
        "options": {
            "normalized_table": _format_table_name(
                normalized_template, workbook_component, sheet_component
            ),
            "column_types": column_types,
        },
    }


def load_workbook(
    path: Path,
    include_sheets: list[str] | None = None,
    staging_template: str = DEFAULT_STAGING_TABLE_TEMPLATE,
    normalized_template: str = DEFAULT_NORMALIZED_TABLE_TEMPLATE,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    overrides: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    frames = pd.read_excel(path, sheet_name=None, header=None, dtype=object)
    summaries: list[dict[str, object]] = []
    workbook_component = _normalize_table_component(path.stem)
    overrides = overrides or {}
    for name, df in frames.items():
        if include_sheets and name not in include_sheets:
            continue
        summaries.append(
            summarize_sheet(
                name,
                df,
                workbook_component=workbook_component,
                staging_template=staging_template,
                normalized_template=normalized_template,
                sample_rows=sample_rows,
                overrides=overrides,
            )
        )
    return summaries


def _build_value_block(sheet: dict[str, object], workbook_type: str) -> str:
    metadata = sheet.get("metadata_columns", [])
    headers = sheet.get("clean_headers", [])
    staging_cols = sheet.get("suggested_staging_columns", [])
    time_range_column = _clean_optional_text(sheet.get("time_range_column"))
    time_range_format = _clean_optional_text(sheet.get("time_range_format"))
    overlap_target_table = _clean_optional_text(sheet.get("overlap_target_table"))

    column_mappings_pairs: list[tuple[str, str, bool]] = []
    for raw, staged in zip(headers, staging_cols):
        column_mappings_pairs.append((str(raw), str(staged), False))

    column_types = sheet.get("options", {}).get("column_types", {})
    column_types_pairs = [
        (str(column), str(sql_type), False) for column, sql_type in column_types.items()
    ]

    # options currently include normalized_table and column_types; keep this builder
    # tolerant so additional keys can be added without reworking SQL escaping logic.
    options_pairs = [
        ("normalized_table", str(sheet.get("options", {}).get("normalized_table", "")), False),
        ("column_types", _json_object_sql(column_types_pairs), True),
    ]

    if not time_range_column:
        time_range_column = _clean_optional_text(
            sheet.get("options", {}).get("time_range_column")
        )
    if not time_range_format:
        time_range_format = _clean_optional_text(
            sheet.get("options", {}).get("time_range_format")
        )
    if not overlap_target_table:
        overlap_target_table = _clean_optional_text(
            sheet.get("options", {}).get("overlap_target_table")
        )

    return "\n    (\n" + "\n".join(
        [
            f"        '{_sql_escape(workbook_type)}',",
            f"        '{_sql_escape(str(sheet.get('sheet_name', '')))}',",
            f"        '{_sql_escape(str(sheet.get('staging_table', '')))}',",
            f"        {_json_array_sql(metadata)},",
            f"        {_json_array_sql(headers)},",
            f"        {_json_object_sql(column_mappings_pairs)}",
            f"        {_json_object_sql(options_pairs)}",
            f"        {_nullable_text_sql(time_range_column)}",
            f"        {_nullable_text_sql(time_range_format)}",
            f"        {_nullable_text_sql(overlap_target_table)}",
        ]
    ) + "\n    )"


def build_ingest_config_sql(plan: dict[str, object], workbook_type: str = "default") -> str:
    sheets: list[dict[str, object]] = plan.get("sheets", []) if plan else []
    if not sheets:
        raise ValueError("Plan is missing sheets to generate SQL")

    values_blocks = [
        _build_value_block(sheet, workbook_type=workbook_type) for sheet in sheets
    ]

    header = (
        "INSERT INTO sheet_ingest_config (\n"
        "    workbook_type,\n"
        "    sheet_name,\n"
        "    staging_table,\n"
        "    metadata_columns,\n"
        "    required_columns,\n"
        "    column_mappings,\n"
        "    options,\n"
        "    time_range_column,\n"
        "    time_range_format,\n"
        "    overlap_target_table\n"
        ")\nVALUES"
    )

    footer = (
        "ON DUPLICATE KEY UPDATE\n"
        "    staging_table = VALUES(staging_table),\n"
        "    metadata_columns = VALUES(metadata_columns),\n"
        "    required_columns = VALUES(required_columns),\n"
        "    column_mappings = VALUES(column_mappings),\n"
        "    options = VALUES(options),\n"
        "    time_range_column = VALUES(time_range_column),\n"
        "    time_range_format = VALUES(time_range_format),\n"
        "    overlap_target_table = VALUES(overlap_target_table);"
    )

    return header + ",".join(values_blocks) + "\n" + footer + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.workbook.exists():
        print(f"Workbook not found: {args.workbook}", file=sys.stderr)
        return 1

    include_sheets = args.sheets if args.sheets else None
    overrides = _parse_overrides(args.column_type_override)
    summaries = load_workbook(
        args.workbook,
        include_sheets=include_sheets,
        staging_template=args.staging_table_template,
        normalized_template=args.normalized_table_template,
        sample_rows=args.sample_rows,
        overrides=overrides,
    )

    if not summaries:
        print("No sheets processed. Check the sheet names or workbook content.", file=sys.stderr)
        return 1

    output_path = args.output
    if output_path is None:
        output_path = args.workbook.with_name(
            f"{args.workbook.stem}{DEFAULT_OUTPUT_SUFFIX}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "workbook": args.workbook.name,
        "workbook_path": str(args.workbook.resolve()),
        "sheets": summaries,
    }
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Wrote ingest plan to {output_path}")

    if args.emit_sql:
        sql_path = args.sql_output
        if sql_path is None:
            sql_path = args.workbook.with_name(
                f"{args.workbook.stem}_sheet_ingest_config.sql"
            )
        sql = build_ingest_config_sql(payload, workbook_type=args.workbook_type)
        sql_path.write_text(sql, encoding="utf-8")
        print(f"Wrote sheet_ingest_config SQL to {sql_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
