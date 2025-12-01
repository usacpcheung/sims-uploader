"""CLI helper to summarize Excel headers for ingest configuration planning.

Usage examples:

    python app/ingest_planner.py uploads/sample.xlsx
    python app/ingest_planner.py uploads/sample.xlsx --sheets Sheet1 Sheet2
    python app/ingest_planner.py uploads/sample.xlsx --output uploads/plan.json

The script reads the provided workbook without modifying it, identifies the first
non-empty header row per sheet, and writes a JSON summary containing the sheet
name, cleaned header values, and suggested snake_case staging column names.
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


def summarize_sheet(
    sheet_name: str,
    df: pd.DataFrame,
    workbook_component: str,
    staging_template: str,
    normalized_template: str,
) -> dict[str, object]:
    workbook_component = _normalize_table_component(workbook_component)
    header_row, raw_headers = _first_non_empty_row(df)
    cleaned_headers = _trim_trailing_empty(raw_headers)
    staged = _dedupe(
        _normalize_header(value, idx) for idx, value in enumerate(cleaned_headers)
    )
    sheet_component = _normalize_table_component(sheet_name)
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
            )
        },
    }


def load_workbook(
    path: Path,
    include_sheets: list[str] | None = None,
    staging_template: str = DEFAULT_STAGING_TABLE_TEMPLATE,
    normalized_template: str = DEFAULT_NORMALIZED_TABLE_TEMPLATE,
) -> list[dict[str, object]]:
    frames = pd.read_excel(path, sheet_name=None, header=None, dtype=object)
    summaries: list[dict[str, object]] = []
    workbook_component = _normalize_table_component(path.stem)
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
            )
        )
    return summaries


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.workbook.exists():
        print(f"Workbook not found: {args.workbook}", file=sys.stderr)
        return 1

    include_sheets = args.sheets if args.sheets else None
    summaries = load_workbook(
        args.workbook,
        include_sheets=include_sheets,
        staging_template=args.staging_table_template,
        normalized_template=args.normalized_table_template,
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
