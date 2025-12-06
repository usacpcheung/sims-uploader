"""Utilities for deriving time ranges from staged upload data."""

from __future__ import annotations

import warnings
from datetime import date, datetime
from typing import Iterable, Mapping

import pandas as pd

from app import prep_excel


def _parse_datetime(value: object, time_format: str | None, *, label: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    try:
        if time_format:
            return datetime.strptime(text, time_format)
        return datetime.fromisoformat(text)
    except ValueError as exc:  # pragma: no cover - passthrough to caller
        raise ValueError(f"Invalid {label}: {text!r}") from exc


def derive_ranges_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    time_range_column: str | None,
    time_range_format: str | None,
) -> list[dict[str, datetime]]:
    """Compute the min/max datetime range from staged row values."""

    if not time_range_column:
        return []

    parsed_values: list[datetime] = []
    for idx, row in enumerate(rows, start=1):
        value = row.get(time_range_column)
        parsed = _parse_datetime(
            value,
            time_range_format,
            label=f"{time_range_column} value in row {idx}",
        )
        if parsed is None:
            continue
        parsed_values.append(parsed)

    if not parsed_values:
        return []

    return [{"start": min(parsed_values), "end": max(parsed_values)}]


def derive_ranges_from_workbook(
    workbook_path: str,
    *,
    sheet: str,
    workbook_type: str,
    time_range_column: str | None,
    time_range_format: str | None,
    rename_last_subject: bool = False,
) -> list[dict[str, datetime]]:
    """Read workbook rows and derive time ranges for preflight checks."""

    if not time_range_column:
        return []

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Workbook contains no default style, apply openpyxl's default",
                category=UserWarning,
                module="openpyxl.styles.stylesheet",
            )
            df = pd.read_excel(workbook_path, sheet_name=sheet, dtype=str)
    except (OSError, ValueError):
        return []

    df = prep_excel.normalize_headers_and_subject(
        df, rename_last_subject=rename_last_subject
    )

    if time_range_column not in df.columns:
        return []

    return derive_ranges_from_rows(
        df.to_dict(orient="records"),
        time_range_column=time_range_column,
        time_range_format=time_range_format,
    )
