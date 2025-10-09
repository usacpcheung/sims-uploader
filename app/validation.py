"""Validation helpers for normalized staging data."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Sequence

from app.normalize_staging import PreparedNormalization, RejectedRow


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating prepared normalized rows."""

    prepared: PreparedNormalization
    rejected_rows_path: str | None
    errors: list[str]


_RowCheck = Callable[[tuple[object, ...], Sequence[str]], str | None]


def _rows_to_dict(
    ordered_columns: Sequence[str], row_values: tuple[object, ...]
) -> dict[str, object]:
    return {column: row_values[idx] for idx, column in enumerate(ordered_columns)}


def _write_rejected_rows(
    job_id: str,
    rejected_rows: Iterable[RejectedRow],
    *,
    output_dir: Path | None = None,
) -> str:
    target_dir = output_dir or Path("uploads") / "rejected"
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{job_id}.csv"

    collected = list(rejected_rows)
    if not collected:
        return str(path)

    fieldnames = set()
    for rejection in collected:
        fieldnames.update(rejection.data.keys())
    ordered_fields = sorted(fieldnames)
    if "errors" not in ordered_fields:
        ordered_fields.append("errors")

    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=ordered_fields)
        writer.writeheader()
        for rejection in collected:
            row = {key: rejection.data.get(key) for key in ordered_fields}
            row["errors"] = "; ".join(rejection.errors)
            writer.writerow(row)

    return str(path)


def validate_rows(
    job_id: str | None,
    prepared: PreparedNormalization,
    *,
    checks: Sequence[_RowCheck] | None = None,
    output_dir: Path | None = None,
) -> ValidationResult:
    """Run validation checks and persist rejected rows to disk."""

    checks = list(checks or [])
    combined_rejections: list[RejectedRow] = list(prepared.rejected_rows)
    valid_rows: list[tuple[object, ...]] = []

    for row in prepared.normalized_rows:
        row_errors = [
            error
            for check in checks
            if (error := check(row, prepared.ordered_columns))
        ]
        if row_errors:
            combined_rejections.append(
                RejectedRow(
                    data=_rows_to_dict(prepared.ordered_columns, row),
                    errors=tuple(row_errors),
                )
            )
            continue
        valid_rows.append(row)

    errors = [error for rejection in combined_rejections for error in rejection.errors]

    rejected_path: str | None = None
    if job_id and combined_rejections:
        rejected_path = _write_rejected_rows(
            job_id,
            combined_rejections,
            output_dir=output_dir,
        )

    updated_prepared = PreparedNormalization(
        normalized_rows=valid_rows,
        rejected_rows=combined_rejections,
        resolved_mappings=prepared.resolved_mappings,
        metadata_columns=prepared.metadata_columns,
        ordered_columns=prepared.ordered_columns,
    )

    return ValidationResult(
        prepared=updated_prepared,
        rejected_rows_path=rejected_path,
        errors=errors,
    )


__all__ = ["ValidationResult", "validate_rows"]
