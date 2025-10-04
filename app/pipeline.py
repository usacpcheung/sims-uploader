"""End-to-end orchestration for preparing, staging, and normalizing uploads."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping

import pymysql

from app import ingest_excel, normalize_staging, prep_excel

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """Structured details about a completed (or skipped) pipeline run."""

    file_hash: str | None
    staging_table: str | None
    normalized_table: str | None
    staged_rows: int
    normalized_rows: int
    batch_id: str | None
    ingested_at: datetime | None
    processed_at: datetime | None
    skipped: bool = False


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", help="Path to the Excel workbook to ingest")
    parser.add_argument(
        "sheet",
        nargs="?",
        default=prep_excel.DEFAULT_SHEET,
        help="Worksheet name inside the workbook (default: %(default)s)",
    )
    parser.add_argument(
        "--source-year",
        required=True,
        help="Source year associated with the uploaded data",
    )
    parser.add_argument(
        "--batch-id",
        help="Optional batch identifier to associate with the upload",
    )
    parser.add_argument(
        "--workbook-type",
        default="default",
        help="Workbook type used to select configuration overrides (default: %(default)s)",
    )
    return parser.parse_args(argv)


def _fetch_staging_rows(connection, table: str, file_hash: str):
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            (
                "SELECT * FROM `{table}` WHERE file_hash = %s "
                "AND (processed_at IS NULL OR processed_at = '0000-00-00 00:00:00')"
            ).format(table=table),
            (file_hash,),
        )
        return cursor.fetchall()


def run_pipeline(
    workbook_path: str,
    sheet: str = prep_excel.DEFAULT_SHEET,
    *,
    workbook_type: str = "default",
    source_year: str,
    batch_id: str | None = None,
    db_settings: Mapping[str, object] | None = None,
) -> PipelineResult:
    csv_path, file_hash = prep_excel.main(
        workbook_path,
        sheet,
        workbook_type=workbook_type,
        emit_stdout=False,
        db_settings=db_settings,
    )

    if csv_path is None:
        LOGGER.info(
            "Skipping pipeline for %s; duplicate hash %s", workbook_path, file_hash
        )
        return PipelineResult(
            file_hash=file_hash,
            staging_table=None,
            normalized_table=None,
            staged_rows=0,
            normalized_rows=0,
            batch_id=batch_id,
            ingested_at=None,
            processed_at=None,
            skipped=True,
        )

    staging_result = ingest_excel.load_csv_into_staging(
        csv_path,
        sheet=sheet,
        workbook_type=workbook_type,
        source_year=source_year,
        file_hash=file_hash,
        batch_id=batch_id,
        db_settings=db_settings,
    )

    table_config = prep_excel._get_table_config(
        sheet,
        workbook_type=workbook_type,
        db_settings=db_settings,
    )
    staging_table = table_config["table"]
    normalized_table = table_config.get("normalized_table")
    if not normalized_table:
        raise ValueError(
            f"Sheet {sheet!r} is missing a normalized_table configuration"
        )
    column_mappings = table_config.get("column_mappings")
    if not column_mappings:
        column_mappings = None
    column_types = table_config.get("column_types") or {}

    settings = ingest_excel._get_db_settings(db_settings)
    connection = pymysql.connect(**settings)
    try:
        staging_rows = _fetch_staging_rows(connection, staging_table, file_hash)
        resolved_mappings = normalize_staging.resolve_column_mappings(
            staging_rows, column_mappings
        )
        normalize_staging.ensure_normalized_schema(
            connection, normalized_table, resolved_mappings, column_types
        )

        connection.begin()
        normalized_rows = normalize_staging.insert_normalized_rows(
            connection,
            normalized_table,
            staging_rows,
            resolved_mappings,
        )
        processed_at = normalize_staging.mark_staging_rows_processed(
            connection,
            staging_table,
            [row["id"] for row in staging_rows],
            file_hash=file_hash,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return PipelineResult(
        file_hash=file_hash,
        staging_table=staging_table,
        normalized_table=normalized_table,
        staged_rows=staging_result.rowcount,
        normalized_rows=normalized_rows,
        batch_id=staging_result.batch_id,
        ingested_at=staging_result.ingested_at,
        processed_at=processed_at,
    )


def cli(argv: Iterable[str] | None = None) -> PipelineResult:
    args = _parse_args(argv)
    try:
        result = run_pipeline(
            args.workbook,
            args.sheet,
            workbook_type=args.workbook_type,
            source_year=args.source_year,
            batch_id=args.batch_id,
        )
    except Exception as exc:  # pragma: no cover - exercised via CLI integration
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if result.skipped:
        print(
            f"Skipped ingest; duplicate file hash {result.file_hash} already processed."
        )
    else:
        print(
            "Staged {rows} rows into {staging} and normalized {normalized} rows into {normalized_table}. "
            "file_hash={hash} batch_id={batch}"
            .format(
                rows=result.staged_rows,
                staging=result.staging_table,
                normalized=result.normalized_rows,
                normalized_table=result.normalized_table,
                hash=result.file_hash,
                batch=result.batch_id,
            )
        )
    return result


if __name__ == "__main__":  # pragma: no cover - exercised via manual CLI usage
    cli()
