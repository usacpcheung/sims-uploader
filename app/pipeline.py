"""End-to-end orchestration for preparing, staging, and normalizing uploads."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Mapping

import pymysql

from app import ingest_excel, normalize_staging, prep_excel, validation

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """Structured details about a completed (or skipped) pipeline run."""

    file_hash: str | None
    staging_table: str | None
    normalized_table: str | None
    staged_rows: int
    normalized_rows: int
    rejected_rows: int
    batch_id: str | None
    ingested_at: datetime | None
    processed_at: datetime | None
    column_coverage: dict[str, list[str]]
    inserted_count: int
    updated_count: int
    rejected_rows_path: str | None
    validation_errors: list[str]
    skipped: bool = False


class PipelineExecutionError(RuntimeError):
    """Raised when the pipeline fails mid-flight."""

    def __init__(self, message: str, *, result: PipelineResult | None = None):
        super().__init__(message)
        self.result = result


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
    job_id: str | None = None,
    status_notifier: Callable[[str, str | None], None] | None = None,
) -> PipelineResult:
    column_coverage: dict[str, list[str]] = {}
    inserted_count = 0
    updated_count = 0
    rejected_rows_path: str | None = None
    validation_errors: list[str] = []
    processed_at: datetime | None = None
    staging_result: ingest_excel.StagingLoadResult | None = None
    prepared_batch: normalize_staging.PreparedNormalization | None = None
    validation_result: validation.ValidationResult | None = None
    staging_rows: list[Mapping[str, object]] = []
    staging_table: str | None = None
    normalized_table: str | None = None
    file_hash: str | None = None
    csv_path: str | None = None
    connection = None

    if status_notifier:
        status_notifier("Parsing", None)

    try:
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
                rejected_rows=0,
                batch_id=batch_id,
                ingested_at=None,
                processed_at=None,
                column_coverage={},
                inserted_count=0,
                updated_count=0,
                rejected_rows_path=None,
                validation_errors=[],
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
        column_mappings = table_config.get("column_mappings") or None
        column_types = table_config.get("column_types") or {}
        metadata_columns = table_config.get("normalized_metadata_columns")
        reserved_source_columns = table_config.get("reserved_source_columns")
        column_type_overrides = table_config.get(
            "normalized_column_type_overrides"
        )

        settings = ingest_excel._get_db_settings(db_settings)
        connection = pymysql.connect(**settings)
        try:
            staging_rows = _fetch_staging_rows(connection, staging_table, file_hash)
            prepared_batch = normalize_staging.prepare_normalization(
                staging_rows,
                column_mappings,
                metadata_columns=metadata_columns,
                reserved_source_columns=reserved_source_columns,
            )
            column_coverage = normalize_staging.build_column_coverage(
                prepared_batch.resolved_mappings
            )
            normalize_staging.ensure_normalized_schema(
                connection,
                normalized_table,
                prepared_batch.resolved_mappings,
                column_types,
                metadata_columns=metadata_columns,
                column_type_overrides=column_type_overrides,
            )

            if status_notifier:
                status_notifier("Validating", None)

            validation_result = validation.validate_rows(job_id, prepared_batch)
            validation_errors = validation_result.errors
            rejected_rows_path = validation_result.rejected_rows_path

            connection.begin()
            insert_result = normalize_staging.insert_normalized_rows(
                connection,
                normalized_table,
                None,
                prepared=validation_result.prepared,
            )
            inserted_count = insert_result.inserted_count
            processed_at = normalize_staging.mark_staging_rows_processed(
                connection,
                staging_table,
                [row["id"] for row in staging_rows],
                file_hash=file_hash,
            )
            connection.commit()
        except Exception as exc:
            if connection is not None:
                connection.rollback()
            rejected_total = 0
            if validation_result is not None:
                rejected_total = len(validation_result.prepared.rejected_rows)
            elif prepared_batch is not None:
                rejected_total = len(prepared_batch.rejected_rows)
            result = PipelineResult(
                file_hash=file_hash,
                staging_table=staging_table,
                normalized_table=normalized_table,
                staged_rows=staging_result.rowcount if staging_result else 0,
                normalized_rows=inserted_count,
                rejected_rows=rejected_total,
                batch_id=staging_result.batch_id if staging_result else batch_id,
                ingested_at=staging_result.ingested_at if staging_result else None,
                processed_at=processed_at,
                column_coverage=column_coverage,
                inserted_count=inserted_count,
                updated_count=updated_count,
                rejected_rows_path=rejected_rows_path,
                validation_errors=validation_errors,
                skipped=False,
            )
            raise PipelineExecutionError(str(exc), result=result) from exc
    except PipelineExecutionError:
        raise
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        result = PipelineResult(
            file_hash=file_hash,
            staging_table=staging_table,
            normalized_table=normalized_table,
            staged_rows=staging_result.rowcount if staging_result else 0,
            normalized_rows=inserted_count,
            rejected_rows=0,
            batch_id=staging_result.batch_id if staging_result else batch_id,
            ingested_at=staging_result.ingested_at if staging_result else None,
            processed_at=processed_at,
            column_coverage=column_coverage,
            inserted_count=inserted_count,
            updated_count=updated_count,
            rejected_rows_path=rejected_rows_path,
            validation_errors=validation_errors,
            skipped=False,
        )
        raise PipelineExecutionError(str(exc), result=result) from exc
    finally:
        if connection is not None:
            connection.close()

    rejected_total = (
        len(validation_result.prepared.rejected_rows)
        if validation_result
        else 0
    )

    return PipelineResult(
        file_hash=file_hash,
        staging_table=staging_table,
        normalized_table=normalized_table,
        staged_rows=staging_result.rowcount if staging_result else 0,
        normalized_rows=inserted_count,
        rejected_rows=rejected_total,
        batch_id=staging_result.batch_id if staging_result else batch_id,
        ingested_at=staging_result.ingested_at if staging_result else None,
        processed_at=processed_at,
        column_coverage=column_coverage,
        inserted_count=inserted_count,
        updated_count=updated_count,
        rejected_rows_path=rejected_rows_path,
        validation_errors=validation_errors,
    )


def cli(argv: Iterable[str] | None = None) -> str:
    from app import job_runner  # Imported lazily to avoid circular dependency

    args = _parse_args(argv)
    try:
        file_size = Path(args.workbook).stat().st_size
    except OSError as exc:  # pragma: no cover - exercised via CLI integration
        print(f"Unable to stat workbook {args.workbook}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        job_id, _ = job_runner.enqueue_job(
            workbook_path=args.workbook,
            sheet=args.sheet,
            workbook_type=args.workbook_type,
            source_year=args.source_year,
            batch_id=args.batch_id,
            file_size=file_size,
        )
    except job_runner.UploadLimitExceeded as exc:
        print(f"Upload rejected: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except Exception as exc:  # pragma: no cover - exercised via CLI integration
        print(f"Failed to enqueue upload: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Queued upload job {job_id} for workbook {args.workbook}")
    return job_id


if __name__ == "__main__":  # pragma: no cover - exercised via manual CLI usage
    cli()
