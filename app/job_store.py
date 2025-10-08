"""Helpers for interacting with upload job tracking tables."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import pymysql
from pymysql import err as pymysql_err

from app.config import get_db_settings


@dataclass(frozen=True)
class UploadJob:
    """Structured representation of a row in ``upload_jobs``."""

    job_id: str
    original_filename: str
    workbook_name: str | None
    worksheet_name: str | None
    file_size: int | None
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class UploadJobEvent:
    """Structured representation of a row in ``upload_job_events``."""

    event_id: int
    job_id: str
    status: str
    message: str | None
    event_at: datetime


@dataclass(frozen=True)
class UploadJobResult:
    """Structured representation of a row in ``upload_job_results``."""

    job_id: str
    total_rows: int | None
    processed_rows: int | None
    successful_rows: int | None
    rejected_rows: int | None
    normalized_table_name: str | None
    rejected_rows_path: str | None
    coverage_metadata: Any
    created_at: datetime
    updated_at: datetime


def _connect(overrides: Mapping[str, Any] | None = None) -> pymysql.connections.Connection:
    settings: dict[str, Any] = dict(get_db_settings())
    if overrides:
        settings.update(overrides)
    return pymysql.connect(**settings)


def _dict_to_upload_job(row: Mapping[str, Any] | None) -> UploadJob:
    if row is None:
        raise ValueError("Upload job not found")
    return UploadJob(
        job_id=row["job_id"],
        original_filename=row["original_filename"],
        workbook_name=row.get("workbook_name"),
        worksheet_name=row.get("worksheet_name"),
        file_size=row.get("file_size"),
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _dict_to_upload_job_event(row: Mapping[str, Any] | None) -> UploadJobEvent:
    if row is None:
        raise ValueError("Upload job event not found")
    return UploadJobEvent(
        event_id=row["event_id"],
        job_id=row["job_id"],
        status=row["status"],
        message=row.get("message"),
        event_at=row["event_at"],
    )


def _dict_to_upload_job_result(row: Mapping[str, Any] | None) -> UploadJobResult:
    if row is None:
        raise ValueError("Upload job results not found")
    return UploadJobResult(
        job_id=row["job_id"],
        total_rows=row.get("total_rows"),
        processed_rows=row.get("processed_rows"),
        successful_rows=row.get("successful_rows"),
        rejected_rows=row.get("rejected_rows"),
        normalized_table_name=row.get("normalized_table_name"),
        rejected_rows_path=row.get("rejected_rows_path"),
        coverage_metadata=row.get("coverage_metadata"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def create_job(
    *,
    original_filename: str,
    status: str,
    job_id: str | None = None,
    workbook_name: str | None = None,
    worksheet_name: str | None = None,
    file_size: int | None = None,
    status_message: str | None = None,
    db_settings: Mapping[str, Any] | None = None,
) -> UploadJob:
    """Create a new upload job and optionally log its first status event."""

    job_id = job_id or str(uuid.uuid4())
    connection = _connect(db_settings)
    try:
        connection.begin()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    (
                        "INSERT INTO `upload_jobs` "
                        "(job_id, original_filename, workbook_name, worksheet_name, file_size, status) "
                        "VALUES (%s, %s, %s, %s, %s, %s)"
                    ),
                    (
                        job_id,
                        original_filename,
                        workbook_name,
                        worksheet_name,
                        file_size,
                        status,
                    ),
                )
                if status_message is not None:
                    cursor.execute(
                        (
                            "INSERT INTO `upload_job_events` (job_id, status, message) "
                            "VALUES (%s, %s, %s)"
                        ),
                        (job_id, status, status_message),
                    )
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM `upload_jobs` WHERE job_id = %s",
                    (job_id,),
                )
                job_row = cursor.fetchone()
            connection.commit()
        except pymysql_err.IntegrityError as exc:
            connection.rollback()
            raise ValueError(f"Failed to create upload job: {exc.args[1] if len(exc.args) > 1 else exc}") from exc
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()

    return _dict_to_upload_job(job_row)


def set_status(
    job_id: str,
    status: str,
    *,
    message: str | None = None,
    db_settings: Mapping[str, Any] | None = None,
) -> tuple[UploadJob, UploadJobEvent]:
    """Update the job status and record a corresponding event."""

    connection = _connect(db_settings)
    try:
        connection.begin()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE `upload_jobs` SET status = %s WHERE job_id = %s",
                    (status, job_id),
                )
                if cursor.rowcount == 0:
                    cursor.execute(
                        "SELECT 1 FROM `upload_jobs` WHERE job_id = %s",
                        (job_id,),
                    )
                    if cursor.fetchone() is None:
                        raise ValueError(f"Upload job {job_id} does not exist")
                cursor.execute(
                    (
                        "INSERT INTO `upload_job_events` (job_id, status, message) "
                        "VALUES (%s, %s, %s)"
                    ),
                    (job_id, status, message),
                )
                event_id = cursor.lastrowid
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM `upload_jobs` WHERE job_id = %s",
                    (job_id,),
                )
                job_row = cursor.fetchone()
                cursor.execute(
                    "SELECT * FROM `upload_job_events` WHERE event_id = %s",
                    (event_id,),
                )
                event_row = cursor.fetchone()
            connection.commit()
        except pymysql_err.IntegrityError as exc:
            connection.rollback()
            raise ValueError(
                f"Failed to update status for upload job {job_id}: "
                f"{exc.args[1] if len(exc.args) > 1 else exc}"
            ) from exc
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()

    return _dict_to_upload_job(job_row), _dict_to_upload_job_event(event_row)


def record_results(
    job_id: str,
    *,
    total_rows: int | None = None,
    processed_rows: int | None = None,
    successful_rows: int | None = None,
    rejected_rows: int | None = None,
    normalized_table_name: str | None = None,
    coverage_metadata: Any = None,
    db_settings: Mapping[str, Any] | None = None,
) -> UploadJobResult:
    """Insert or update upload job results and return the stored record."""

    columns: list[str] = []
    values: list[Any] = []
    updates: list[str] = []
    field_map = {
        "total_rows": total_rows,
        "processed_rows": processed_rows,
        "successful_rows": successful_rows,
        "rejected_rows": rejected_rows,
        "normalized_table_name": normalized_table_name,
        "coverage_metadata": json.dumps(coverage_metadata)
        if isinstance(coverage_metadata, (dict, list))
        else coverage_metadata,
    }

    for column, value in field_map.items():
        columns.append(f"`{column}`")
        values.append(value)
        updates.append(f"`{column}` = VALUES(`{column}`)")

    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)
    update_clause = ", ".join(updates) + ", `updated_at` = CURRENT_TIMESTAMP"

    connection = _connect(db_settings)
    try:
        connection.begin()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    (
                        "INSERT INTO `upload_job_results` (job_id, {columns}) "
                        "VALUES (%s, {placeholders}) "
                        "ON DUPLICATE KEY UPDATE {updates}"
                    ).format(columns=column_list, placeholders=placeholders, updates=update_clause),
                    [job_id, *values],
                )
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM `upload_job_results` WHERE job_id = %s",
                    (job_id,),
                )
                result_row = cursor.fetchone()
            connection.commit()
        except pymysql_err.IntegrityError as exc:
            connection.rollback()
            raise ValueError(
                f"Failed to record results for upload job {job_id}: "
                f"{exc.args[1] if len(exc.args) > 1 else exc}"
            ) from exc
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()

    return _dict_to_upload_job_result(result_row)


def save_rejected_rows_path(
    job_id: str,
    rejected_rows_path: str | None,
    *,
    db_settings: Mapping[str, Any] | None = None,
) -> UploadJobResult:
    """Persist the rejected rows path for an upload job."""

    connection = _connect(db_settings)
    try:
        connection.begin()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    (
                        "UPDATE `upload_job_results` "
                        "SET rejected_rows_path = %s, updated_at = CURRENT_TIMESTAMP "
                        "WHERE job_id = %s"
                    ),
                    (rejected_rows_path, job_id),
                )
                if cursor.rowcount == 0:
                    raise ValueError(
                        "Cannot save rejected rows path before recording results"
                    )
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM `upload_job_results` WHERE job_id = %s",
                    (job_id,),
                )
                result_row = cursor.fetchone()
            connection.commit()
        except pymysql_err.IntegrityError as exc:
            connection.rollback()
            raise ValueError(
                f"Failed to save rejected rows path for upload job {job_id}: "
                f"{exc.args[1] if len(exc.args) > 1 else exc}"
            ) from exc
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()

    return _dict_to_upload_job_result(result_row)

