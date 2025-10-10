"""Queue and worker utilities for running ingestion jobs asynchronously."""

from __future__ import annotations

import argparse
import logging
import os
import signal
from pathlib import Path
from typing import Any, Mapping

try:  # pragma: no cover - optional dependency guard
    from redis import Redis
except ImportError:  # pragma: no cover
    Redis = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency guard
    from rq import Queue, Worker
except ImportError:  # pragma: no cover
    Queue = Worker = None  # type: ignore[assignment]

from app import job_store, pipeline, prep_excel
from app.config import load_environment

LOGGER = logging.getLogger(__name__)

REDIS_URL_ENV = "REDIS_URL"
QUEUE_NAME_ENV = "UPLOAD_QUEUE_NAME"
FILE_SIZE_LIMIT_ENV = "UPLOAD_MAX_FILE_SIZE_BYTES"
ROW_LIMIT_ENV = "UPLOAD_MAX_ROWS"

DEFAULT_QUEUE_NAME = "sims_uploads"
DEFAULT_FILE_SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB
DEFAULT_ROW_LIMIT = 500_000


class UploadLimitExceeded(ValueError):
    """Raised when a workbook violates pre-enqueue limits."""


def _load_environment() -> None:
    """Ensure .env values are loaded before inspecting os.environ."""

    load_environment()


def _resolve_int(value: int | None, env_key: str, default: int | None = None) -> int | None:
    if value is not None:
        return value
    raw = os.getenv(env_key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Environment variable {env_key} must be an integer") from exc


def get_queue(
    queue_name: str | None = None,
    *,
    redis_url: str | None = None,
) -> Queue:
    """Return an RQ queue configured from environment variables."""

    _load_environment()
    if Redis is None or Queue is None:
        raise RuntimeError("Install redis and rq to use the upload worker queue")
    redis_url = redis_url or os.getenv(REDIS_URL_ENV)
    if not redis_url:
        raise RuntimeError(
            "Missing REDIS_URL environment variable; configure Redis before starting workers"
        )

    queue_name = queue_name or os.getenv(QUEUE_NAME_ENV, DEFAULT_QUEUE_NAME)
    connection = Redis.from_url(redis_url)
    return Queue(queue_name, connection=connection)


def _enforce_limits(
    file_size: int | None,
    row_count: int | None,
    max_file_size: int | None,
    max_rows: int | None,
) -> None:
    resolved_file_limit = _resolve_int(max_file_size, FILE_SIZE_LIMIT_ENV, DEFAULT_FILE_SIZE_LIMIT)
    resolved_row_limit = _resolve_int(max_rows, ROW_LIMIT_ENV, DEFAULT_ROW_LIMIT)

    if file_size is not None and resolved_file_limit is not None and file_size > resolved_file_limit:
        size_mb = file_size / (1024 * 1024)
        limit_mb = resolved_file_limit / (1024 * 1024)
        raise UploadLimitExceeded(
            f"File size {size_mb:.1f} MiB exceeds limit of {limit_mb:.1f} MiB"
        )

    if row_count is not None and resolved_row_limit is not None and row_count > resolved_row_limit:
        raise UploadLimitExceeded(
            f"Workbook row count {row_count:,} exceeds limit of {resolved_row_limit:,}"
        )


def enqueue_job(
    *,
    workbook_path: str,
    sheet: str,
    source_year: str,
    workbook_type: str = "default",
    batch_id: str | None = None,
    workbook_name: str | None = None,
    worksheet_name: str | None = None,
    file_size: int | None = None,
    row_count: int | None = None,
    queue: Queue | None = None,
    db_settings: Mapping[str, Any] | None = None,
    max_file_size: int | None = None,
    max_rows: int | None = None,
) -> tuple[str, Any]:
    """Create a job record and enqueue work on the Redis queue."""

    _load_environment()
    queue = queue or get_queue()

    job = job_store.create_job(
        original_filename=Path(workbook_path).name,
        workbook_name=workbook_name,
        worksheet_name=worksheet_name,
        file_size=file_size,
        status="Queued",
        status_message="Queued for processing",
        db_settings=db_settings,
    )

    try:
        _enforce_limits(
            file_size=file_size,
            row_count=row_count,
            max_file_size=max_file_size,
            max_rows=max_rows,
        )
    except UploadLimitExceeded as exc:
        job_store.mark_error(job.job_id, message=str(exc), db_settings=db_settings)
        raise

    payload = {
        "workbook_path": workbook_path,
        "sheet": sheet,
        "workbook_type": workbook_type,
        "source_year": source_year,
        "batch_id": batch_id,
        "db_settings": db_settings,
    }

    rq_job = queue.enqueue(process_job, job.job_id, payload, job_id=job.job_id)
    LOGGER.info("Enqueued upload job %s for %s", job.job_id, workbook_path)
    return job.job_id, rq_job


def _record_job_results(
    job_id: str,
    result: pipeline.PipelineResult,
    *,
    db_settings: Mapping[str, Any] | None,
) -> None:
    processed_rows = result.inserted_count + result.updated_count
    job_store.record_results(
        job_id,
        total_rows=result.staged_rows,
        processed_rows=processed_rows,
        successful_rows=processed_rows,
        rejected_rows=result.rejected_rows,
        normalized_table_name=result.normalized_table,
        coverage_metadata=result.column_coverage,
        db_settings=db_settings,
    )
    if result.rejected_rows_path:
        job_store.save_rejected_rows_path(
            job_id,
            result.rejected_rows_path,
            db_settings=db_settings,
        )


def process_job(job_id: str, payload: Mapping[str, Any]) -> pipeline.PipelineResult:
    """Worker entry point executed by RQ."""

    db_settings = payload.get("db_settings")

    def _notify(status: str, message: str | None) -> None:
        if status == "Parsing":
            job_store.mark_parsing(job_id, message=message, db_settings=db_settings)
        elif status == "Validating":
            job_store.mark_validating(job_id, message=message, db_settings=db_settings)
        else:  # pragma: no cover - defensive logging for future states
            LOGGER.info("Unhandled status notification %s for job %s", status, job_id)

    try:
        result = pipeline.run_pipeline(
            payload["workbook_path"],
            payload.get("sheet", prep_excel.DEFAULT_SHEET),
            workbook_type=payload.get("workbook_type", "default"),
            source_year=payload["source_year"],
            batch_id=payload.get("batch_id"),
            db_settings=db_settings,
            job_id=job_id,
            status_notifier=_notify,
        )
    except pipeline.PipelineExecutionError as exc:
        if exc.result is not None:
            _record_job_results(job_id, exc.result, db_settings=db_settings)
            if exc.result.validation_errors:
                message = ", ".join(exc.result.validation_errors[:3])
            else:
                message = str(exc)
        else:
            message = str(exc)
        job_store.mark_error(job_id, message=message, db_settings=db_settings)
        raise

    _record_job_results(job_id, result, db_settings=db_settings)

    if result.validation_errors:
        message = ", ".join(result.validation_errors[:3])
        if len(result.validation_errors) > 3:
            message += f" (and {len(result.validation_errors) - 3} more)"
        job_store.mark_error(job_id, message=message, db_settings=db_settings)
    elif result.skipped:
        job_store.mark_loaded(
            job_id,
            message="Duplicate upload detected",
            db_settings=db_settings,
        )
    else:
        job_store.mark_loaded(job_id, db_settings=db_settings)

    return result


def _install_signal_handlers(worker: Worker) -> None:
    def _graceful_shutdown(signum, frame):  # pragma: no cover - signal handling
        LOGGER.info("Received signal %s; draining queue before shutdown", signum)
        worker._burst = True  # type: ignore[attr-defined]
        worker.request_stop()

    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)


def run_worker(*, queue_name: str | None = None, redis_url: str | None = None) -> None:
    if Queue is None or Worker is None:  # pragma: no cover - dependency guard
        raise RuntimeError("Install rq to run the background worker")
    queue = get_queue(queue_name=queue_name, redis_url=redis_url)
    worker = Worker([queue], connection=queue.connection)
    _install_signal_handlers(worker)
    LOGGER.info("Starting worker for queue %s", queue.name)
    worker.work(with_scheduler=False)
    LOGGER.info("Worker stopped")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    worker_parser = subparsers.add_parser("worker", help="Start an RQ worker")
    worker_parser.add_argument("--queue", help="Queue name override")
    worker_parser.add_argument("--redis-url", help="Redis connection URL override")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.command == "worker":
        run_worker(queue_name=args.queue, redis_url=args.redis_url)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
