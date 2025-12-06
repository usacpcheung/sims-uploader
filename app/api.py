"""FastAPI application exposing upload job management endpoints."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from http import HTTPStatus
from typing import Any, Literal

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from app import job_runner, job_store, prep_excel
from app.config import get_upload_storage_dir
from app.storage import generate_stored_path, get_original_filename, validate_extension
from app.web import router as web_router

try:  # pragma: no cover - optional during unit tests
    from app.pipeline import PipelineExecutionError
except Exception:  # pragma: no cover - defensive when DB env vars are missing
    PipelineExecutionError = None  # type: ignore[assignment]

app = FastAPI(title="SIMS Upload API")

BASE_DIR = Path(__file__).resolve().parent.parent

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
app.include_router(web_router)


class UploadJobModel(BaseModel):
    """API representation of :class:`app.job_store.UploadJob`."""

    job_id: str
    original_filename: str
    workbook_name: str | None
    worksheet_name: str | None
    file_size: int | None
    status: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UploadJobEventModel(BaseModel):
    """API representation of :class:`app.job_store.UploadJobEvent`."""

    event_id: int
    job_id: str
    status: str
    message: str | None
    event_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UploadJobResultModel(BaseModel):
    """API representation of :class:`app.job_store.UploadJobResult`."""

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

    model_config = ConfigDict(from_attributes=True)


class UploadJobDetailModel(BaseModel):
    """Detailed response for a single upload job."""

    job: UploadJobModel
    result: UploadJobResultModel | None = None


class ParsedTimeRange(BaseModel):
    start: datetime
    end: datetime


class EnqueueUploadRequest(BaseModel):
    """Request body for ``POST /uploads``."""

    workbook_path: str
    sheet: str
    source_year: str
    workbook_type: str = "default"
    batch_id: str | None = None
    workbook_name: str | None = None
    worksheet_name: str | None = None
    file_size: int | None = None
    row_count: int | None = None
    max_file_size: int | None = None
    max_rows: int | None = None
    time_ranges: list[ParsedTimeRange] | None = None
    conflict_resolution: Literal["append", "replace", "skip"] = "append"


class OverlapDetail(BaseModel):
    target_table: str
    time_range_column: str
    requested_start: datetime
    requested_end: datetime
    existing_start: datetime | None = None
    existing_end: datetime | None = None
    record_id: int | None = None


class OverlapDetected(BaseModel):
    """Payload returned when overlap preflight checks fail."""

    summary: str | None = None
    overlaps: list[OverlapDetail]


class EnqueueUploadResponse(BaseModel):
    """Response body after enqueueing an upload job."""

    job: UploadJobModel | None = None
    overlaps: list[OverlapDetail] | None = None
    overlap_detected: OverlapDetected | None = None


class ErrorResponse(BaseModel):
    """Standard error payload returned by the API."""

    detail: str
    validation_summary: str | None = None


class UploadFileResponse(BaseModel):
    """Response returned after persisting an uploaded file."""

    stored_path: str
    original_filename: str
    file_size: int


@app.exception_handler(job_runner.UploadLimitExceeded)
async def _handle_limit_error(request, exc: job_runner.UploadLimitExceeded):  # pragma: no cover - FastAPI hooks
    summary = str(exc)
    return JSONResponse(
        status_code=HTTPStatus.BAD_REQUEST,
        content=ErrorResponse(detail=summary, validation_summary=None).model_dump(),
    )


@app.exception_handler(ValueError)
async def _handle_value_error(request, exc: ValueError):  # pragma: no cover - FastAPI hooks
    message = str(exc)
    lowered = message.lower()
    if "not found" in lowered or "does not exist" in lowered:
        status_code = HTTPStatus.NOT_FOUND
    else:
        status_code = HTTPStatus.BAD_REQUEST
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(detail=message, validation_summary=None).model_dump(),
    )


if PipelineExecutionError is not None:  # pragma: no cover - registration happens when available

    @app.exception_handler(PipelineExecutionError)
    async def _handle_pipeline_error(request, exc: PipelineExecutionError):
        summary: str | None = None
        result = getattr(exc, "result", None)
        if result and getattr(result, "validation_errors", None):
            validation_errors = result.validation_errors
            summary = ", ".join(validation_errors[:3])
            if len(validation_errors) > 3:
                summary += f" (and {len(validation_errors) - 3} more)"
        return JSONResponse(
            status_code=HTTPStatus.BAD_REQUEST,
            content=ErrorResponse(detail=str(exc), validation_summary=summary).model_dump(),
        )


@app.post("/uploads", response_model=EnqueueUploadResponse, status_code=HTTPStatus.ACCEPTED)
def create_upload_job(payload: EnqueueUploadRequest) -> EnqueueUploadResponse:
    """Create a new upload job and enqueue it for background processing."""

    table_config = prep_excel._get_table_config(
        payload.sheet, workbook_type=payload.workbook_type
    )
    overlaps = job_runner.check_time_overlap(
        workbook_type=payload.workbook_type,
        target_table=table_config.get("overlap_target_table"),
        time_range_column=table_config.get("time_range_column"),
        time_ranges=[range_.model_dump() for range_ in payload.time_ranges]
        if payload.time_ranges
        else None,
    )
    if overlaps and payload.conflict_resolution == "append":
        primary_overlap = overlaps[0]
        table = primary_overlap.get("target_table") or "target table"
        column = primary_overlap.get("time_range_column") or "time range"
        summary = f"Detected overlapping time ranges in {table} ({column})"
        overlap_detected = OverlapDetected(summary=summary, overlaps=overlaps)
        return JSONResponse(
            status_code=HTTPStatus.CONFLICT,
            content=EnqueueUploadResponse(
                overlaps=overlaps, overlap_detected=overlap_detected
            ).model_dump(mode="json"),
        )

    job_id, _ = job_runner.enqueue_job(
        workbook_path=payload.workbook_path,
        sheet=payload.sheet,
        source_year=payload.source_year,
        workbook_type=payload.workbook_type,
        batch_id=payload.batch_id,
        workbook_name=payload.workbook_name,
        worksheet_name=payload.worksheet_name,
        file_size=payload.file_size,
        row_count=payload.row_count,
        max_file_size=payload.max_file_size,
        max_rows=payload.max_rows,
        time_ranges=[range_.model_dump() for range_ in payload.time_ranges]
        if payload.time_ranges
        else None,
        conflict_resolution=payload.conflict_resolution,
    )
    job = job_store.get_job(job_id)
    return EnqueueUploadResponse(job=job)


@app.post("/uploads/files", response_model=UploadFileResponse, status_code=HTTPStatus.CREATED)
async def upload_workbook(file: UploadFile = File(...)) -> UploadFileResponse:
    """Persist an uploaded workbook to the configured storage directory."""

    original_filename = file.filename or ""
    try:
        validate_extension(original_filename)
    except ValueError as exc:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(exc)) from exc

    storage_dir = get_upload_storage_dir()
    stored_path = generate_stored_path(original_filename, storage_dir=storage_dir)
    resolved_limit = job_runner.resolve_file_size_limit(None)
    total_bytes = 0
    chunk_size = 1024 * 1024

    try:
        with stored_path.open("wb") as destination:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if resolved_limit is not None and total_bytes > resolved_limit:
                    size_mb = total_bytes / (1024 * 1024)
                    limit_mb = resolved_limit / (1024 * 1024)
                    raise job_runner.UploadLimitExceeded(
                        f"File size {size_mb:.1f} MiB exceeds limit of {limit_mb:.1f} MiB"
                    )
                destination.write(chunk)
    except Exception:
        if stored_path.exists():
            stored_path.unlink(missing_ok=True)
        raise
    finally:
        await file.close()

    return UploadFileResponse(
        stored_path=str(stored_path),
        original_filename=get_original_filename(stored_path),
        file_size=total_bytes,
    )


@app.get("/uploads/{job_id}", response_model=UploadJobDetailModel)
def get_upload_job(job_id: str) -> UploadJobDetailModel:
    """Return metadata and latest results for an upload job."""

    job = job_store.get_job(job_id)
    try:
        result = job_store.get_job_result(job_id)
    except ValueError as exc:
        message = str(exc).lower()
        if "not found" in message or "does not exist" in message:
            result = None
        else:
            raise
    return UploadJobDetailModel(job=job, result=result)


@app.get("/uploads/{job_id}/events", response_model=list[UploadJobEventModel])
def list_upload_events(job_id: str, limit: int | None = Query(None, gt=0)) -> list[UploadJobEventModel]:
    """Return the event history for an upload job."""

    events = job_store.list_job_events(job_id, limit=limit)
    return events


@app.get("/uploads", response_model=list[UploadJobModel])
def list_recent_uploads(limit: int = Query(20, gt=0, le=100)) -> list[UploadJobModel]:
    """Return recently created upload jobs ordered newest first."""

    return job_store.list_recent_jobs(limit=limit)
