"""FastAPI application exposing upload job management endpoints."""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from app import job_runner, job_store

try:  # pragma: no cover - optional during unit tests
    from app.pipeline import PipelineExecutionError
except Exception:  # pragma: no cover - defensive when DB env vars are missing
    PipelineExecutionError = None  # type: ignore[assignment]

app = FastAPI(title="SIMS Upload API")


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


class EnqueueUploadResponse(BaseModel):
    """Response body after enqueueing an upload job."""

    job: UploadJobModel


class ErrorResponse(BaseModel):
    """Standard error payload returned by the API."""

    detail: str
    validation_summary: str | None = None


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
    )
    job = job_store.get_job(job_id)
    return EnqueueUploadResponse(job=job)


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
