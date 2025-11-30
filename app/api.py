"""FastAPI application exposing upload job management endpoints."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from datetime import datetime
from http import HTTPStatus
from typing import Any
import warnings

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from app import job_runner, job_store
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


class UploadFileResponse(BaseModel):
    """Response returned after persisting an uploaded file."""

    stored_path: str
    original_filename: str
    file_size: int


class ConfigPreviewResponse(BaseModel):
    """Response returned after previewing an uploaded workbook's headers."""

    sheet: str
    workbook_type: str
    row_limit: int
    headers: list[str]
    metadata_columns: list[str]
    metadata_collisions: list[str]
    suggested_required_columns: list[str]
    suggested_column_mappings: dict[str, str]


def _resolve_metadata_defaults() -> tuple[str, ...]:
    try:
        from app.normalize_staging import DEFAULT_METADATA_COLUMNS
    except Exception:
        return (
            "raw_id",
            "file_hash",
            "batch_id",
            "source_year",
            "ingested_at",
        )

    return tuple(DEFAULT_METADATA_COLUMNS)


DEFAULT_PREVIEW_SHEET = "TEACH_RECORD"
DEFAULT_PREVIEW_ROWS = 20
MAX_PREVIEW_ROWS = 200


async def _read_preview_dataframe(
    file: UploadFile, *, sheet: str, row_limit: int, rename_last_subject: bool
):
    try:
        from app import prep_excel
    except Exception as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Unable to import header normalizer: {exc}",
        ) from exc

    content = await file.read()
    if not content:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail="Uploaded workbook is empty."
        )

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Workbook contains no default style, apply openpyxl's default",
                category=UserWarning,
                module="openpyxl.styles.stylesheet",
            )
            dataframe = pd.read_excel(
                BytesIO(content), sheet_name=sheet, dtype=str, nrows=row_limit
            )
    except ValueError as exc:
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive catch for IO/format errors
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Unable to read workbook: {exc}",
        ) from exc

    dataframe = prep_excel.normalize_headers_and_subject(
        dataframe, rename_last_subject=rename_last_subject
    )
    return dataframe


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


@app.post("/config/preview", response_model=ConfigPreviewResponse)
async def preview_config(
    workbook: UploadFile = File(...),
    sheet: str = Form(DEFAULT_PREVIEW_SHEET),
    workbook_type: str = Form("default"),
    row_limit: int = Form(DEFAULT_PREVIEW_ROWS),
    rename_last_subject: bool = Form(False),
) -> ConfigPreviewResponse:
    """Read a workbook sample and suggest staging configuration values."""

    normalized_sheet = sheet.strip() or DEFAULT_PREVIEW_SHEET
    normalized_workbook_type = workbook_type.strip() or "default"

    if row_limit <= 0 or row_limit > MAX_PREVIEW_ROWS:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"row_limit must be between 1 and {MAX_PREVIEW_ROWS}",
        )

    dataframe = await _read_preview_dataframe(
        workbook,
        sheet=normalized_sheet,
        row_limit=row_limit,
        rename_last_subject=rename_last_subject,
    )

    headers = [str(header) for header in dataframe.columns]
    metadata_defaults = _resolve_metadata_defaults()
    metadata_set = set(metadata_defaults)
    metadata_collisions = [header for header in headers if header in metadata_set]
    suggested_required = [header for header in headers if header not in metadata_set]
    suggested_mappings = {header: header for header in suggested_required}

    return ConfigPreviewResponse(
        sheet=normalized_sheet,
        workbook_type=normalized_workbook_type,
        row_limit=row_limit,
        headers=headers,
        metadata_columns=list(metadata_defaults),
        metadata_collisions=metadata_collisions,
        suggested_required_columns=suggested_required,
        suggested_column_mappings=suggested_mappings,
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
