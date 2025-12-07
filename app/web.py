"""HTML UI routes for the SIMS uploader application."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app import job_store

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(prefix="/ui", tags=["ui"])


@router.get("/", response_class=HTMLResponse)
async def render_home(request: Request) -> HTMLResponse:
    """Render the landing page for the uploader UI."""

    return templates.TemplateResponse("home.html", {"request": request})


@router.get("/uploads/new", response_class=HTMLResponse)
async def render_new_upload_form(request: Request) -> HTMLResponse:
    """Render the form for enqueuing a new upload job."""

    return templates.TemplateResponse("uploads/new.html", {"request": request})


@router.get("/uploads", response_class=HTMLResponse)
async def render_uploads_list(request: Request) -> HTMLResponse:
    """Render a listing of recent upload jobs."""

    load_error = False
    try:
        jobs = job_store.list_recent_jobs(limit=20)
    except Exception:
        jobs = []
        load_error = True

    def _serialize_job(job: job_store.UploadJob) -> dict[str, str | None]:
        def _format_dt(value):
            return value.isoformat() if value else None

        return {
            "job_id": job.job_id,
            "status": job.status,
            "latest_message": job.latest_message,
            "processed_rows": job.processed_rows,
            "successful_rows": job.successful_rows,
            "rejected_rows": job.rejected_rows,
            "normalized_table_name": job.normalized_table_name,
            "created_at": _format_dt(job.created_at),
            "updated_at": _format_dt(job.updated_at),
        }

    initial_jobs = [_serialize_job(job) for job in jobs]

    return templates.TemplateResponse(
        "uploads/index.html",
        {
            "request": request,
            "jobs": jobs,
            "initial_jobs": initial_jobs,
            "load_error": load_error,
        },
    )


@router.get("/uploads/{job_id}", response_class=HTMLResponse)
async def render_upload_detail(request: Request, job_id: str) -> HTMLResponse:
    """Render details and event history for a single upload job."""

    not_found_message: str | None = None
    status_code = 200

    try:
        job_store.get_job(job_id)
    except (HTTPException, ValueError):
        not_found_message = (
            "This upload job no longer exists. You can return to uploads to start a new one."
        )
        status_code = 404

    return templates.TemplateResponse(
        "uploads/detail.html",
        {
            "request": request,
            "job_id": job_id,
            "not_found_message": not_found_message,
        },
        status_code=status_code,
    )
