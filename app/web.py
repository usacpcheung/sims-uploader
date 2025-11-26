"""HTML UI routes for the SIMS uploader application."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
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

    return templates.TemplateResponse(
        "uploads/index.html",
        {"request": request, "jobs": jobs, "load_error": load_error},
    )
