"""HTML UI routes for the SIMS uploader application."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(prefix="/ui", tags=["ui"])


@router.get("/", response_class=HTMLResponse)
async def render_home(request: Request) -> HTMLResponse:
    """Render the landing page for the uploader UI."""

    return templates.TemplateResponse("base.html", {"request": request})
