"""Helpers for managing uploaded workbook file paths."""

from __future__ import annotations

import uuid
from pathlib import Path

from app.config import get_upload_storage_dir

_FILENAME_SEPARATOR = "__"
ALLOWED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}


def _sanitize_filename(filename: str) -> str:
    name = Path(filename).name
    if not name:
        raise ValueError("Filename must not be empty")
    return name


def validate_extension(filename: str) -> None:
    """Raise ``ValueError`` if ``filename`` has an unsupported extension."""

    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        raise ValueError(f"Unsupported file type: {suffix or '<none>'}. Allowed extensions: {allowed}")


def generate_stored_path(original_filename: str, *, storage_dir: Path | None = None) -> Path:
    """Return a unique path within the storage directory for ``original_filename``."""

    storage_root = storage_dir or get_upload_storage_dir()
    sanitized = _sanitize_filename(original_filename)
    unique_name = f"{uuid.uuid4().hex}{_FILENAME_SEPARATOR}{sanitized}"
    return storage_root / unique_name


def get_original_filename(stored_path: str | Path) -> str:
    """Extract the original filename from a stored upload path."""

    name = Path(stored_path).name
    parts = name.split(_FILENAME_SEPARATOR, 1)
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return name
