"""Centralized configuration helpers for environment-driven settings."""
from __future__ import annotations

import os
from typing import Dict

from dotenv import load_dotenv

_REQUIRED_DB_KEYS = ("DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_CHARSET")
_ENV_LOADED = False


def load_environment(dotenv_path: str | None = None) -> None:
    """Load environment variables from a .env file once per process."""
    global _ENV_LOADED
    if not _ENV_LOADED:
        load_dotenv(dotenv_path=dotenv_path)
        _ENV_LOADED = True


def get_db_settings() -> Dict[str, str]:
    """Return database connection settings from the environment.

    Raises:
        RuntimeError: If any required database environment variables are missing.
    """
    load_environment()
    missing = [key for key in _REQUIRED_DB_KEYS if not os.getenv(key)]
    if missing:
        missing_list = ", ".join(missing)
        raise RuntimeError(
            "Missing required database configuration environment variable(s): "
            f"{missing_list}. Copy .env.example to .env and provide values before running the ingestion tools."
        )

    return {
        "host": os.environ["DB_HOST"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "database": os.environ["DB_NAME"],
        "charset": os.environ["DB_CHARSET"],
    }
