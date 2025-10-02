"""Utilities for working with SQL identifiers."""
from __future__ import annotations

import re
import unicodedata
from typing import MutableSet

_MAX_IDENTIFIER_LENGTH = 64
_MULTIPLE_UNDERSCORES_RE = re.compile(r"_+")


def _truncate_identifier(identifier: str, suffix: str = "") -> str:
    if suffix and not identifier.endswith(suffix):
        identifier = f"{identifier}{suffix}"
    if len(identifier) <= _MAX_IDENTIFIER_LENGTH:
        return identifier
    if suffix and len(suffix) >= _MAX_IDENTIFIER_LENGTH:
        raise ValueError("Suffix is too long to fit within identifier length limit")
    trimmed_length = _MAX_IDENTIFIER_LENGTH - len(suffix)
    return f"{identifier[:trimmed_length]}{suffix}" if suffix else identifier[:_MAX_IDENTIFIER_LENGTH]


def sanitize_identifier(
    name: object,
    *,
    existing: MutableSet[str] | None = None,
    default: str = "column",
) -> str:
    """Return a MariaDB-safe identifier for ``name``.

    Parameters
    ----------
    name:
        Original value to sanitize. ``None`` or empty values fall back to ``default``.
    existing:
        Set of identifiers that are already in use. When provided, ensures the
        returned identifier is unique by appending a numeric suffix as needed.
    default:
        Replacement used when the sanitized value would otherwise be empty.
    """

    value = "" if name is None else str(name)
    value = unicodedata.normalize("NFKC", value).strip()

    sanitized_chars: list[str] = []
    for char in value:
        category = unicodedata.category(char)
        if category.startswith(("L", "N")):
            sanitized_chars.append(char.lower())
        elif category.startswith(("Z", "P", "S")):
            sanitized_chars.append("_")
        # Control and other categories are skipped entirely.

    value = "".join(sanitized_chars)
    value = _MULTIPLE_UNDERSCORES_RE.sub("_", value)
    value = value.strip("_")

    if not value:
        value = default

    if value[0].isdigit():
        value = f"_{value}"

    value = _truncate_identifier(value)

    if existing is None:
        existing = set()

    candidate = value
    counter = 1
    while candidate in existing:
        suffix = f"_{counter}"
        candidate = _truncate_identifier(value, suffix)
        counter += 1

    existing.add(candidate)
    return candidate


__all__ = ["sanitize_identifier"]
