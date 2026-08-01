"""Title cleanup and filename sanitization."""

from __future__ import annotations

import re

RESERVED_PATH_CHARS = re.compile(r'[<>:"\\|?*\x00-\x1f]')
WHITESPACE = re.compile(r"\s+")


def clean_title(title: str) -> str:
    """Match the notebook's title cleanup for known awkward characters."""
    return title.replace('"', "").replace("$", "")


def sanitize_path_component(value: str) -> str:
    """Return a safe single path component while preserving notebook conventions."""
    cleaned = clean_title(value).replace("/", "-slash-")
    cleaned = RESERVED_PATH_CHARS.sub("-", cleaned)
    cleaned = WHITESPACE.sub(" ", cleaned).strip()
    cleaned = cleaned.rstrip(". ")
    return cleaned or "untitled"
