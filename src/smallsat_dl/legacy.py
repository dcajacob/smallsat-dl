"""Reuse attachments saved under .pdf names by older downloader versions."""

from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path


def presentation_suffix(path: Path) -> str | None:
    """Recognize presentation packages even when an upload was named .pdf."""
    try:
        with path.open("rb") as stream:
            if stream.read(2) != b"PK":
                return None
        with zipfile.ZipFile(path) as archive:
            if "ppt/presentation.xml" in archive.namelist():
                return ".pptx"
            if archive.getinfo("mimetype").file_size <= 128:
                mime = archive.read("mimetype").strip()
                if mime == b"application/vnd.oasis.opendocument.presentation":
                    return ".odp"
    except (OSError, ValueError, KeyError, zipfile.BadZipFile):
        pass
    return None


def _matches_format(path: Path, suffix: str) -> bool:
    with path.open("rb") as stream:
        header = stream.read(4096)
    if suffix in {".pptx", ".pptm", ".ppsx", ".ppsm", ".potx", ".potm"}:
        if not header.startswith(b"PK"):
            return False
        with zipfile.ZipFile(path) as archive:
            return "ppt/presentation.xml" in archive.namelist()
    if suffix in {".odp", ".otp"}:
        if not header.startswith(b"PK"):
            return False
        with zipfile.ZipFile(path) as archive:
            expected = b"application/vnd.oasis.opendocument.presentation"
            if suffix == ".otp":
                expected += b"-template"
            if archive.getinfo("mimetype").file_size > 128:
                return False
            return archive.read("mimetype").strip() == expected
    if suffix in {".mp4", ".m4v", ".mov"}:
        return len(header) >= 12 and header[4:8] == b"ftyp"
    if suffix in {".jpg", ".jpeg"}:
        return header.startswith(b"\xff\xd8\xff")
    if suffix == ".png":
        return header.startswith(b"\x89PNG\r\n\x1a\n")
    if suffix == ".gif":
        return header.startswith((b"GIF87a", b"GIF89a"))
    if suffix == ".webp":
        return header.startswith(b"RIFF") and header[8:12] == b"WEBP"
    if suffix in {".tif", ".tiff"}:
        return header.startswith((b"II*\x00", b"MM\x00*"))
    if suffix == ".bmp":
        return header.startswith(b"BM")
    if suffix == ".vtt":
        return header.removeprefix(b"\xef\xbb\xbf").startswith(b"WEBVTT")
    if suffix == ".srt":
        return re.search(rb"\d\d:\d\d:\d\d[,\.]\d{3}\s+-->\s+\d\d:\d\d:\d\d", header) is not None
    return False


def reuse_legacy_download(destination: Path) -> bool:
    """Add a correctly named hard link only when the legacy format matches.

    The old name remains intact. A later PDF conversion replaces that name
    atomically while the original presentation survives under its proper name.
    Unknown formats are left alone and use the normal download path.
    """
    if destination.suffix.lower() == ".pdf" or destination.exists() or destination.is_symlink():
        return False
    legacy = destination.with_suffix(".pdf")
    if legacy.is_symlink() or not legacy.is_file():
        return False
    try:
        if not _matches_format(legacy, destination.suffix.lower()):
            return False
        os.link(legacy, destination, follow_symlinks=False)
        return True
    except (OSError, ValueError, KeyError, zipfile.BadZipFile):
        return False
