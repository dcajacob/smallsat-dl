"""Output path helpers for the notebook-compatible directory layout."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote, urlsplit

from .titles import sanitize_path_component


def session_dir(output_dir: Path, year: int, session_title: str) -> Path:
    return output_dir / str(year) / sanitize_path_component(session_title)


def paper_path(
    output_dir: Path, year: int, session_title: str, paper_title: str, *, url: str | None = None,
) -> Path:
    # Some primary records are videos rather than papers. Opaque legacy PDF
    # endpoints keep their existing .pdf paths.
    suffix = _file_suffix(unquote(urlsplit(url or "").path)) or ".pdf"
    return session_dir(output_dir, year, session_title) / f"{sanitize_path_component(paper_title)}{suffix}"


def _file_suffix(value: str) -> str:
    # Digital Commons sometimes preserves a flattened, expired signed-URL
    # query as part of the uploaded filename, after the real extension.
    if "_AWSAccessKeyId_" in value and "_Signature_" in value and "_Expires_" in value:
        value = value.split("_AWSAccessKeyId_", 1)[0]
    suffix = Path(value).suffix.lower()
    if (
        not suffix
        or suffix in {".cgi", ".php", ".asp", ".aspx", ".jsp"}
        or not suffix[1:].isascii()
        or not suffix[1:].isalnum()
    ):
        return ""
    return suffix


def presentation_path(
    output_dir: Path,
    year: int,
    session_title: str,
    paper_title: str,
    *,
    filename: str | None = None,
    url: str | None = None,
) -> Path:
    """Use the attachment's filename, then its URL, to preserve its format."""
    # An opaque download endpoint does not tell us that the attachment is a PDF.
    suffix = (
        _file_suffix(filename or "")
        or _file_suffix(unquote(urlsplit(url or "").path))
        or ".bin"
    )
    return (
        session_dir(output_dir, year, session_title)
        / "presentations"
        / f"{sanitize_path_component(paper_title.removesuffix('.pdf'))}{suffix}"
    )
