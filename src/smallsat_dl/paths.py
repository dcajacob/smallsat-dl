"""Output path helpers for the notebook-compatible directory layout."""

from __future__ import annotations

from pathlib import Path

from .titles import sanitize_path_component


def session_dir(output_dir: Path, year: int, session_title: str) -> Path:
    return output_dir / str(year) / sanitize_path_component(session_title)


def paper_path(output_dir: Path, year: int, session_title: str, paper_title: str) -> Path:
    return session_dir(output_dir, year, session_title) / f"{sanitize_path_component(paper_title)}.pdf"


def presentation_path(output_dir: Path, year: int, session_title: str, paper_title: str) -> Path:
    return (
        session_dir(output_dir, year, session_title)
        / "presentations"
        / f"{sanitize_path_component(paper_title.removesuffix('.pdf'))}.pdf"
    )
