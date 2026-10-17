import asyncio
import zipfile
from pathlib import Path

import httpx
import pytest

from smallsat_dl.legacy import reuse_legacy_download
from smallsat_dl.paths import paper_path, presentation_path
from smallsat_dl.scraper import EventDetail, Link, SmallSatDownloader


def presentation(path, *, powerpoint=True):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("ppt/presentation.xml" if powerpoint else "word/document.xml", "content")


@pytest.mark.parametrize("suffix,content", [
    (".png", b"\x89PNG\r\n\x1a\nimage"),
    (".jpg", b"\xff\xd8\xffimage"),
    (".m4v", b"\x00\x00\x00\x20ftypM4V video"),
    (".mp4", b"\x00\x00\x00\x20ftypisomvideo"),
    (".srt", b"1\n00:00:00,000 --> 00:00:01,000\nCaption"),
])
def test_verified_legacy_format_is_reused_without_changing_old_name(tmp_path, suffix, content):
    legacy = tmp_path / "Talk.pdf"
    legacy.write_bytes(content)
    correct = legacy.with_suffix(suffix)
    assert reuse_legacy_download(correct)
    assert correct.read_bytes() == legacy.read_bytes() == content
    assert correct.stat().st_ino == legacy.stat().st_ino
    assert not reuse_legacy_download(correct)


def test_powerpoint_original_survives_replacement_of_legacy_pdf_name(tmp_path):
    legacy = tmp_path / "Talk.pdf"
    presentation(legacy)
    original = legacy.read_bytes()
    correct = legacy.with_suffix(".pptx")
    assert reuse_legacy_download(correct)
    converted = tmp_path / "converted.pdf"
    converted.write_bytes(b"%PDF-1.7\nconverted\n%%EOF")
    converted.replace(legacy)
    assert correct.read_bytes() == original
    assert legacy.read_bytes().startswith(b"%PDF-")


@pytest.mark.parametrize("content", [b"%PDF-existing valid conversion", b"<html>challenge</html>", b"PK broken ZIP"])
def test_unverified_or_different_format_is_not_reused(tmp_path, content):
    legacy = tmp_path / "Talk.pdf"
    legacy.write_bytes(content)
    assert not reuse_legacy_download(legacy.with_suffix(".pptx"))
    assert legacy.read_bytes() == content
    assert list(tmp_path.iterdir()) == [legacy]


def test_word_zip_is_not_mistaken_for_powerpoint(tmp_path):
    legacy = tmp_path / "Talk.pdf"
    presentation(legacy, powerpoint=False)
    assert not reuse_legacy_download(legacy.with_suffix(".pptx"))


def test_dry_run_does_not_create_legacy_alias(tmp_path):
    folder = tmp_path / "2025/Session/presentations"
    folder.mkdir(parents=True)
    legacy = folder / "Talk.pdf"
    presentation(legacy)
    downloader = SmallSatDownloader(client=object(), log=lambda _: None)
    stats, _ = asyncio.run(downloader._download_event_files(
        2025, Link("Session", "https://archive.test/session"),
        EventDetail("Talk", None, "https://archive.test/Slides.pptx"), tmp_path, dry_run=True,
    ))
    assert stats.dry_run == 1
    assert not legacy.with_suffix(".pptx").exists()


def test_legacy_reuse_skips_network_even_after_download_circuit_opens(tmp_path):
    folder = tmp_path / "2025/Session/presentations"
    folder.mkdir(parents=True)
    legacy = folder / "Talk.pdf"
    presentation(legacy)
    ready = []
    downloader = SmallSatDownloader(client=object(), file_ready=ready.append, log=lambda _: None)
    response = httpx.Response(403, request=httpx.Request("GET", "https://archive.test/blocked"))
    downloader._download_circuit_error = httpx.HTTPStatusError("blocked", request=response.request, response=response)
    stats, deferred = asyncio.run(downloader._download_event_files(
        2025, Link("Session", "https://archive.test/session"),
        EventDetail("Talk", None, "https://archive.test/Slides.pptx"), tmp_path, dry_run=False,
    ))
    assert stats.skipped == 1 and stats.downloaded == 0
    assert not deferred and not stats.failures
    assert ready == [legacy.with_suffix(".pptx")]


@pytest.mark.parametrize("suffix", [".pptx", ".png", ".jpg", ".pdf"])
def test_flattened_signed_url_uses_original_file_extension(tmp_path, suffix):
    filename = f"Slides{suffix}_AWSAccessKeyId_public_Signature_expired_Expires_123"
    url = f"https://archive.test/viewcontent/{filename}"
    assert paper_path(tmp_path, 2024, "Session", "Talk", url=url).suffix == suffix
    assert presentation_path(tmp_path, 2024, "Session", "Talk", filename=filename, url=url).suffix == suffix


def test_presentation_uploaded_with_pdf_name_is_preserved_and_queued(tmp_path):
    folder = tmp_path / "2025/Session/presentations"
    folder.mkdir(parents=True)
    wrong_name = folder / "Talk.pdf"
    presentation(wrong_name)
    original = wrong_name.read_bytes()
    ready = []
    downloader = SmallSatDownloader(client=object(), file_ready=ready.append, log=lambda _: None)
    stats, pending = asyncio.run(downloader._download_event_files(
        2025, Link("Session", "https://archive.test/session"),
        EventDetail("Talk", None, "https://archive.test/Incorrectly_Named.pdf"),
        tmp_path, dry_run=False,
    ))
    correct = wrong_name.with_suffix(".pptx")
    assert stats.skipped == 1 and not pending
    assert ready == [correct]
    assert correct.read_bytes() == wrong_name.read_bytes() == original
