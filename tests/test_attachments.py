import asyncio
from pathlib import Path

import httpx
import pytest

from smallsat_dl.paths import presentation_path
from smallsat_dl.scraper import HttpClient, Link, SmallSatDownloader, parse_event_detail


@pytest.mark.parametrize(
    ("filename", "content", "suffix"),
    [
        ("Mission Slides.PPTX", b"PK\x03\x04powerpoint", ".pptx"),
        ("Keynote_Captions.srt", b"1\n00:00:00,000 --> 00:00:01,000\nHello", ".srt"),
        ("Poster.png", b"\x89PNG\r\n\x1a\nposter", ".png"),
        ("Poster.jpeg", b"\xff\xd8\xffposter", ".jpeg"),
        ("Keynote.m4v", b"\x00\x00\x00\x20ftypM4V ", ".m4v"),
        ("Slides.pdf", b"%PDF-1.7\nslides\n%%EOF", ".pdf"),
    ],
)
def test_attachment_format_is_preserved_when_downloading_and_resuming(
    tmp_path: Path, filename: str, content: bytes, suffix: str
):
    url = "https://example.test/cgi/viewcontent.cgi?filename=0&type=additional"
    detail = parse_event_detail(
        f'<div class="files"><a href="{url}">{filename}</a></div>',
        "https://example.test/event/",
        "Mission",
    )
    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, content=content, headers={"content-type": "application/octet-stream"})

    async def download_twice():
        client = HttpClient(client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
        downloader = SmallSatDownloader(client=client, delay=0, log=lambda _: None)
        try:
            first, deferred = await downloader._download_event_files(
                2025, Link("Session", "https://example.test/session/"), detail, tmp_path,
                dry_run=False,
            )
            second, _ = await downloader._download_event_files(
                2025, Link("Session", "https://example.test/session/"), detail, tmp_path,
                dry_run=False,
            )
            return first, second, deferred
        finally:
            await client.close()

    first, second, deferred = asyncio.run(download_twice())

    destination = tmp_path / "2025" / "Session" / "presentations" / f"Mission{suffix}"
    assert detail.presentation_filename == filename
    assert destination.read_bytes() == content
    assert first.downloaded == 1
    assert not first.failures
    assert not deferred
    assert second.skipped == 1
    assert calls == [url]
    if suffix != ".pdf":
        assert not destination.with_suffix(".pdf").exists()


@pytest.mark.parametrize(
    ("filename", "url", "expected_suffix"),
    [
        (None, "https://example.test/poster.PNG?download=1", ".png"),
        ("Slides", "https://example.test/slides.pptx", ".pptx"),
        ("slides.pptx (3 MB)", "https://example.test/slides.pptx", ".pptx"),
        ("viewcontent.cgi", "https://example.test/slides.pptx", ".pptx"),
        (None, "https://example.test/viewcontent.cgi?filename=slides.pdf", ".bin"),
        ("Download", "https://example.test/download", ".bin"),
        ("../../remote-file.srt", "https://example.test/download", ".srt"),
    ],
)
def test_attachment_paths_use_filename_or_url_without_guessing_pdf(
    tmp_path, filename, url, expected_suffix
):
    assert presentation_path(
        tmp_path, 2025, "Session", "Mission.pdf", filename=filename, url=url,
    ) == tmp_path / "2025" / "Session" / "presentations" / f"Mission{expected_suffix}"


def test_attachment_download_attribute_supplies_original_filename():
    detail = parse_event_detail(
        '<div class="files"><a href="/download" download="slides.pptx">Slides</a></div>',
        "https://example.test/event/", "Mission",
    )

    assert detail.presentation_filename == "slides.pptx"
