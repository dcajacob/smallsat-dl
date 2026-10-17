import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from smallsat_dl.oai import OaiRecord
from smallsat_dl.scraper import HttpClient, Link, SmallSatDownloader, parse_event_detail
from smallsat_dl.transports import CurlCffiClient, write_stream_atomic


PDF = b"%PDF-1.7\nfirst attachment\n%%EOF"
SECOND = b"%PDF-1.7\nsecond attachment\n%%EOF"
VIDEO = b"\x00\x00\x00\x20ftypisomvideo contents"
SESSION = Link("Session", "https://archive.test/session/")


class Files:
    def __init__(self, content):
        self.content = content
        self.calls = []

    async def get_bytes(self, url):
        self.calls.append(url)
        return self.content[url]


def test_html_downloads_every_attachment_without_same_extension_collisions(tmp_path):
    detail = parse_event_detail(
        '<div class="files"><a href="first" download="slides.pdf">Download</a>'
        '<a href="second" download="slides.pdf">Download</a>'
        '<a href="first" download="slides.pdf">Duplicate link</a></div>',
        "https://archive.test/", "Talk",
    )
    client = Files({"https://archive.test/first": PDF, "https://archive.test/second": SECOND})
    downloader = SmallSatDownloader(client=client, downloads_per_minute=1_000_000, log=lambda _: None)
    first, deferred = asyncio.run(downloader._download_event_files(
        2026, SESSION, detail, tmp_path, dry_run=False,
    ))
    assert first.downloaded == first.file_targets == 2
    assert not deferred and not first.failures
    folder = tmp_path / "2026/Session/presentations"
    assert (folder / "Talk.pdf").read_bytes() == PDF
    assert (folder / "Talk - Attachment 2.pdf").read_bytes() == SECOND
    assert len(client.calls) == 2


def test_oai_preserves_video_format_and_all_supplemental_files(tmp_path):
    primary = "https://archive.test/context/smallsat/article/1/viewcontent/keynote.mp4"
    slides = "https://archive.test/slides.pdf"
    transcript = "https://archive.test/transcript.pdf"
    client = Files({primary: VIDEO, slides: PDF, transcript: SECOND})
    downloader = SmallSatDownloader(client=client, downloads_per_minute=1_000_000, log=lambda _: None)
    event = Link("Keynote", "https://archive.test/smallsat/2026/all2026/1")
    downloader._oai_records[2026] = [OaiRecord(
        identifier="oai:archive.test:smallsat-1", title="Keynote", event_url=event.url,
        year=2026, primary_url=primary, supplemental_urls=(slides, transcript), formats=("video/mp4",),
    )]
    detail = downloader._oai_detail(event)

    async def run():
        first, pending = await downloader._download_event_files(2026, SESSION, detail, tmp_path, dry_run=False)
        second, _ = await downloader._download_event_files(2026, SESSION, detail, tmp_path, dry_run=False)
        return first, second, pending

    first, resumed, pending = asyncio.run(run())
    folder = tmp_path / "2026/Session"
    assert (folder / "Keynote.mp4").read_bytes() == VIDEO
    assert not (folder / "Keynote.pdf").exists()
    assert (folder / "presentations/Keynote.pdf").read_bytes() == PDF
    assert (folder / "presentations/Keynote - Attachment 2.pdf").read_bytes() == SECOND
    assert first.downloaded == first.file_targets == resumed.skipped == 3
    assert len(client.calls) == 3
    assert not pending and not first.failures


@pytest.mark.parametrize("fail", [False, True])
def test_streaming_is_atomic_and_cleans_up_interrupted_downloads(tmp_path, fail):
    destination = tmp_path / "large.mp4"

    async def chunks():
        yield VIDEO[:12]
        assert not destination.exists()
        assert len(list(tmp_path.glob(".smallsat-*.part"))) == 1
        if fail:
            raise httpx.ReadError("stream interrupted")
        yield VIDEO[12:]

    if fail:
        with pytest.raises(httpx.ReadError):
            asyncio.run(write_stream_atomic(destination, chunks()))
        assert list(tmp_path.iterdir()) == []
    else:
        count = asyncio.run(write_stream_atomic(destination, chunks()))
        assert count == len(VIDEO)
        assert destination.read_bytes() == VIDEO
        assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("status,body,content_type", [
    (403, b"blocked", "text/html"), (429, b"blocked", "text/html"),
    (200, b"<html>challenge</html>", "text/html"),
    (200, b"<html>challenge</html>", "application/octet-stream"),
])
def test_httpx_stream_never_publishes_block_or_challenge(tmp_path, status, body, content_type):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(status, content=body, headers={"content-type": content_type}),
        )) as raw:
            client = HttpClient(client=raw, transport="httpx")
            with pytest.raises((httpx.HTTPStatusError, RuntimeError)):
                await client.download_to("https://archive.test/file.mp4", tmp_path / "video.mp4")

    asyncio.run(run())
    assert list(tmp_path.iterdir()) == []


def test_curl_streams_without_accessing_buffered_content_and_closes(tmp_path):
    closed = []
    requests = []

    class Response:
        status_code = 200
        headers = {"content-type": "video/mp4"}
        url = "https://archive.test/large.mp4"

        @property
        def content(self):
            raise AssertionError("large file must not be buffered")

        async def aiter_content(self):
            yield VIDEO[:12]
            yield VIDEO[12:]

        async def aclose(self):
            closed.append(True)

    async def get(url, **kwargs):
        requests.append(kwargs)
        return Response()

    client = object.__new__(CurlCffiClient)
    client._session = SimpleNamespace(get=get)
    destination = tmp_path / "video.mp4"
    count = asyncio.run(client.download_to(Response.url, destination))
    assert count == len(VIDEO)
    assert destination.read_bytes() == VIDEO
    assert requests == [{"allow_redirects": True, "stream": True}]
    assert closed == [True]


def test_streaming_curl_keeps_http_errors_and_closes_without_reading_body(tmp_path):
    closed = []

    async def aclose():
        closed.append(True)

    response = SimpleNamespace(status_code=403, headers={"retry-after": "60"},
                               url="https://archive.test/video.mp4", aclose=aclose)

    async def get(*args, **kwargs):
        return response

    client = object.__new__(CurlCffiClient)
    client._session = SimpleNamespace(get=get)
    with pytest.raises(httpx.HTTPStatusError) as raised:
        asyncio.run(client.download_to(response.url, tmp_path / "video.mp4"))
    assert raised.value.response.status_code == 403
    assert closed == [True]
    assert list(tmp_path.iterdir()) == []
