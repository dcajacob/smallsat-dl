import asyncio
import tempfile
from pathlib import Path

import httpx

from smallsat_dl.paths import paper_path
from smallsat_dl.scraper import (
    DeferredDownload,
    DownloadJob,
    DownloadStats,
    EventDetail,
    Link,
    SmallSatDownloader,
)


PDF_CONTENT = b"%PDF-1.7\ncomplete paper content\n%%EOF"
PAPER_URL = "https://example.test/paper.pdf"
SESSION = Link("Session", "https://example.test/session")


class DownloadClient:
    def __init__(self, *, forbidden: bool = False) -> None:
        self.forbidden = forbidden
        self.calls: list[str] = []

    async def get_bytes(self, url: str) -> bytes:
        self.calls.append(url)
        if self.forbidden:
            response = httpx.Response(403, request=httpx.Request("GET", url))
            response.raise_for_status()
        return PDF_CONTENT


def make_downloader(client: DownloadClient) -> SmallSatDownloader:
    return SmallSatDownloader(
        client=client,
        delay=0,
        downloads_per_minute=1_000_000,
        retry_rounds=2,
        retry_backoff=0,
        log=lambda _: None,
    )


def test_partial_write_is_removed_and_next_run_downloads_again(monkeypatch, tmp_path: Path):
    client = DownloadClient()
    downloader = make_downloader(client)
    detail = EventDetail("Paper", PAPER_URL, None)
    destination = paper_path(tmp_path, 2025, SESSION.title, detail.title)
    create_temporary = tempfile.NamedTemporaryFile

    def failing_temporary(*args, **kwargs):
        temporary = create_temporary(*args, **kwargs)
        write = temporary.write

        def partial_write(content):
            write(content[:6])
            temporary.flush()
            assert Path(temporary.name).read_bytes() == content[:6]
            assert not destination.exists()
            raise OSError("disk full during file write")

        temporary.write = partial_write
        return temporary

    async def run():
        with monkeypatch.context() as patch:
            patch.setattr("smallsat_dl.scraper.tempfile.NamedTemporaryFile", failing_temporary)
            failed, deferred = await downloader._download_event_files(
                2025, SESSION, detail, tmp_path, dry_run=False
            )

        assert len(failed.failures) == 1
        assert failed.downloaded == 0
        assert failed.first_pass_failures == 1
        assert not deferred
        assert not destination.exists()
        assert list(destination.parent.iterdir()) == []
        return await downloader._download_event_files(
            2025, SESSION, detail, tmp_path, dry_run=False
        )

    recovered, deferred = asyncio.run(run())

    assert destination.read_bytes() == PDF_CONTENT
    assert client.calls == [PAPER_URL, PAPER_URL]
    assert recovered.downloaded == 1
    assert recovered.first_pass_downloaded == 1
    assert recovered.skipped == 0
    assert not recovered.failures
    assert not deferred
    assert list(destination.parent.iterdir()) == [destination]


def test_complete_file_is_published_only_after_temporary_write(monkeypatch, tmp_path: Path):
    client = DownloadClient()
    downloader = make_downloader(client)
    destination = tmp_path / "paper.pdf"
    create_temporary = tempfile.NamedTemporaryFile
    observed_temporary: list[Path] = []

    def inspecting_temporary(*args, **kwargs):
        temporary = create_temporary(*args, **kwargs)
        write = temporary.write

        def inspect_write(content):
            result = write(content)
            temporary.flush()
            temporary_path = Path(temporary.name)
            assert temporary_path.parent == destination.parent
            assert temporary_path.read_bytes() == PDF_CONTENT
            assert not destination.exists()
            observed_temporary.append(temporary_path)
            return result

        temporary.write = inspect_write
        return temporary

    monkeypatch.setattr("smallsat_dl.scraper.tempfile.NamedTemporaryFile", inspecting_temporary)
    stats = DownloadStats()
    asyncio.run(
        downloader._download_once(
            DownloadJob(PAPER_URL, destination), stats, dry_run=False, retry_phase=False
        )
    )

    assert destination.read_bytes() == PDF_CONTENT
    assert len(observed_temporary) == 1
    assert not observed_temporary[0].exists()
    assert list(tmp_path.iterdir()) == [destination]
    assert stats.downloaded == 1
    assert stats.first_pass_downloaded == 1


def test_first_pass_skips_existing_files_after_persistent_403(tmp_path: Path):
    client = DownloadClient(forbidden=True)
    downloader = make_downloader(client)
    missing_detail = EventDetail("Missing", PAPER_URL, None)
    existing_detail = EventDetail("Existing", "https://example.test/existing.pdf", None)
    existing = paper_path(tmp_path, 2025, SESSION.title, existing_detail.title)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(PDF_CONTENT)

    async def run():
        stats, deferred = await downloader._download_event_files(
            2025, SESSION, missing_detail, tmp_path, dry_run=False
        )
        existing_stats, existing_deferred = await downloader._download_event_files(
            2025, SESSION, existing_detail, tmp_path, dry_run=False
        )
        stats.extend(existing_stats)
        deferred.extend(existing_deferred)
        assert len(deferred) == 1
        await downloader._retry_deferred_downloads(deferred, stats)
        return stats

    stats = asyncio.run(run())

    assert existing.read_bytes() == PDF_CONTENT
    assert client.calls == [PAPER_URL] * 3
    assert stats.file_targets == 2
    assert stats.skipped == 1
    assert stats.first_pass_deferred == 1
    assert stats.retry_attempts == 2
    assert stats.retry_exhausted == 1
    assert stats.downloaded == 0
    assert [failure.url for failure in stats.failures] == [PAPER_URL]


def test_retry_pass_skips_existing_files_after_persistent_403(tmp_path: Path):
    client = DownloadClient(forbidden=True)
    downloader = make_downloader(client)
    existing = tmp_path / "existing.pdf"
    existing.write_bytes(PDF_CONTENT)
    initial_response = httpx.Response(403, request=httpx.Request("GET", PAPER_URL))
    initial_error = httpx.HTTPStatusError(
        "forbidden", request=initial_response.request, response=initial_response
    )
    deferred = [
        DeferredDownload(DownloadJob(PAPER_URL, tmp_path / "missing.pdf"), initial_error),
        DeferredDownload(DownloadJob("https://example.test/existing.pdf", existing), initial_error),
    ]
    stats = DownloadStats(file_targets=2, first_pass_deferred=2)

    asyncio.run(downloader._retry_deferred_downloads(deferred, stats))

    assert existing.read_bytes() == PDF_CONTENT
    assert client.calls == [PAPER_URL] * 2
    assert stats.skipped == 1
    assert stats.retry_attempts == 2
    assert stats.retry_exhausted == 1
    assert stats.retry_recovered == 0
    assert stats.downloaded == 0
    assert [failure.url for failure in stats.failures] == [PAPER_URL]
