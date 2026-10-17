import asyncio
from pathlib import Path

import pytest

import smallsat_dl.cli as cli
import smallsat_dl.scraper as scraper
from smallsat_dl.scraper import DownloadFailure, DownloadJob, DownloadStats, SmallSatDownloader


PDF_CONTENT = b"%PDF-1.7\ncomplete paper content\n%%EOF"


class DownloadClient:
    async def get_bytes(self, url: str) -> bytes:
        return PDF_CONTENT


def make_downloader(file_ready) -> SmallSatDownloader:
    return SmallSatDownloader(
        client=DownloadClient(),
        delay=0,
        downloads_per_minute=1_000_000,
        retry_rounds=0,
        log=lambda _: None,
        file_ready=file_ready,
    )


@pytest.mark.parametrize("retry_phase", [False, True])
def test_file_ready_runs_after_complete_file_is_published(tmp_path: Path, retry_phase: bool):
    destination = tmp_path / "paper.pdf"
    ready: list[Path] = []

    def file_ready(path: Path) -> None:
        assert isinstance(path, Path)
        assert path.read_bytes() == PDF_CONTENT
        assert list(tmp_path.iterdir()) == [destination]
        ready.append(path)

    downloader = make_downloader(file_ready)
    stats = DownloadStats()

    asyncio.run(downloader._download_once(
        DownloadJob("https://example.test/paper.pdf", destination),
        stats, dry_run=False, retry_phase=retry_phase,
    ))

    assert ready == [destination]
    assert stats.downloaded == 1


def test_file_ready_includes_existing_skipped_files(tmp_path: Path):
    destination = tmp_path / "slides.pptx"
    destination.write_bytes(b"existing slides")
    ready: list[Path] = []
    downloader = make_downloader(ready.append)
    stats = DownloadStats()

    asyncio.run(downloader._download_once(
        DownloadJob("https://example.test/slides.pptx", destination),
        stats, dry_run=False, retry_phase=False,
    ))

    assert ready == [destination]
    assert destination.read_bytes() == b"existing slides"
    assert stats.skipped == 1
    assert stats.downloaded == 0


def test_failed_write_never_calls_file_ready(monkeypatch, tmp_path: Path):
    destination = tmp_path / "paper.pdf"
    ready: list[Path] = []

    def fail_write(path: Path, content: bytes) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(scraper, "_write_bytes_atomic", fail_write)
    downloader = make_downloader(ready.append)
    stats = DownloadStats()

    with pytest.raises(OSError, match="disk full"):
        asyncio.run(downloader._download_once(
            DownloadJob("https://example.test/paper.pdf", destination),
            stats, dry_run=False, retry_phase=False,
        ))

    assert ready == []
    assert not destination.exists()
    assert stats.downloaded == 0


@pytest.mark.parametrize("existing", [False, True])
def test_dry_run_never_calls_file_ready(tmp_path: Path, existing: bool):
    destination = tmp_path / "slides.pptx"
    if existing:
        destination.write_bytes(b"existing slides")
    ready: list[Path] = []
    downloader = make_downloader(ready.append)
    stats = DownloadStats()

    asyncio.run(downloader._download_once(
        DownloadJob("https://example.test/slides.pptx", destination),
        stats, dry_run=True, retry_phase=False,
    ))

    assert ready == []
    assert stats.downloaded == 0
    assert stats.skipped == int(existing)
    assert stats.dry_run == int(not existing)


def test_conversion_options_default_on_and_allow_one_or_two_workers():
    parser = cli.build_parser()
    defaults = parser.parse_args([])

    assert defaults.no_convert is False
    assert defaults.conversion_workers == 1
    assert parser.parse_args(["--no-convert"]).no_convert is True
    assert parser.parse_args(["--conversion-workers", "2"]).conversion_workers == 2
    with pytest.raises(SystemExit):
        parser.parse_args(["--conversion-workers", "3"])


def install_fake_pipeline(monkeypatch, tmp_path: Path, *, download_failed: bool = False,
                          conversion_failed: bool = False, wait_for_release: bool = False):
    from smallsat_dl.conversion import ConversionFailure, ConversionStats

    events: list[str] = []
    queues = []
    downloader_options = []
    source = tmp_path / "slides.pptx"
    finish_started = asyncio.Event()
    release_finish = asyncio.Event()

    class FakeQueue:
        def __init__(self, *, workers=1, **kwargs):
            self.workers = workers
            self.sources: list[Path] = []
            queues.append(self)

        def submit(self, path: Path) -> None:
            self.sources.append(path)
            events.append("submit")

        async def finish(self):
            events.append("finish_started")
            finish_started.set()
            if wait_for_release:
                await release_finish.wait()
            events.append("finish_complete")
            return ConversionStats(
                converted=0 if conversion_failed else 1,
                skipped=0,
                failures=[ConversionFailure(source, "Office conversion failed")]
                if conversion_failed else [],
            )

        async def close(self):
            events.append("queue_closed")

    class FakeDownloader:
        def __init__(self, **kwargs):
            downloader_options.append(kwargs)
            self.file_ready = kwargs.get("file_ready")

        async def discover_latest_year(self):
            return 2025

        async def download_years(self, years, output_dir, *, dry_run=False):
            events.append("download_started")
            if self.file_ready is not None and not dry_run:
                self.file_ready(source)
            events.append("download_complete")
            return DownloadStats(
                file_targets=1 + int(download_failed),
                downloaded=int(not dry_run),
                dry_run=int(dry_run),
                failures=[DownloadFailure("https://example.test/paper", "download failed")]
                if download_failed else [],
            )

        async def close(self):
            events.append("downloader_closed")

    monkeypatch.setattr(cli, "PresentationConversionQueue", FakeQueue)
    monkeypatch.setattr(cli, "SmallSatDownloader", FakeDownloader)
    return events, queues, downloader_options, source, finish_started, release_finish


@pytest.mark.parametrize("workers", [1, 2])
def test_cli_queues_conversion_by_default_and_awaits_finish(monkeypatch, tmp_path: Path,
                                                         capsys, workers: int):
    async def run():
        events, queues, options, source, finish_started, release_finish = install_fake_pipeline(
            monkeypatch, tmp_path, wait_for_release=True,
        )
        task = asyncio.create_task(cli._async_main([
            "--output", str(tmp_path), "--conversion-workers", str(workers),
        ]))
        started = asyncio.create_task(finish_started.wait())
        done, _ = await asyncio.wait([task, started], return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            started.cancel()
            await task
            pytest.fail("CLI returned before waiting for conversion completion")
        assert "download_complete" in events
        assert not task.done()
        release_finish.set()
        result = await task
        assert result == 0
        assert len(queues) == 1
        assert queues[0].workers == workers
        assert queues[0].sources == [source]
        assert callable(options[0]["file_ready"])
        assert events.index("download_complete") < events.index("finish_started")
        assert events.index("finish_complete") < events.index("queue_closed")
        assert "downloader_closed" in events

    asyncio.run(run())

    output = " ".join(capsys.readouterr().out.split()).lower()
    assert "conversion" in output
    assert "1 converted" in output
    assert "0 already available" in output


def test_no_convert_disables_queue_and_file_ready_callback(monkeypatch, tmp_path: Path):
    events, queues, options, *_ = install_fake_pipeline(monkeypatch, tmp_path)

    result = asyncio.run(cli._async_main(["--output", str(tmp_path), "--no-convert"]))

    assert result == 0
    assert queues == []
    assert options[0].get("file_ready") is None
    assert "finish_started" not in events
    assert "downloader_closed" in events


def test_cli_dry_run_does_not_start_conversion_queue(monkeypatch, tmp_path: Path):
    events, queues, options, *_ = install_fake_pipeline(monkeypatch, tmp_path)

    result = asyncio.run(cli._async_main(["--output", str(tmp_path), "--dry-run"]))

    assert result == 0
    assert queues == []
    assert options[0].get("file_ready") is None
    assert "finish_started" not in events


@pytest.mark.parametrize(
    ("download_failed", "conversion_failed"), [(True, False), (False, True), (True, True)],
)
def test_conversion_preserves_download_failures_and_reports_own_failures(
    monkeypatch, tmp_path: Path, capsys, download_failed: bool, conversion_failed: bool,
):
    events, queues, *_ = install_fake_pipeline(
        monkeypatch, tmp_path, download_failed=download_failed,
        conversion_failed=conversion_failed,
    )

    result = asyncio.run(cli._async_main(["--output", str(tmp_path)]))

    output = capsys.readouterr()
    combined = " ".join((output.out + output.err).split()).lower()
    assert result == 1
    assert len(queues) == 1
    assert "finish_complete" in events
    assert "queue_closed" in events
    assert "conversion" in combined
    if download_failed:
        assert "download failed" in combined
    if conversion_failed:
        assert "office conversion failed" in combined
        assert "slides.pptx" in combined
        assert "1 failure" in combined
