import asyncio
import signal
from pathlib import Path

import pytest

from smallsat_dl.conversion import PRESENTATION_SUFFIXES, PresentationConversionQueue


PDF_CONTENT = b"%PDF-1.7\nconverted slides\n%%EOF"


class FakeProcess:
    def __init__(self, factory, source: Path, output: Path, mode: str) -> None:
        self.factory = factory
        self.source = source
        self.output = output
        self.mode = mode
        self.pid = 1000 + len(factory.processes)
        self.returncode = None
        self.exited = asyncio.Event()
        self.signals = []

    async def communicate(self):
        self.factory.active += 1
        self.factory.max_active = max(self.factory.max_active, self.factory.active)
        self.factory.started.put_nowait(self)
        try:
            await self.factory.gate.wait()
            if self.mode == "hang":
                await asyncio.Event().wait()
            if self.mode not in {"missing", "nonzero"}:
                content = {"invalid": b"not a PDF", "truncated": b"%PDF-1.7\nincomplete"}.get(
                    self.mode, PDF_CONTENT
                )
                self.output.write_bytes(content)
            self.returncode = 7 if self.mode == "nonzero" else 0
            self.exited.set()
            return b"converted", b"unsupported input" if self.mode == "nonzero" else b""
        finally:
            self.factory.active -= 1

    async def wait(self):
        await self.exited.wait()
        return self.returncode

    def stop(self, value):
        self.signals.append(value)
        self.returncode = -int(value)
        self.exited.set()


class FakeConverter:
    def __init__(self, modes=None, *, blocked=False) -> None:
        self.modes = modes or {}
        self.calls = []
        self.processes = []
        self.started = asyncio.Queue()
        self.gate = asyncio.Event()
        if not blocked:
            self.gate.set()
        self.active = 0
        self.max_active = 0

    async def spawn(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        source = Path(args[-1])
        output_dir = Path(args[args.index("--outdir") + 1])
        process = FakeProcess(
            self, source, output_dir / source.with_suffix(".pdf").name,
            self.modes.get(source.name, "success"),
        )
        self.processes.append(process)
        return process

    def killpg(self, pid, value):
        next(process for process in self.processes if process.pid == pid).stop(value)


def install_converter(monkeypatch, converter):
    monkeypatch.setattr("smallsat_dl.conversion.asyncio.create_subprocess_exec", converter.spawn)
    monkeypatch.setattr("smallsat_dl.conversion.os.killpg", converter.killpg)


def presentation(tmp_path: Path, name="deck.pptx") -> Path:
    source = tmp_path / name
    source.write_bytes(b"original presentation")
    return source


@pytest.mark.parametrize("workers", [1, 2])
def test_queue_is_nonblocking_and_bounds_concurrent_conversions(monkeypatch, tmp_path, workers):
    converter = FakeConverter(blocked=True)
    install_converter(monkeypatch, converter)
    sources = [presentation(tmp_path, f"deck{index}.pptx") for index in range(5)]

    async def run():
        queue = PresentationConversionQueue(workers=workers, executable="fake-office", log=lambda _: None)
        for source in sources:
            queue.submit(source)
        assert not converter.calls
        for _ in range(workers):
            await asyncio.wait_for(converter.started.get(), timeout=1)
        assert converter.active == workers
        assert len(converter.calls) == workers

        async def download_keeps_running():
            await asyncio.sleep(0)
            return "downloaded"

        assert await asyncio.wait_for(download_keeps_running(), timeout=1) == "downloaded"
        finishing = asyncio.create_task(queue.finish())
        await asyncio.sleep(0)
        assert not finishing.done()
        converter.gate.set()
        return await finishing

    stats = asyncio.run(run())

    assert stats.converted == 5
    assert stats.skipped == 0
    assert not stats.failures
    assert converter.max_active == workers
    assert not list(tmp_path.glob(".smallsat-convert-*"))
    profiles = {args[1] for args, _ in converter.calls}
    assert len(profiles) == 5
    assert all(profile.startswith("-env:UserInstallation=file://") for profile in profiles)
    assert all("pdf:impress_pdf_Export" in args for args, _ in converter.calls)


@pytest.mark.parametrize("legacy_content", [b"PK legacy presentation mislabeled as PDF", b"%PDF-1"])
def test_conversion_preserves_original_repairs_invalid_pdf_and_skips_valid_output(monkeypatch, tmp_path, legacy_content):
    converter = FakeConverter()
    install_converter(monkeypatch, converter)
    source = presentation(tmp_path)
    target = source.with_suffix(".pdf")
    target.write_bytes(legacy_content)

    async def run():
        queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        queue.submit(source)
        queue.submit(source)
        first = await queue.finish()
        assert await queue.finish() is first
        second_queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        second_queue.submit(source)
        return first, await second_queue.finish()

    first, second = asyncio.run(run())

    assert source.read_bytes() == b"original presentation"
    assert target.read_bytes() == PDF_CONTENT
    assert first.converted == 1
    assert not first.failures
    assert second.skipped == 1
    assert second.converted == 0
    assert len(converter.calls) == 1
    assert not list(tmp_path.glob(".smallsat-convert-*"))


def test_missing_converter_is_reported_for_each_job(monkeypatch, tmp_path):
    monkeypatch.setattr("smallsat_dl.conversion.shutil.which", lambda _: None)
    sources = [presentation(tmp_path, name) for name in ["one.pptx", "two.odp"]]

    async def run():
        queue = PresentationConversionQueue(log=lambda _: None)
        for source in sources:
            queue.submit(source)
        return await queue.finish()

    stats = asyncio.run(run())

    assert stats.converted == 0
    assert [failure.source for failure in stats.failures] == sources
    assert all("LibreOffice was not found" in failure.message for failure in stats.failures)
    assert all(source.exists() and not source.with_suffix(".pdf").exists() for source in sources)


@pytest.mark.parametrize("mode", ["nonzero", "missing", "invalid", "truncated"])
def test_failed_conversion_does_not_block_later_jobs(monkeypatch, tmp_path, mode):
    converter = FakeConverter({"bad.pptx": mode})
    install_converter(monkeypatch, converter)
    bad = presentation(tmp_path, "bad.pptx")
    good = presentation(tmp_path, "good.odp")

    async def run():
        queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        queue.submit(bad)
        queue.submit(good)
        return await queue.finish()

    stats = asyncio.run(run())

    assert stats.converted == 1
    assert len(stats.failures) == 1
    assert stats.failures[0].source == bad
    assert not bad.with_suffix(".pdf").exists()
    assert good.with_suffix(".pdf").read_bytes() == PDF_CONTENT
    assert bad.exists() and good.exists()
    assert not list(tmp_path.glob(".smallsat-convert-*"))


def test_timeout_stops_converter_cleans_staging_and_continues(monkeypatch, tmp_path):
    converter = FakeConverter({"slow.pptx": "hang"})
    install_converter(monkeypatch, converter)
    slow = presentation(tmp_path, "slow.pptx")
    good = presentation(tmp_path, "good.pptx")

    async def run():
        queue = PresentationConversionQueue(timeout=0.01, executable="fake-office", log=lambda _: None)
        queue.submit(slow)
        queue.submit(good)
        return await queue.finish()

    stats = asyncio.run(run())

    assert stats.converted == 1
    assert len(stats.failures) == 1
    assert "timed out" in stats.failures[0].message
    assert converter.processes[0].signals == [signal.SIGTERM]
    assert not slow.with_suffix(".pdf").exists()
    assert good.with_suffix(".pdf").read_bytes() == PDF_CONTENT
    assert not list(tmp_path.glob(".smallsat-convert-*"))


def test_close_cancels_active_and_pending_conversions(monkeypatch, tmp_path):
    converter = FakeConverter(blocked=True)
    install_converter(monkeypatch, converter)
    sources = [presentation(tmp_path, name) for name in ["one.pptx", "two.pptx"]]

    async def run():
        queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        for source in sources:
            queue.submit(source)
        process = await asyncio.wait_for(converter.started.get(), timeout=1)
        await queue.close()
        await queue.close()
        assert process.signals == [signal.SIGTERM]
        assert converter.active == 0
        assert len(converter.calls) == 1
        with pytest.raises(RuntimeError, match="closed"):
            queue.submit(presentation(tmp_path, "late.pptx"))
        return await queue.finish()

    stats = asyncio.run(run())

    assert stats.converted == 0
    assert not stats.failures
    assert all(source.exists() and not source.with_suffix(".pdf").exists() for source in sources)
    assert not list(tmp_path.glob(".smallsat-convert-*"))


def test_cancellation_during_process_startup_still_stops_the_process(monkeypatch, tmp_path):
    converter = FakeConverter()
    install_converter(monkeypatch, converter)
    source = presentation(tmp_path)

    async def run():
        spawned = asyncio.Event()
        release_startup = asyncio.Event()

        async def delayed_spawn(*args, **kwargs):
            process = await converter.spawn(*args, **kwargs)
            spawned.set()
            await release_startup.wait()
            return process

        monkeypatch.setattr("smallsat_dl.conversion.asyncio.create_subprocess_exec", delayed_spawn)
        queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        queue.submit(source)
        await asyncio.wait_for(spawned.wait(), timeout=1)
        closing = asyncio.create_task(queue.close())
        await asyncio.sleep(0)
        assert not closing.done()
        release_startup.set()
        await asyncio.wait_for(closing, timeout=1)

    asyncio.run(run())

    assert converter.processes[0].signals == [signal.SIGTERM]
    assert source.exists()
    assert not source.with_suffix(".pdf").exists()
    assert not list(tmp_path.glob(".smallsat-convert-*"))


def test_only_presentation_extensions_are_queued_case_insensitively(monkeypatch, tmp_path):
    converter = FakeConverter()
    install_converter(monkeypatch, converter)
    supported = [presentation(tmp_path, f"deck{index}{suffix.upper()}")
                 for index, suffix in enumerate(sorted(PRESENTATION_SUFFIXES))]
    for source in supported:
        source.with_suffix(".pdf").write_bytes(PDF_CONTENT)
    unsupported = [presentation(tmp_path, f"other{suffix}")
                   for suffix in [".pdf", ".jpg", ".png", ".m4v", ".srt", ".zip", ".bin"]]

    async def run():
        queue = PresentationConversionQueue(executable="fake-office", log=lambda _: None)
        for source in supported + unsupported:
            queue.submit(source)
        return await queue.finish()

    stats = asyncio.run(run())

    assert stats.skipped == len(PRESENTATION_SUFFIXES)
    assert stats.converted == 0
    assert not stats.failures
    assert not converter.calls


@pytest.mark.parametrize("options", [{"workers": 0}, {"workers": 3}, {"timeout": 0}, {"timeout": float("nan")}])
def test_invalid_conversion_settings_are_rejected(options):
    with pytest.raises(ValueError):
        PresentationConversionQueue(**options)
