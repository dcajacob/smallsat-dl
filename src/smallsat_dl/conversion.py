"""Bounded background conversion of presentation attachments to PDF."""

from __future__ import annotations

import asyncio
import math
import os
import shutil
import signal
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


PRESENTATION_SUFFIXES = frozenset(
    {".ppt", ".pptx", ".pps", ".ppsx", ".pptm", ".ppsm", ".pot", ".potx", ".potm", ".odp", ".otp", ".fodp"}
)
DiagnosticCallback = Callable[[str, dict[str, Any]], None]


@dataclass(frozen=True)
class ConversionFailure:
    source: Path
    message: str


@dataclass
class ConversionStats:
    converted: int = 0
    skipped: int = 0
    failures: list[ConversionFailure] = field(default_factory=list)


def _is_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as source:
            if source.read(5) != b"%PDF-":
                return False
            source.seek(0, os.SEEK_END)
            source.seek(max(0, source.tell() - 2048))
            return source.read().rstrip().endswith(b"%%EOF")
    except OSError:
        return False


class PresentationConversionQueue:
    """Run one or two LibreOffice conversions while downloads keep running."""

    def __init__(
        self,
        workers: int = 1,
        timeout: float = 120.0,
        executable: str | None = None,
        log: Callable[[str], None] = print,
        diagnostic: DiagnosticCallback | None = None,
    ) -> None:
        if workers not in {1, 2}:
            raise ValueError("conversion workers must be 1 or 2")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("conversion timeout must be a finite number greater than zero")
        self.workers = workers
        self.timeout = timeout
        self.executable = (
            executable
            if executable is not None
            else shutil.which("libreoffice") or shutil.which("soffice")
        )
        self.log = log
        self.diagnostic = diagnostic
        self._queue: asyncio.Queue[Path] = asyncio.Queue()
        self._seen: set[Path] = set()
        self._workers: list[asyncio.Task[None]] = []
        self._processes: set[asyncio.subprocess.Process] = set()
        self._target_locks: dict[Path, asyncio.Lock] = {}
        self._stats = ConversionStats()
        self._finishing = False
        self._closed = False

    def submit(self, source: Path) -> None:
        """Queue a presentation without waiting for its conversion."""
        source = Path(source)
        if source.suffix.casefold() not in PRESENTATION_SUFFIXES:
            return
        if self._closed or self._finishing:
            raise RuntimeError("presentation conversion queue is closed")
        loop = asyncio.get_running_loop()
        source = source.resolve()
        if source in self._seen:
            return
        self._seen.add(source)
        self._queue.put_nowait(source)
        if not self._workers:
            self._workers = [loop.create_task(self._worker()) for _ in range(self.workers)]
        self._diagnose("presentation_conversion_queued", source=str(source))

    async def finish(self) -> ConversionStats:
        """Wait for submitted presentations, then stop the idle workers."""
        self._finishing = True
        try:
            await self._queue.join()
        finally:
            await self.close()
        return self._stats

    async def close(self) -> None:
        """Cancel outstanding work and clean up any active converter processes."""
        self._closed = True
        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        # Usually the workers have already stopped their own processes. Retain
        # this cleanup for cancellation arriving between process setup steps.
        for process in tuple(self._processes):
            await self._stop_process(process)
            self._processes.discard(process)
        while not self._queue.empty():
            self._queue.get_nowait()
            self._queue.task_done()

    async def _worker(self) -> None:
        while True:
            source = await self._queue.get()
            try:
                target = source.with_suffix(".pdf")
                lock = self._target_locks.setdefault(target, asyncio.Lock())
                async with lock:
                    await self._convert(source, target)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Keep later presentations moving after a failed job.
                self._stats.failures.append(ConversionFailure(source, str(exc)))
                self.log(f"  Presentation conversion failed: {source.name}: {exc}")
                self._diagnose(
                    "presentation_conversion_failed", source=str(source), error=str(exc)
                )
            finally:
                self._queue.task_done()

    async def _convert(self, source: Path, target: Path) -> None:
        if _is_pdf(target):
            self._stats.skipped += 1
            self._diagnose(
                "presentation_conversion_skipped", source=str(source), destination=str(target)
            )
            return
        if self.executable is None:
            raise RuntimeError("LibreOffice was not found; install libreoffice to convert presentations")
        if not source.is_file():
            raise RuntimeError(f"presentation source does not exist: {source}")

        self._diagnose("presentation_conversion_start", source=str(source), destination=str(target))
        # Staging beside the destination keeps the final replace on one filesystem.
        # A separate profile prevents reuse of a running LibreOffice UI or worker.
        with tempfile.TemporaryDirectory(prefix=".smallsat-convert-", dir=target.parent) as temporary:
            staging = Path(temporary)
            output_dir = staging / "output"
            output_dir.mkdir()
            profile = staging / "profile"
            startup = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    self.executable,
                    f"-env:UserInstallation={profile.as_uri()}",
                    "--headless",
                    "--convert-to",
                    "pdf:impress_pdf_Export",
                    "--outdir",
                    str(output_dir),
                    str(source),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=os.name == "posix",
                )
            )
            try:
                process = await asyncio.shield(startup)
            except asyncio.CancelledError:
                # Obtain the process even if cancellation arrived during startup,
                # so its entire process group is stopped before staging disappears.
                try:
                    process = await startup
                except Exception:
                    pass
                else:
                    await self._stop_process(process)
                raise
            self._processes.add(process)
            try:
                try:
                    stdout, stderr = await asyncio.wait_for(process.communicate(), self.timeout)
                except TimeoutError:
                    await self._stop_process(process)
                    raise RuntimeError(f"LibreOffice timed out after {self.timeout:g} seconds") from None
                except (Exception, asyncio.CancelledError):
                    await self._stop_process(process)
                    raise
            finally:
                self._processes.discard(process)

            detail = (stderr or stdout).decode(errors="replace").strip()[:1000]
            if process.returncode != 0:
                message = f"LibreOffice exited with status {process.returncode}"
                raise RuntimeError(f"{message}: {detail}" if detail else message)
            generated = output_dir / target.name
            if not _is_pdf(generated):
                message = "LibreOffice did not produce a valid PDF"
                raise RuntimeError(f"{message}: {detail}" if detail else message)
            generated.replace(target)

        self._stats.converted += 1
        self.log(f"  Converted presentation: {source.name} -> {target.name}")
        self._diagnose(
            "presentation_conversion_complete", source=str(source), destination=str(target)
        )

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        def send_signal(value: signal.Signals) -> None:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, value)
                elif process.returncode is None:
                    if value == signal.SIGTERM:
                        process.terminate()
                    else:
                        process.kill()
            except ProcessLookupError:
                pass

        send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except TimeoutError:
            send_signal(signal.SIGKILL)
            await process.wait()

    def _diagnose(self, event: str, **fields: Any) -> None:
        if self.diagnostic is not None:
            self.diagnostic(event, fields)
