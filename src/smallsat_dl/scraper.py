"""Digital Commons scraping and download orchestration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
import datetime as dt
from pathlib import Path
import random
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import httpx

from .paths import paper_path, presentation_path, session_dir

DEFAULT_BASE_URL = "https://digitalcommons.usu.edu/smallsat/"
YEAR_RE = re.compile(r"/smallsat/(\d{4})/")
ProgressCallback = Callable[[str, int, int], None]
RETRYABLE_DOWNLOAD_STATUSES = {403, 429, 500, 502, 503, 504}


def is_retryable_download_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.TransportError)
        or (
            isinstance(exc, httpx.HTTPStatusError)
            and exc.response.status_code in RETRYABLE_DOWNLOAD_STATUSES
        )
    )


class AdaptiveConcurrencyLimiter:
    def __init__(
        self,
        maximum: int,
        *,
        increase_after: int = 12,
        decrease_cooldown: float = 2.0,
        on_change: Callable[[int, str], None] | None = None,
    ) -> None:
        self.maximum = maximum
        self.limit = maximum
        self.increase_after = increase_after
        self.decrease_cooldown = decrease_cooldown
        self.on_change = on_change
        self._active = 0
        self._successes = 0
        self._last_decrease = float("-inf")
        self._condition = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._active < self.limit)
            self._active += 1

    async def release(self) -> None:
        async with self._condition:
            self._active -= 1
            self._condition.notify_all()

    async def record_success(self) -> None:
        async with self._condition:
            self._successes += 1
            if self.limit < self.maximum and self._successes >= self.increase_after:
                self.limit += 1
                self._successes = 0
                self._condition.notify_all()
                if self.on_change is not None:
                    self.on_change(self.limit, "increase")

    async def record_rate_limit(self) -> None:
        now = asyncio.get_running_loop().time()
        async with self._condition:
            if now - self._last_decrease < self.decrease_cooldown:
                return
            new_limit = max(1, self.limit // 2)
            self._last_decrease = now
            self._successes = 0
            if new_limit != self.limit:
                self.limit = new_limit
                if self.on_change is not None:
                    self.on_change(self.limit, "decrease")


@dataclass(frozen=True)
class Link:
    title: str
    url: str


@dataclass(frozen=True)
class EventDetail:
    title: str
    page_url: str
    pdf_url: str | None
    presentation_url: str | None


@dataclass(frozen=True)
class DownloadJob:
    url: str
    destination: Path


@dataclass(frozen=True)
class DeferredDownload:
    job: DownloadJob
    last_error: BaseException


@dataclass
class DownloadFailure:
    url: str
    message: str


@dataclass
class DownloadStats:
    downloaded: int = 0
    skipped: int = 0
    dry_run: int = 0
    unavailable: int = 0
    retryable_errors: int = 0
    rate_limits: int = 0
    first_pass_rate_limits: int = 0
    retry_rate_limits: int = 0
    first_pass_downloaded: int = 0
    first_pass_deferred: int = 0
    first_pass_failures: int = 0
    retry_attempts: int = 0
    retry_recovered: int = 0
    retry_exhausted: int = 0
    retry_rounds: int = 0
    failures: list[DownloadFailure] = field(default_factory=list)

    def extend(self, other: "DownloadStats") -> None:
        self.downloaded += other.downloaded
        self.skipped += other.skipped
        self.dry_run += other.dry_run
        self.unavailable += other.unavailable
        self.retryable_errors += other.retryable_errors
        self.rate_limits += other.rate_limits
        self.first_pass_rate_limits += other.first_pass_rate_limits
        self.retry_rate_limits += other.retry_rate_limits
        self.first_pass_downloaded += other.first_pass_downloaded
        self.first_pass_deferred += other.first_pass_deferred
        self.first_pass_failures += other.first_pass_failures
        self.retry_attempts += other.retry_attempts
        self.retry_recovered += other.retry_recovered
        self.retry_exhausted += other.retry_exhausted
        self.retry_rounds = max(self.retry_rounds, other.retry_rounds)
        self.failures.extend(other.failures)


class HttpClient:
    def __init__(
        self, timeout: float = 30.0, *, client: httpx.AsyncClient | None = None
    ) -> None:
        self.client = client or httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
        )

    async def get_text(self, url: str) -> str:
        response = await self._get(url)
        response.raise_for_status()
        return response.text

    async def get_bytes(self, url: str) -> bytes:
        response = await self.client.get(url)
        response.raise_for_status()
        if "text/html" in response.headers.get("content-type", "").lower():
            raise RuntimeError(f"expected a downloadable file but received HTML from {url}")
        return response.content

    async def close(self) -> None:
        await self.client.aclose()

    async def _get(self, url: str) -> httpx.Response:
        max_attempts = 3
        for attempt in range(max_attempts):
            response = await self.client.get(url)
            retryable_statuses = {429, 500, 502, 503, 504}
            if response.status_code not in retryable_statuses:
                return response
            if attempt < max_attempts - 1:
                retry_after = response.headers.get("retry-after")
                try:
                    delay = float(retry_after) if retry_after is not None else 2**attempt
                except ValueError:
                    delay = 2**attempt
                await asyncio.sleep(delay * random.uniform(0.75, 1.25))
        return response


def parse_available_years(html: str) -> list[int]:
    soup = BeautifulSoup(html, "html.parser")
    years: set[int] = set()

    for link in soup.find_all("a", href=True):
        match = YEAR_RE.search(link["href"])
        if match:
            years.add(int(match.group(1)))

    # Some archive pages expose years in text without stable hrefs.
    for text in soup.stripped_strings:
        if text.isdigit() and len(text) == 4:
            years.add(int(text))

    return sorted(year for year in years if 1987 <= year <= 2100)


def parse_session_links(html: str, base_url: str) -> list[Link]:
    soup = BeautifulSoup(html, "html.parser")
    listing = soup.find("dl", id="events-listing")
    if listing is None:
        return []

    links: list[Link] = []
    for anchor in listing.find_all("a", href=True):
        title = anchor.get("title") or anchor.get_text(" ", strip=True)
        if title:
            links.append(Link(title=title, url=urljoin(base_url, anchor["href"])))
    return links


def prefer_categorized_sessions(sessions: list[Link]) -> list[Link]:
    categorized = [
        session
        for session in sessions
        if not (
            session.title.casefold().startswith("all ")
            and session.title.casefold().endswith(" content")
        )
    ]
    return categorized or sessions


def parse_event_links(html: str, base_url: str) -> list[Link]:
    soup = BeautifulSoup(html, "html.parser")
    events: list[Link] = []
    for event in soup.find_all("td", class_="event"):
        summary = event.find("p", class_="summary")
        if summary is None:
            continue
        anchor = summary.find("a", href=True)
        if anchor is None:
            continue
        title = anchor.get_text(" ", strip=True)
        if title:
            events.append(Link(title=title, url=urljoin(base_url, anchor["href"])))
    return events


def parse_event_detail(html: str, base_url: str, fallback_title: str, page_url: str) -> EventDetail:
    soup = BeautifulSoup(html, "html.parser")
    pdf_anchor = soup.find("a", id="pdf", href=True)
    files = soup.find("div", class_="files")
    presentation_anchor = files.find("a", href=True) if files is not None else None

    return EventDetail(
        title=fallback_title,
        page_url=page_url,
        pdf_url=urljoin(base_url, pdf_anchor["href"]) if pdf_anchor is not None else None,
        presentation_url=(
            urljoin(base_url, presentation_anchor["href"])
            if presentation_anchor is not None
            else None
        ),
    )


class SmallSatDownloader:
    def __init__(
        self,
        *,
        client: HttpClient | None = None,
        base_url: str = DEFAULT_BASE_URL,
        delay: float = 0.25,
        download_delay: float = 1.0,
        concurrency: int = 8,
        download_concurrency: int = 1,
        retry_rounds: int = 5,
        retry_backoff: float = 60.0,
        log: Callable[[str], None] = print,
        progress: ProgressCallback | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if download_concurrency < 1:
            raise ValueError("download concurrency must be at least 1")
        if retry_rounds < 0:
            raise ValueError("retry rounds must be zero or greater")
        if download_delay < 0:
            raise ValueError("download delay must be zero or greater")
        if retry_backoff < 0:
            raise ValueError("retry backoff must be zero or greater")
        self._owns_client = client is None
        self.client = client or HttpClient()
        self.base_url = base_url.rstrip("/") + "/"
        self.delay = delay
        self.download_delay = download_delay
        self.retry_backoff = retry_backoff
        self._semaphore = asyncio.Semaphore(concurrency)
        self.log = log
        self.progress = progress
        self._download_limiter = AdaptiveConcurrencyLimiter(
            download_concurrency,
            on_change=self._report_download_concurrency,
        )
        self._download_worker_count = download_concurrency
        self._max_retry_rounds = retry_rounds

    async def close(self) -> None:
        if self._owns_client:
            await self.client.close()

    async def discover_available_years(self) -> list[int]:
        try:
            html = await self._get_text(self.base_url)
            years = parse_available_years(html)
        except Exception:  # noqa: BLE001 - fallback probes year pages directly.
            years = []

        if not years:
            years = await self._probe_available_years()

        if not years:
            raise RuntimeError("Could not find any SmallSat archive years")
        return years

    async def discover_latest_year(self) -> int:
        today = dt.date.today()
        candidates = [today.year - 1]
        if (today.month, today.day) >= (8, 1):
            candidates.insert(0, today.year)

        results = await asyncio.gather(
            *(self._year_has_downloadable_pdf(year) for year in candidates)
        )
        for year, has_pdf in zip(candidates, results, strict=True):
            if has_pdf:
                return year
        raise RuntimeError("Could not find a SmallSat archive year containing a paper PDF")

    async def download_years(
        self, years: list[int], output_dir: Path, *, dry_run: bool = False
    ) -> DownloadStats:
        total = DownloadStats()
        for year in years:
            total.extend(await self.download_year(year, output_dir, dry_run=dry_run))
        return total

    async def download_year(
        self, year: int, output_dir: Path, *, dry_run: bool = False
    ) -> DownloadStats:
        stats = DownloadStats()
        year_url = urljoin(self.base_url, f"{year}/")
        self.log(f"Year {year}")

        try:
            year_html = await self._get_text(year_url)
            sessions = prefer_categorized_sessions(parse_session_links(year_html, year_url))
        except Exception as exc:  # noqa: BLE001 - keep crawling other requested years.
            stats.failures.append(DownloadFailure(year_url, str(exc)))
            return stats

        session_results = await asyncio.gather(
            *(self._load_session(session) for session in sessions),
            return_exceptions=True,
        )
        event_jobs: list[tuple[Link, Link]] = []
        seen_event_urls: set[str] = set()
        for session, result in zip(sessions, session_results, strict=True):
            if isinstance(result, BaseException):
                stats.failures.append(DownloadFailure(session.url, str(result)))
                continue
            session_path = session_dir(output_dir, year, session.title)
            session_path.mkdir(parents=True, exist_ok=True)
            (session_path / "presentations").mkdir(parents=True, exist_ok=True)
            for event in result:
                if event.url in seen_event_urls:
                    continue
                seen_event_urls.add(event.url)
                event_jobs.append((session, event))

        total_events = len(event_jobs)
        self.log(f"  Found {len(sessions)} sessions and {total_events} unique papers")
        if total_events:
            self._report_progress("Inspecting paper pages...", 0, total_events)

        detail_results = await asyncio.gather(
            *(self._load_event_detail(event) for _, event in event_jobs),
            return_exceptions=True,
        )
        download_queue: asyncio.Queue[tuple[Link, Link, EventDetail]] = asyncio.Queue()
        deferred_downloads: list[DeferredDownload] = []
        completed = 0
        for (session, event), result in zip(event_jobs, detail_results, strict=True):
            if isinstance(result, BaseException):
                stats.failures.append(DownloadFailure(event.url, str(result)))
                stats.first_pass_failures += 1
                completed += 1
                self._report_progress(event.title, completed, total_events)
                continue
            if result.pdf_url is None:
                stats.unavailable += 1
                completed += 1
                self._report_progress(event.title, completed, total_events)
                continue
            download_queue.put_nowait((session, event, result))

        async def download_worker() -> None:
            nonlocal completed
            while True:
                try:
                    session, event, detail = download_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                event_stats, event_deferred = await self._download_event_files(
                    year,
                    session,
                    detail,
                    output_dir,
                    dry_run=dry_run,
                )
                stats.extend(event_stats)
                deferred_downloads.extend(event_deferred)
                download_queue.task_done()
                completed += 1
                self._report_progress(event.title, completed, total_events)

        workers = [
            asyncio.create_task(download_worker())
            for _ in range(min(self._download_worker_count, download_queue.qsize()))
        ]
        if workers:
            await asyncio.gather(*workers)

        self.log(
            "  First pass complete: "
            f"{stats.first_pass_downloaded} downloaded, "
            f"{stats.skipped} skipped, "
            f"{stats.first_pass_deferred} deferred, "
            f"{stats.first_pass_failures} failed"
        )
        await self._retry_deferred_downloads(deferred_downloads, stats)

        return stats

    async def _load_session(self, session: Link) -> list[Link]:
        session_html = await self._get_text(session.url)
        return parse_event_links(session_html, session.url)

    async def _load_event_detail(self, event: Link) -> EventDetail:
        event_html = await self._get_text(event.url)
        return parse_event_detail(event_html, event.url, event.title, event.url)

    async def _download_event_files(
        self,
        year: int,
        session: Link,
        detail: EventDetail,
        output_dir: Path,
        *,
        dry_run: bool,
    ) -> tuple[DownloadStats, list[DeferredDownload]]:
        stats = DownloadStats()
        deferred: list[DeferredDownload] = []
        downloads = [
            DownloadJob(
                detail.pdf_url,
                paper_path(output_dir, year, session.title, detail.title),
            )
        ]
        if detail.presentation_url is not None:
            downloads.append(
                DownloadJob(
                    detail.presentation_url,
                    presentation_path(output_dir, year, session.title, detail.title),
                )
            )

        for job in downloads:
            try:
                await self._download_once(
                    job,
                    stats,
                    dry_run=dry_run,
                    retry_phase=False,
                )
            except Exception as exc:  # noqa: BLE001 - continue with other available files.
                if is_retryable_download_error(exc):
                    stats.first_pass_deferred += 1
                    deferred.append(DeferredDownload(job, exc))
                else:
                    stats.first_pass_failures += 1
                    stats.failures.append(DownloadFailure(job.url, str(exc)))

        return stats, deferred

    async def _download_once(
        self,
        job: DownloadJob,
        stats: DownloadStats,
        *,
        dry_run: bool,
        retry_phase: bool,
    ) -> None:
        if job.destination.exists():
            stats.skipped += 1
            return

        if dry_run:
            stats.dry_run += 1
            return

        if retry_phase:
            stats.retry_attempts += 1
        job.destination.parent.mkdir(parents=True, exist_ok=True)
        content = await self._get_bytes_once(
            job.url,
            stats,
            retry_phase=retry_phase,
        )
        await asyncio.to_thread(job.destination.write_bytes, content)
        stats.downloaded += 1
        if retry_phase:
            stats.retry_recovered += 1
        else:
            stats.first_pass_downloaded += 1

    async def _retry_deferred_downloads(
        self,
        deferred: list[DeferredDownload],
        stats: DownloadStats,
    ) -> None:
        pending = deferred
        for round_number in range(1, self._max_retry_rounds + 1):
            if not pending:
                return

            delay = min(
                600.0,
                self.retry_backoff * (2 ** (round_number - 1)),
            ) * random.uniform(0.75, 1.25)
            self.log(
                f"  Retry round {round_number}: "
                f"{len(pending)} deferred file(s), waiting {delay:.1f}s"
            )
            await asyncio.sleep(delay)

            queue: asyncio.Queue[DeferredDownload] = asyncio.Queue()
            for item in pending:
                queue.put_nowait(item)
            next_pending: list[DeferredDownload] = []
            recovered_before = stats.retry_recovered

            async def retry_worker() -> None:
                while True:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        await self._download_once(
                            item.job,
                            stats,
                            dry_run=False,
                            retry_phase=True,
                        )
                    except Exception as exc:  # noqa: BLE001 - classify for the next round.
                        if is_retryable_download_error(exc):
                            next_pending.append(DeferredDownload(item.job, exc))
                        else:
                            stats.retry_exhausted += 1
                            stats.failures.append(DownloadFailure(item.job.url, str(exc)))
                    finally:
                        queue.task_done()

            workers = [
                asyncio.create_task(retry_worker())
                for _ in range(min(self._download_worker_count, queue.qsize()))
            ]
            if workers:
                await asyncio.gather(*workers)

            stats.retry_rounds = round_number
            recovered = stats.retry_recovered - recovered_before
            self.log(
                f"  Retry round {round_number} complete: "
                f"{recovered} recovered, {len(next_pending)} still deferred"
            )
            pending = next_pending

        for item in pending:
            stats.retry_exhausted += 1
            stats.failures.append(DownloadFailure(item.job.url, str(item.last_error)))

    async def _polite_delay(self) -> None:
        if self.delay > 0:
            await asyncio.sleep(random.uniform(self.delay, self.delay * 2))

    def _report_progress(self, title: str, completed: int, total: int) -> None:
        if self.progress is not None:
            self.progress(title, completed, total)

    async def _get_text(self, url: str) -> str:
        async with self._semaphore:
            await self._polite_delay()
            return await self.client.get_text(url)

    async def _get_bytes_once(
        self,
        url: str,
        stats: DownloadStats,
        *,
        retry_phase: bool,
    ) -> bytes:
        await self._download_limiter.acquire()
        try:
            async with self._semaphore:
                await self._download_polite_delay()
                try:
                    content = await self.client.get_bytes(url)
                except Exception as exc:
                    if is_retryable_download_error(exc):
                        stats.retryable_errors += 1
                    if (
                        isinstance(exc, httpx.HTTPStatusError)
                        and exc.response.status_code in {403, 429}
                    ):
                        stats.rate_limits += 1
                        if retry_phase:
                            stats.retry_rate_limits += 1
                        else:
                            stats.first_pass_rate_limits += 1
                        await self._download_limiter.record_rate_limit()
                    raise
            await self._download_limiter.record_success()
            return content
        finally:
            await self._download_limiter.release()

    async def _download_polite_delay(self) -> None:
        if self.download_delay > 0:
            await asyncio.sleep(
                random.uniform(self.download_delay, self.download_delay * 2)
            )

    def _report_download_concurrency(self, limit: int, direction: str) -> None:
        if direction == "decrease":
            self.log(f"  Server throttled; reducing PDF concurrency to {limit}")
        else:
            self.log(f"  Server stable; increasing PDF concurrency to {limit}")

    async def _probe_available_years(self) -> list[int]:
        current_year = dt.datetime.now().year
        candidates = list(range(1987, current_year + 2))
        results = await asyncio.gather(*(self._year_has_sessions(year) for year in candidates))
        return [year for year, available in zip(candidates, results, strict=True) if available]

    async def _year_has_sessions(self, year: int) -> bool:
        year_url = urljoin(self.base_url, f"{year}/")
        try:
            html = await self._get_text(year_url)
        except Exception:  # noqa: BLE001 - missing/failing years are not available.
            return False
        return bool(parse_session_links(html, year_url))

    async def _year_has_downloadable_pdf(self, year: int) -> bool:
        year_url = urljoin(self.base_url, f"{year}/")
        try:
            year_html = await self._get_text(year_url)
        except Exception:  # noqa: BLE001 - missing/failing years cannot contain a PDF.
            return False

        sessions = prefer_categorized_sessions(parse_session_links(year_html, year_url))
        session_results = await asyncio.gather(
            *(self._load_session(session) for session in sessions),
            return_exceptions=True,
        )
        events_by_url = {
            event.url: event
            for result in session_results
            if not isinstance(result, BaseException)
            for event in result
        }
        events = list(events_by_url.values())

        tasks = [asyncio.create_task(self._event_has_pdf(event)) for event in events]
        for task in asyncio.as_completed(tasks):
            try:
                if await task:
                    for pending in tasks:
                        if not pending.done():
                            pending.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    return True
            except Exception:  # noqa: BLE001 - continue checking other event pages.
                continue

        return False

    async def _event_has_pdf(self, event: Link) -> bool:
        event_html = await self._get_text(event.url)
        return parse_event_detail(event_html, event.url, event.title, event.url).pdf_url is not None
