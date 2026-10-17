"""Digital Commons scraping and download orchestration."""

from __future__ import annotations

import asyncio
import datetime as dt
import math
import random
import re
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit

import httpx
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

from .cache import MetadataCache
from . import oai
from .legacy import presentation_suffix, reuse_legacy_download
from .paths import paper_path, presentation_path, session_dir
from .transports import CurlCffiClient, write_stream_atomic

DEFAULT_BASE_URL = "https://digitalcommons.usu.edu/smallsat/"
YEAR_RE = re.compile(r"/smallsat/(\d{4})/")
ProgressCallback = Callable[[str, int, int], None]
DiagnosticCallback = Callable[[str, dict[str, Any]], None]
RETRYABLE_DOWNLOAD_STATUSES = {403, 429, 500, 502, 503, 504}


def is_retryable_download_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.TransportError)
        or (
            isinstance(exc, httpx.HTTPStatusError)
            and exc.response.status_code in RETRYABLE_DOWNLOAD_STATUSES
        )
    )


def _write_bytes_atomic(destination: Path, content: bytes) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            prefix=".smallsat-",
            suffix=".part",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(content)
        temporary_path.replace(destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


@dataclass(frozen=True)
class Link:
    title: str
    url: str


@dataclass(frozen=True)
class EventDetail:
    title: str
    pdf_url: str | None
    presentation_url: str | None
    presentation_filename: str | None = None
    additional_files: tuple[Link, ...] = ()


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
    file_targets: int = 0
    downloaded: int = 0
    skipped: int = 0
    dry_run: int = 0
    unavailable: int = 0
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

    def extend(self, other: DownloadStats) -> None:
        self.file_targets += other.file_targets
        self.downloaded += other.downloaded
        self.skipped += other.skipped
        self.dry_run += other.dry_run
        self.unavailable += other.unavailable
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
        self, timeout: float = 30.0, *, client: httpx.AsyncClient | None = None,
        transport: str = "curl-cffi",
    ) -> None:
        if transport not in {"httpx", "curl-cffi"}:
            raise ValueError("transport must be httpx or curl-cffi")
        self.client = client or (
            CurlCffiClient(timeout=timeout)
            if transport == "curl-cffi"
            else httpx.AsyncClient(timeout=timeout, follow_redirects=True)
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

    async def download_to(
        self, url: str, destination: Path, *, progress: Callable[[int], None] | None = None,
    ) -> int:
        if isinstance(self.client, CurlCffiClient):
            return await self.client.download_to(url, destination, progress=progress)
        if isinstance(self.client, httpx.AsyncClient):
            async with self.client.stream("GET", url) as response:
                response.raise_for_status()
                if "html" in response.headers.get("content-type", "").lower():
                    raise RuntimeError(f"expected a downloadable file but received HTML from {url}")
                return await write_stream_atomic(destination, response.aiter_bytes(), progress=progress)
        # Preserve injected get-only clients used by integrations.
        content = await self.get_bytes(url)
        await asyncio.to_thread(_write_bytes_atomic, destination, content)
        return len(content)

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


class InvalidArchivePage(RuntimeError):
    """A successful HTTP response did not contain the expected archive page."""


def parse_session_links(html: str, base_url: str) -> list[Link]:
    soup = BeautifulSoup(html, "html.parser")
    listing = soup.find("dl", id="events-listing")
    if listing is None:
        raise InvalidArchivePage(f"Unrecognized SmallSat year page: {base_url}")

    links: list[Link] = []
    for anchor in listing.find_all("a", href=True):
        title = anchor.get("title") or anchor.get_text(" ", strip=True)
        if title:
            links.append(Link(title=title, url=urljoin(base_url, anchor["href"])))
    return links


def is_aggregate_session(session: Link) -> bool:
    title = session.title.casefold()
    return title.startswith("all ") and title.endswith(" content")


def prefer_categorized_sessions(sessions: list[Link]) -> list[Link]:
    # Aggregate listings can contain papers missing from every category. Inspect
    # them last so shared events retain their categorized output directory.
    return sorted(sessions, key=is_aggregate_session)


def parse_event_links(html: str, base_url: str) -> list[Link]:
    soup = BeautifulSoup(html, "html.parser")
    event_cells = soup.find_all("td", class_="event")
    if not event_cells and soup.find("table", class_="vcalendar") is None:
        raise InvalidArchivePage(f"Unrecognized SmallSat session page: {base_url}")
    events: list[Link] = []
    for event in event_cells:
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


def resolve_event_session(
    source_session: Link,
    event: Link,
    sessions_by_url: dict[str, Link],
) -> Link:
    event_parent_url = event.url.rstrip("/").rsplit("/", 1)[0]
    canonical_session = sessions_by_url.get(event_parent_url)
    if canonical_session is None or is_aggregate_session(canonical_session):
        return source_session
    return canonical_session


def validate_event_page(html: str, base_url: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    if (
        soup.find("a", id="pdf", href=True) is not None
        or soup.select_one("div.files a[href]") is not None
        or soup.find("meta", attrs={"name": "bepress_citation_title"}) is not None
        or (
            soup.find("div", id="title") is not None
            and soup.find("div", id="document_type") is not None
        )
    ):
        return
    raise InvalidArchivePage(f"Unrecognized SmallSat event page: {base_url}")


def parse_event_detail(html: str, base_url: str, fallback_title: str) -> EventDetail:
    soup = BeautifulSoup(html, "html.parser")
    pdf_anchor = soup.find("a", id="pdf", href=True)
    files = soup.find("div", class_="files")
    anchors = files.find_all("a", href=True) if files is not None else []
    attachments = list({
        urljoin(base_url, anchor["href"]): Link(
            anchor.get("download") or anchor.get_text(" ", strip=True),
            urljoin(base_url, anchor["href"]),
        ) for anchor in anchors
    }.values())
    first = attachments[0] if attachments else None

    return EventDetail(
        title=fallback_title,
        pdf_url=urljoin(base_url, pdf_anchor["href"]) if pdf_anchor is not None else None,
        presentation_url=(
            first.url
            if first is not None
            else None
        ),
        presentation_filename=(
            first.title
            if first is not None
            else None
        ),
        additional_files=tuple(attachments[1:]),
    )


class SmallSatDownloader:
    def __init__(
        self,
        *,
        client: HttpClient | None = None,
        base_url: str = DEFAULT_BASE_URL,
        delay: float = 0.25,
        downloads_per_minute: float = 30.0,
        concurrency: int = 8,
        retry_rounds: int = 20,
        retry_backoff: float = 60.0,
        log: Callable[[str], None] = print,
        progress: ProgressCallback | None = None,
        diagnostic: DiagnosticCallback | None = None,
        file_ready: Callable[[Path], None] | None = None,
        discovery: str = "auto",
        cache: MetadataCache | None = None,
        transport: str = "curl-cffi",
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if retry_rounds < 0:
            raise ValueError("retry rounds must be zero or greater")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be a finite number zero or greater")
        if not math.isfinite(downloads_per_minute) or downloads_per_minute <= 0:
            raise ValueError("downloads per minute must be a finite number greater than zero")
        if not math.isfinite(retry_backoff) or retry_backoff < 0:
            raise ValueError("retry backoff must be a finite number zero or greater")
        if discovery not in {"auto", "oai", "html"}:
            raise ValueError("discovery must be auto, oai, or html")
        if transport not in {"httpx", "curl-cffi"}:
            raise ValueError("transport must be httpx or curl-cffi")
        self._owns_client = client is None
        self.client = client or HttpClient(transport=transport)
        self.base_url = base_url.rstrip("/") + "/"
        self.delay = delay
        self.downloads_per_minute = downloads_per_minute
        self.retry_backoff = retry_backoff
        self._semaphore = asyncio.Semaphore(concurrency)
        self.log = log
        self.progress = progress
        self.diagnostic = diagnostic
        self.file_ready = file_ready
        self.discovery = discovery
        self.cache = cache
        self._oai_tasks: dict[int, asyncio.Task[list[oai.OaiRecord] | None]] = {}
        self._oai_records: dict[int, list[oai.OaiRecord]] = {}
        self._download_lock = asyncio.Lock()
        self._download_meter = AsyncLimiter(1, 60.0 / downloads_per_minute)
        self._max_retry_rounds = retry_rounds
        self._download_circuit_error: BaseException | None = None

    async def close(self) -> None:
        pending = [task for task in self._oai_tasks.values() if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self._owns_client:
            await self.client.close()

    async def _records_for_year(self, year: int) -> list[oai.OaiRecord] | None:
        if self.discovery == "html":
            return None
        if year not in self._oai_tasks:
            self._oai_tasks[year] = asyncio.create_task(self._discover_oai_year(year))
        return await self._oai_tasks[year]

    async def _discover_oai_year(self, year: int) -> list[oai.OaiRecord] | None:
        endpoint = oai.endpoint_for_base_url(self.base_url)
        set_spec = f"publication:{urlsplit(self.base_url).path.strip('/')}/{year}"
        cache_key = f"{endpoint}#smallsat-dl:oai-v1:{set_spec}"
        try:
            cached = self.cache.get(cache_key) if self.cache is not None else None
            records = None
            if cached is not None:
                try:
                    records = oai.records_from_json(cached)
                    self._diagnose("metadata_cache_hit", url=cache_key, records=len(records))
                except (oai.OaiError, ValueError, TypeError, KeyError):
                    self.cache.invalidate(cache_key)
                    records = None
            if records is None:
                records = await oai.discover_records(
                    self._get_text, endpoint=endpoint, set_spec=set_spec,
                )
                # Cache complete snapshots, never expiring pagination tokens or
                # partial harvests that could silently omit papers next time.
                if self.cache is not None:
                    self.cache.put(cache_key, oai.records_to_json(records))
            matching = [record for record in records if record.year == year]
            if len(matching) != len(records):
                self._diagnose(
                    "oai_records_outside_year", year=year, ignored=len(records) - len(matching),
                )
            # Set membership can lag behind moved papers. Use the landing URL's
            # year and retain HTML fallback for stale/missing record identities.
            records = matching
            self._oai_records[year] = records
            self.log(f"  OAI discovery: {len(records)} records for {year}")
            self._diagnose("oai_discovery_complete", year=year, records=len(records))
            return records
        except oai.OaiError as exc:
            self._diagnose("oai_discovery_failed", year=year, error=str(exc))
            if self.discovery == "oai":
                raise
            self.log(f"  OAI unavailable for {year}; using HTML discovery: {exc}")
            return None

    def _oai_detail(self, event: Link) -> EventDetail | None:
        match = YEAR_RE.search(urlsplit(event.url).path)
        if match is None:
            return None
        records = self._oai_records.get(int(match.group(1)), [])
        candidates = [record for record in records if record.event_url.rstrip("/") == event.url.rstrip("/")]
        # A title alone cannot prove identity: a distinct paper omitted from the
        # feed can share the title of a known record. Resolve unmatched URLs via
        # their own HTML metadata before deduplicating by file identity.
        if len(candidates) != 1:
            return None
        record = candidates[0]
        presentation_url = record.supplemental_urls[0] if record.supplemental_urls else None
        if record.primary_url is None and presentation_url is None:
            return None
        presentation_filename = (
            Path(unquote(urlsplit(presentation_url).path)).name
            if presentation_url is not None
            else None
        )
        return EventDetail(
            event.title, record.primary_url, presentation_url, presentation_filename,
            tuple(Link(Path(unquote(urlsplit(url).path)).name, url)
                  for url in record.supplemental_urls[1:]),
        )

    async def discover_available_years(self) -> list[int]:
        try:
            def validate_years(html: str) -> None:
                if not parse_available_years(html):
                    raise InvalidArchivePage(f"Unrecognized SmallSat archive page: {self.base_url}")
            html = await self._get_text(self.base_url, validator=validate_years)
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
            *(self._year_has_downloadable_pdf(year) for year in candidates),
            return_exceptions=True,
        )
        for year, has_pdf in zip(candidates, results, strict=True):
            if isinstance(has_pdf, BaseException):
                raise has_pdf
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
        year_started = time.monotonic()
        stats = DownloadStats()
        year_url = urljoin(self.base_url, f"{year}/")
        self.log(f"Year {year}")
        self._diagnose(
            "year_start",
            year=year,
            output_dir=str(output_dir),
            dry_run=dry_run,
        )

        try:
            records = await self._records_for_year(year)
            year_html = await self._get_text(
                year_url, validator=lambda html: parse_session_links(html, year_url),
            )
            sessions = prefer_categorized_sessions(parse_session_links(year_html, year_url))
        except Exception as exc:  # noqa: BLE001 - keep crawling other requested years.
            stats.failures.append(DownloadFailure(year_url, str(exc)))
            self._diagnose("year_page_failed", year=year, url=year_url, error=repr(exc))
            return stats

        sessions_started = time.monotonic()
        session_results = await asyncio.gather(
            *(self._load_session(session) for session in sessions),
            return_exceptions=True,
        )
        event_jobs: list[tuple[Link, Link]] = []
        seen_event_urls: set[str] = set()
        seen_event_destinations: set[Path] = set()
        sessions_by_url = {session.url.rstrip("/"): session for session in sessions}
        for source_session, result in zip(sessions, session_results, strict=True):
            if isinstance(result, BaseException):
                stats.failures.append(DownloadFailure(source_session.url, str(result)))
                continue
            session_path = session_dir(output_dir, year, source_session.title)
            session_path.mkdir(parents=True, exist_ok=True)
            (session_path / "presentations").mkdir(parents=True, exist_ok=True)
            for event in result:
                event_url = event.url.rstrip("/")
                if event_url in seen_event_urls:
                    continue
                seen_event_urls.add(event_url)
                owner_session = resolve_event_session(
                    source_session,
                    event,
                    sessions_by_url,
                )
                destination = paper_path(
                    output_dir,
                    year,
                    owner_session.title,
                    event.title,
                )
                if destination in seen_event_destinations:
                    self._diagnose(
                        "duplicate_event_destination",
                        event_url=event.url,
                        destination=str(destination),
                    )
                    continue
                seen_event_destinations.add(destination)
                event_jobs.append((owner_session, event))

        # OAI can expose records absent from every HTML listing. Keep those too,
        # using the aggregate directory when there is no categorized owner.
        if records:
            fallback_session = next(
                (session for session in sessions if is_aggregate_session(session)),
                Link(f"All {year} Content", year_url),
            )
            for record in records:
                if record.event_url.rstrip("/") in seen_event_urls:
                    continue
                event = Link(record.title, record.event_url)
                owner = resolve_event_session(fallback_session, event, sessions_by_url)
                destination = paper_path(output_dir, year, owner.title, event.title)
                if destination not in seen_event_destinations:
                    seen_event_urls.add(record.event_url.rstrip("/"))
                    seen_event_destinations.add(destination)
                    event_jobs.append((owner, event))

        total_events = len(event_jobs)
        self.log(f"  Found {len(sessions)} sessions and {total_events} unique papers")
        self._diagnose(
            "sessions_loaded",
            year=year,
            session_count=len(sessions),
            event_count=total_events,
            failed_sessions=sum(
                isinstance(result, BaseException) for result in session_results
            ),
            duration_seconds=round(time.monotonic() - sessions_started, 6),
        )
        if total_events:
            self._report_progress("Inspecting paper pages...", 0, total_events)

        details_started = time.monotonic()
        detail_results = await asyncio.gather(
            *(self._load_event_detail(event) for _, event in event_jobs),
            return_exceptions=True,
        )
        download_events: list[tuple[Link, Link, EventDetail]] = []
        deferred_downloads: list[DeferredDownload] = []
        seen_event_files: set[tuple[str | None, ...]] = set()
        completed = 0
        for (session, event), result in zip(event_jobs, detail_results, strict=True):
            if isinstance(result, BaseException):
                stats.failures.append(DownloadFailure(event.url, str(result)))
                stats.first_pass_failures += 1
                completed += 1
                self._report_progress(event.title, completed, total_events)
                continue
            if result.pdf_url is None and result.presentation_url is None and not result.additional_files:
                stats.unavailable += 1
                completed += 1
                self._report_progress(event.title, completed, total_events)
                continue
            event_files = (result.pdf_url, result.presentation_url, *(link.url for link in result.additional_files))
            if event_files in seen_event_files:
                self._diagnose("duplicate_event_files", event_url=event.url)
                completed += 1
                self._report_progress(event.title, completed, total_events)
                continue
            seen_event_files.add(event_files)
            download_events.append((session, event, result))

        self._diagnose(
            "event_details_loaded",
            year=year,
            requested=total_events,
            queued=len(download_events),
            unavailable=stats.unavailable,
            failures=stats.first_pass_failures,
            duration_seconds=round(time.monotonic() - details_started, 6),
        )
        self._diagnose(
            "first_pass_start",
            year=year,
            queued_events=len(download_events),
        )
        self._download_circuit_error = None

        for session, event, detail in download_events:
            event_stats, event_deferred = await self._download_event_files(
                year,
                session,
                detail,
                output_dir,
                dry_run=dry_run,
            )
            stats.extend(event_stats)
            deferred_downloads.extend(event_deferred)
            completed += 1
            self._report_progress(event.title, completed, total_events)
            self._diagnose(
                "event_files_complete",
                year=year,
                event_url=event.url,
                title=event.title,
                completed=completed,
                total=total_events,
                downloaded=event_stats.downloaded,
                skipped=event_stats.skipped,
                deferred=len(event_deferred),
                failures=len(event_stats.failures),
            )

        self.log(
            "  First pass complete: "
            f"{stats.first_pass_downloaded} downloaded, "
            f"{stats.skipped} skipped, "
            f"{stats.first_pass_deferred} deferred, "
            f"{stats.first_pass_failures} failed"
        )
        self._diagnose(
            "first_pass_complete",
            year=year,
            downloaded=stats.first_pass_downloaded,
            skipped=stats.skipped,
            deferred=stats.first_pass_deferred,
            failures=stats.first_pass_failures,
            rate_limits=stats.first_pass_rate_limits,
            elapsed_seconds=round(time.monotonic() - year_started, 6),
        )
        await self._retry_deferred_downloads(deferred_downloads, stats)

        self._diagnose(
            "year_complete",
            year=year,
            downloaded=stats.downloaded,
            skipped=stats.skipped,
            unavailable=stats.unavailable,
            failures=len(stats.failures),
            first_pass_deferred=stats.first_pass_deferred,
            retry_recovered=stats.retry_recovered,
            retry_exhausted=stats.retry_exhausted,
            first_pass_rate_limits=stats.first_pass_rate_limits,
            retry_rate_limits=stats.retry_rate_limits,
            retry_rounds=stats.retry_rounds,
            elapsed_seconds=round(time.monotonic() - year_started, 6),
        )
        return stats

    async def _load_session(self, session: Link) -> list[Link]:
        session_html = await self._get_text(
            session.url, validator=lambda html: parse_event_links(html, session.url),
        )
        return parse_event_links(session_html, session.url)

    async def _load_event_detail(self, event: Link) -> EventDetail:
        detail = self._oai_detail(event)
        if detail is not None:
            self._diagnose("oai_event_detail", event_url=event.url)
            return detail
        event_html = await self._get_text(
            event.url, validator=lambda html: validate_event_page(html, event.url),
        )
        return parse_event_detail(event_html, event.url, event.title)

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
        downloads: list[DownloadJob] = []
        if detail.pdf_url is not None:
            downloads.append(
                DownloadJob(
                    detail.pdf_url,
                    paper_path(output_dir, year, session.title, detail.title, url=detail.pdf_url),
                )
            )
        if detail.pdf_url is None and (detail.presentation_url is not None or detail.additional_files):
            stats.unavailable += 1
        attachments = []
        if detail.presentation_url is not None:
            attachments.append(Link(detail.presentation_filename or "", detail.presentation_url))
        attachments.extend(detail.additional_files)
        seen_attachment_urls: set[str] = set()
        for attachment in attachments:
            if attachment.url in seen_attachment_urls:
                continue
            seen_attachment_urls.add(attachment.url)
            number = len(seen_attachment_urls)
            title = detail.title if number == 1 else f"{detail.title} - Attachment {number}"
            downloads.append(
                DownloadJob(
                    attachment.url,
                    presentation_path(
                        output_dir,
                        year,
                        session.title,
                        title,
                        filename=attachment.title,
                        url=attachment.url,
                    ),
                )
            )
        stats.file_targets = len(downloads)

        for job in downloads:
            if not dry_run:
                await self._reuse_legacy_file(job)
            if self._download_circuit_error is not None and not job.destination.exists():
                stats.first_pass_deferred += 1
                deferred.append(
                    DeferredDownload(job, self._download_circuit_error)
                )
                self._diagnose(
                    "download_deferred",
                    phase="first_pass",
                    reason="rate_limit_circuit_open",
                    url=job.url,
                    destination=str(job.destination),
                    status_code=self._status_code(self._download_circuit_error),
                )
                continue
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
                    self._diagnose(
                        "download_deferred",
                        phase="first_pass",
                        url=job.url,
                        destination=str(job.destination),
                        status_code=self._status_code(exc),
                        error=repr(exc),
                    )
                else:
                    stats.first_pass_failures += 1
                    stats.failures.append(DownloadFailure(job.url, str(exc)))
                    self._diagnose(
                        "download_failed",
                        phase="first_pass",
                        url=job.url,
                        destination=str(job.destination),
                        status_code=self._status_code(exc),
                        error=repr(exc),
                    )

        return stats, deferred

    async def _reuse_legacy_file(self, job: DownloadJob) -> None:
        if job.destination.suffix.lower() == ".pdf" or job.destination.exists():
            return
        if await asyncio.to_thread(reuse_legacy_download, job.destination):
            self._diagnose(
                "legacy_file_reused", url=job.url, destination=str(job.destination),
                legacy_path=str(job.destination.with_suffix(".pdf")),
                bytes=job.destination.stat().st_size,
            )

    async def _notify_file_ready(self, destination: Path) -> None:
        if self.file_ready is None:
            return
        source = destination
        if destination.suffix.lower() == ".pdf":
            suffix = await asyncio.to_thread(presentation_suffix, destination)
            if suffix is not None:
                corrected = destination.with_suffix(suffix)
                reused = await asyncio.to_thread(reuse_legacy_download, corrected)
                if reused or (corrected.exists() and corrected.samefile(destination)):
                    source = corrected
                    self._diagnose(
                        "download_format_corrected", original_path=str(destination),
                        destination=str(corrected), suffix=suffix,
                    )
        self.file_ready(source)

    async def _download_once(
        self,
        job: DownloadJob,
        stats: DownloadStats,
        *,
        dry_run: bool,
        retry_phase: bool,
    ) -> None:
        if not dry_run:
            await self._reuse_legacy_file(job)
        if job.destination.exists():
            stats.skipped += 1
            self._diagnose(
                "download_skipped",
                phase="retry" if retry_phase else "first_pass",
                url=job.url,
                destination=str(job.destination),
                existing_bytes=job.destination.stat().st_size,
            )
            if not dry_run and self.file_ready is not None:
                await self._notify_file_ready(job.destination)
            return

        if dry_run:
            stats.dry_run += 1
            self._diagnose(
                "download_dry_run",
                phase="retry" if retry_phase else "first_pass",
                url=job.url,
                destination=str(job.destination),
            )
            return

        if retry_phase:
            stats.retry_attempts += 1
        started = time.monotonic()
        self._diagnose(
            "download_start",
            phase="retry" if retry_phase else "first_pass",
            url=job.url,
            destination=str(job.destination),
        )
        job.destination.parent.mkdir(parents=True, exist_ok=True)
        content = await self._get_bytes_once(
            job.url,
            stats,
            retry_phase=retry_phase,
            destination=job.destination,
        )
        if isinstance(content, bytes):
            await asyncio.to_thread(_write_bytes_atomic, job.destination, content)
            byte_count = len(content)
        else:
            byte_count = content
        stats.downloaded += 1
        if retry_phase:
            stats.retry_recovered += 1
        else:
            stats.first_pass_downloaded += 1
        self._diagnose(
            "download_complete",
            phase="retry" if retry_phase else "first_pass",
            url=job.url,
            destination=str(job.destination),
            bytes=byte_count,
            duration_seconds=round(time.monotonic() - started, 6),
        )
        if self.file_ready is not None:
            await self._notify_file_ready(job.destination)

    async def _retry_deferred_downloads(
        self,
        deferred: list[DeferredDownload],
        stats: DownloadStats,
    ) -> None:
        pending = deferred
        for round_number in range(1, self._max_retry_rounds + 1):
            if not pending:
                self._report_progress("Complete", 1, 1)
                return

            delay = self._retry_round_delay()
            self.log(
                f"  Retry round {round_number}: "
                f"{len(pending)} deferred file(s), waiting {delay:.1f}s"
            )
            self._diagnose(
                "retry_round_wait",
                round=round_number,
                pending=len(pending),
                delay_seconds=round(delay, 6),
            )
            await self._wait_for_retry_round(delay, round_number, len(pending))

            next_pending: list[DeferredDownload] = []
            recovered_before = stats.retry_recovered
            round_started = time.monotonic()
            self._download_circuit_error = None
            self._diagnose(
                "retry_round_start",
                round=round_number,
                pending=len(pending),
            )

            for item in pending:
                await self._reuse_legacy_file(item.job)
                if (
                    self._download_circuit_error is not None
                    and not item.job.destination.exists()
                ):
                    next_pending.append(
                        DeferredDownload(item.job, self._download_circuit_error)
                    )
                    self._diagnose(
                        "download_deferred",
                        phase=f"retry_round_{round_number}",
                        reason="rate_limit_circuit_open",
                        url=item.job.url,
                        destination=str(item.job.destination),
                        status_code=self._status_code(
                            self._download_circuit_error
                        ),
                    )
                    continue
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
                        self._diagnose(
                            "download_deferred",
                            phase=f"retry_round_{round_number}",
                            url=item.job.url,
                            destination=str(item.job.destination),
                            status_code=self._status_code(exc),
                            error=repr(exc),
                        )
                    else:
                        stats.retry_exhausted += 1
                        stats.failures.append(DownloadFailure(item.job.url, str(exc)))
                        self._diagnose(
                            "download_failed",
                            phase=f"retry_round_{round_number}",
                            url=item.job.url,
                            destination=str(item.job.destination),
                            status_code=self._status_code(exc),
                            error=repr(exc),
                        )

            stats.retry_rounds = round_number
            recovered = stats.retry_recovered - recovered_before
            self.log(
                f"  Retry round {round_number} complete: "
                f"{recovered} recovered, {len(next_pending)} still deferred"
            )
            self._diagnose(
                "retry_round_complete",
                round=round_number,
                attempted=len(pending),
                recovered=recovered,
                deferred=len(next_pending),
                rate_limits=stats.retry_rate_limits,
                duration_seconds=round(time.monotonic() - round_started, 6),
            )
            pending = next_pending

        for item in pending:
            stats.retry_exhausted += 1
            stats.failures.append(DownloadFailure(item.job.url, str(item.last_error)))
            self._diagnose(
                "download_exhausted",
                url=item.job.url,
                destination=str(item.job.destination),
                status_code=self._status_code(item.last_error),
                error=repr(item.last_error),
            )
        self._report_progress("Complete", 1, 1)

    async def _wait_for_retry_round(
        self, delay: float, round_number: int, pending: int
    ) -> None:
        deadline = asyncio.get_running_loop().time() + delay
        while True:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            self._report_progress(
                f"Retry {round_number}: waiting {remaining:.0f}s for {pending} files",
                0,
                pending,
            )
            if remaining <= 0:
                return
            await asyncio.sleep(min(5.0, remaining))

    def _retry_round_delay(self) -> float:
        return min(600.0, self.retry_backoff) * random.uniform(0.75, 1.25)

    async def _polite_delay(self) -> None:
        if self.delay > 0:
            await asyncio.sleep(random.uniform(self.delay, self.delay * 2))

    def _report_progress(self, title: str, completed: int, total: int) -> None:
        if self.progress is not None:
            self.progress(title, completed, total)

    async def _get_text(
        self, url: str, *, validator: Callable[[str], object] | None = None,
    ) -> str:
        if self.cache is not None and validator is not None:
            cached = self.cache.get(url)
            if cached is not None:
                try:
                    validator(cached)
                except Exception:
                    self.cache.invalidate(url)
                else:
                    self._diagnose("metadata_cache_hit", url=url)
                    return cached
        async with self._semaphore:
            await self._polite_delay()
            text = await self.client.get_text(url)
        if validator is not None:
            validator(text)
            if self.cache is not None:
                self.cache.put(url, text)
        return text

    async def _get_bytes_once(
        self,
        url: str,
        stats: DownloadStats,
        *,
        retry_phase: bool,
        destination: Path | None = None,
    ) -> bytes | int:
        lock_started = time.monotonic()
        async with self._download_lock:
            lock_wait = time.monotonic() - lock_started
            meter_started = time.monotonic()
            await self._download_meter.acquire()
            meter_wait = time.monotonic() - meter_started
            async with self._semaphore:
                request_started = time.monotonic()
                self._diagnose(
                    "http_download_request_start",
                    phase="retry" if retry_phase else "first_pass",
                    url=url,
                    serial_wait_seconds=round(lock_wait, 6),
                    meter_wait_seconds=round(meter_wait, 6),
                    downloads_per_minute=self.downloads_per_minute,
                )
                try:
                    if destination is not None and hasattr(self.client, "download_to"):
                        content = await self.client.download_to(
                            url, destination,
                            progress=lambda count: self._diagnose(
                                "http_download_progress", url=url, bytes=count,
                                duration_seconds=round(time.monotonic() - request_started, 6),
                            ),
                        )
                    else:
                        content = await self.client.get_bytes(url)
                except Exception as exc:
                    if (
                        isinstance(exc, httpx.HTTPStatusError)
                        and exc.response.status_code in {403, 429}
                    ):
                        if retry_phase:
                            stats.retry_rate_limits += 1
                        else:
                            stats.first_pass_rate_limits += 1
                        if self._download_circuit_error is None:
                            self._download_circuit_error = exc
                            self._diagnose(
                                "rate_limit_circuit_open",
                                phase="retry" if retry_phase else "first_pass",
                                url=url,
                                status_code=exc.response.status_code,
                            )
                    response = (
                        exc.response
                        if isinstance(exc, httpx.HTTPStatusError)
                        else None
                    )
                    self._diagnose(
                        "http_download_request_error",
                        phase="retry" if retry_phase else "first_pass",
                        url=url,
                        status_code=self._status_code(exc),
                        error_type=type(exc).__name__,
                        error=str(exc),
                        duration_seconds=round(
                            time.monotonic() - request_started, 6
                        ),
                        retry_after=(
                            response.headers.get("retry-after")
                            if response is not None
                            else None
                        ),
                        content_type=(
                            response.headers.get("content-type")
                            if response is not None
                            else None
                        ),
                        server=(
                            response.headers.get("server")
                            if response is not None
                            else None
                        ),
                    )
                    raise
                self._diagnose(
                    "http_download_request_complete",
                    phase="retry" if retry_phase else "first_pass",
                    url=url,
                    bytes=len(content) if isinstance(content, bytes) else content,
                    duration_seconds=round(time.monotonic() - request_started, 6),
                )
            return content

    def _diagnose(self, event: str, **fields: Any) -> None:
        if self.diagnostic is not None:
            self.diagnostic(event, fields)

    @staticmethod
    def _status_code(exc: BaseException) -> int | None:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code
        return None

    async def _probe_available_years(self) -> list[int]:
        current_year = dt.datetime.now().year
        candidates = list(range(1987, current_year + 2))
        results = await asyncio.gather(*(self._year_has_sessions(year) for year in candidates))
        return [year for year, available in zip(candidates, results, strict=True) if available]

    async def _year_has_sessions(self, year: int) -> bool:
        year_url = urljoin(self.base_url, f"{year}/")
        try:
            html = await self._get_text(
                year_url, validator=lambda html: parse_session_links(html, year_url),
            )
            return bool(parse_session_links(html, year_url))
        except Exception:  # noqa: BLE001 - missing/failing years are not available.
            return False

    async def _year_has_downloadable_pdf(self, year: int) -> bool:
        records = await self._records_for_year(year)
        if records and any(record.primary_url for record in records):
            return True
        year_url = urljoin(self.base_url, f"{year}/")
        try:
            year_html = await self._get_text(
                year_url, validator=lambda html: parse_session_links(html, year_url),
            )
        except InvalidArchivePage:
            raise
        except Exception:  # noqa: BLE001 - missing/failing years cannot contain a PDF.
            return False

        sessions = prefer_categorized_sessions(parse_session_links(year_html, year_url))
        session_results = await asyncio.gather(
            *(self._load_session(session) for session in sessions),
            return_exceptions=True,
        )
        invalid_pages = [
            result for result in session_results if isinstance(result, InvalidArchivePage)
        ]
        events_by_url = {
            event.url.rstrip("/"): event
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
            except InvalidArchivePage as exc:
                invalid_pages.append(exc)
            except Exception:  # noqa: BLE001 - continue checking other event pages.
                continue

        if invalid_pages:
            raise invalid_pages[0]
        return False

    async def _event_has_pdf(self, event: Link) -> bool:
        detail = self._oai_detail(event)
        if detail is not None:
            return detail.pdf_url is not None
        event_html = await self._get_text(
            event.url, validator=lambda html: validate_event_page(html, event.url),
        )
        return parse_event_detail(event_html, event.url, event.title).pdf_url is not None
