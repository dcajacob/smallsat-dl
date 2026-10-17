import asyncio
import datetime as dt
from pathlib import Path

import httpx

from smallsat_dl.scraper import (
    DeferredDownload,
    DownloadJob,
    DownloadStats,
    EventDetail,
    HttpClient,
    Link,
    SmallSatDownloader,
    parse_available_years,
    parse_event_detail,
    parse_event_links,
    parse_session_links,
    prefer_categorized_sessions,
    resolve_event_session,
)

ARCHIVE_HTML = """
<html><body>
  <a href="/smallsat/2024/">2024 archive</a>
  <a href="https://digitalcommons.usu.edu/smallsat/2025/">2025 archive</a>
  <a href="/elsewhere/1999/">not smallsat</a>
</body></html>
"""

YEAR_HTML = """
<dl id="events-listing">
  <a title="All 2025 Content" href="all2025/">All 2025 Content</a>
</dl>
"""

SESSION_HTML = """
<table>
  <tr><td class="event"><p class="summary"><a href="/smallsat/2025/all2025/1/">Paper One</a></p></td></tr>
  <tr><td class="event"><p class="summary"><a href="/smallsat/2025/all2025/2/">Paper Two</a></p></td></tr>
</table>
"""

EVENT_ONE_HTML = """
<html><body>
  <a id="pdf" href="/smallsat/2025/all2025/1/paper.pdf">Download</a>
  <div class="files"><a href="/smallsat/2025/all2025/1/slides.pdf">Slides</a></div>
</body></html>
"""

EVENT_ONE_PAPER_ONLY_HTML = """
<html><body>
  <a id="pdf" href="/smallsat/2025/all2025/1/paper.pdf">Download</a>
</body></html>
"""

EVENT_TWO_HTML = """
<html><body>
  <a id="pdf" href="/smallsat/2025/all2025/2/paper.pdf">Download</a>
</body></html>
"""

EVENT_WITHOUT_PDF_HTML = """
<html><head><meta name="bepress_citation_title" content="No paper file"></head>
<body><p>No paper file</p></body></html>
"""

YEAR_2026_HTML = """
<dl id="events-listing">
  <a title="All 2026 Content" href="all2026/">All 2026 Content</a>
</dl>
"""

EMPTY_SESSION_HTML = '<table class="vcalendar"></table>'


class FakeClient:
    def __init__(self, text: dict[str, str], content: dict[str, bytes] | None = None) -> None:
        self.text = text
        self.content = content or {}

    async def get_text(self, url: str) -> str:
        return self.text[url]

    async def get_bytes(self, url: str) -> bytes:
        return self.content[url]


class TrackingClient(FakeClient):
    def __init__(self, text: dict[str, str], content: dict[str, bytes] | None = None) -> None:
        super().__init__(text, content)
        self.active_requests = 0
        self.max_active_requests = 0
        self.requested_urls: list[str] = []

    async def get_text(self, url: str) -> str:
        self.requested_urls.append(url)
        self.active_requests += 1
        self.max_active_requests = max(self.max_active_requests, self.active_requests)
        try:
            await asyncio.sleep(0.01)
            return await super().get_text(url)
        finally:
            self.active_requests -= 1

    async def get_bytes(self, url: str) -> bytes:
        self.active_requests += 1
        self.max_active_requests = max(self.max_active_requests, self.active_requests)
        try:
            await asyncio.sleep(0.01)
            return await super().get_bytes(url)
        finally:
            self.active_requests -= 1


class SequencedAsyncClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.calls = 0

    async def get(self, url: str) -> httpx.Response:
        response = self.responses[self.calls]
        self.calls += 1
        return response

    async def aclose(self) -> None:
        pass


class DeferredThenSuccessClient(FakeClient):
    def __init__(
        self,
        text: dict[str, str],
        outcomes: dict[str, list[bytes | int]],
    ) -> None:
        super().__init__(text)
        self.outcomes = outcomes
        self.download_calls: list[str] = []

    async def get_bytes(self, url: str) -> bytes:
        self.download_calls.append(url)
        outcome = self.outcomes[url].pop(0)
        if isinstance(outcome, int):
            request = httpx.Request("GET", url)
            response = httpx.Response(outcome, request=request)
            raise httpx.HTTPStatusError(
                f"status {outcome}",
                request=request,
                response=response,
            )
        return outcome


def test_parse_available_years_uses_smallsat_links_only():
    assert parse_available_years(ARCHIVE_HTML) == [2024, 2025]


def test_parse_session_links_prefers_title_attribute():
    sessions = parse_session_links(YEAR_HTML, "https://digitalcommons.usu.edu/smallsat/2025/")
    assert sessions[0].title == "All 2025 Content"
    assert sessions[0].url == "https://digitalcommons.usu.edu/smallsat/2025/all2025/"


def test_prefer_categorized_sessions_keeps_aggregate_listing_last():
    sessions = [
        Link("All 2025 Content", "https://example.test/all"),
        Link("Keynote 2025", "https://example.test/keynote"),
    ]

    assert prefer_categorized_sessions(sessions) == [sessions[1], sessions[0]]


def test_event_is_assigned_to_session_named_in_its_canonical_url():
    enterprise = Link(
        "Session VII: Science/Mission Payloads - Enterprise",
        "https://digitalcommons.usu.edu/smallsat/2025/E-S7-2025",
    )
    research = Link(
        "Session II: Next on the Pad - Research & Academia",
        "https://digitalcommons.usu.edu/smallsat/2025/RA-S2-2025",
    )
    event = Link(
        "STP-H12-VANTAGE",
        "https://digitalcommons.usu.edu/smallsat/2025/RA-S2-2025/1",
    )

    owner = resolve_event_session(
        enterprise,
        event,
        {
            enterprise.url: enterprise,
            research.url: research,
        },
    )

    assert owner == research


def test_crosslisted_event_without_session_url_keeps_source_session():
    enterprise = Link(
        "Session II: Next on the Pad - Enterprise",
        "https://digitalcommons.usu.edu/smallsat/2025/E-S2-2025",
    )
    event = Link(
        "ACMES",
        "https://digitalcommons.usu.edu/smallsat/2025/all2025/159",
    )

    assert resolve_event_session(enterprise, event, {enterprise.url: enterprise}) == enterprise


def test_parse_event_links_extracts_summary_links():
    events = parse_event_links(SESSION_HTML, "https://digitalcommons.usu.edu/smallsat/2025/all2025/")
    assert [event.title for event in events] == ["Paper One", "Paper Two"]
    assert events[0].url == "https://digitalcommons.usu.edu/smallsat/2025/all2025/1/"


def test_parse_event_detail_handles_missing_presentation():
    detail = parse_event_detail(
        EVENT_TWO_HTML,
        "https://digitalcommons.usu.edu/smallsat/2025/all2025/2/",
        "Paper Two",
    )
    assert detail.pdf_url == "https://digitalcommons.usu.edu/smallsat/2025/all2025/2/paper.pdf"
    assert detail.presentation_url is None


def test_downloader_skips_existing_files_and_downloads_missing_targets(tmp_path: Path):
    base = "https://digitalcommons.usu.edu/smallsat/"
    progress_updates: list[tuple[str, int, int]] = []
    diagnostic_events: list[tuple[str, dict[str, object]]] = []
    paper_one = tmp_path / "2025" / "All 2025 Content" / "Paper One.pdf"
    paper_one.parent.mkdir(parents=True)
    paper_one.write_bytes(b"already here")

    client = FakeClient(
        text={
            base: ARCHIVE_HTML,
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_ONE_HTML,
            f"{base}2025/all2025/2/": EVENT_TWO_HTML,
        },
        content={
            f"{base}2025/all2025/1/slides.pdf": b"slides",
            f"{base}2025/all2025/2/paper.pdf": b"paper two",
        },
    )
    downloader = SmallSatDownloader(
        client=client,
        delay=0,
        downloads_per_minute=1_000_000,
        log=lambda _: None,
        progress=lambda title, completed, total: progress_updates.append(
            (title, completed, total)
        ),
        diagnostic=lambda event, fields: diagnostic_events.append((event, fields)),
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path))

    assert stats.file_targets == 3
    assert stats.downloaded == 2
    assert stats.skipped == 1
    assert not stats.failures
    assert (tmp_path / "2025" / "All 2025 Content" / "presentations" / "Paper One.pdf").read_bytes() == b"slides"
    assert (tmp_path / "2025" / "All 2025 Content" / "Paper Two.pdf").read_bytes() == b"paper two"
    assert not (tmp_path / "2025" / "All 2025 Content" / "presentations" / "Paper Two.pdf").exists()
    assert progress_updates[0] == ("Inspecting paper pages...", 0, 2)
    assert [update[1] for update in progress_updates[1:-1]] == [1, 2]
    assert {update[0] for update in progress_updates[1:-1]} == {"Paper One", "Paper Two"}
    assert progress_updates[-1] == ("Complete", 1, 1)
    event_names = [event for event, _ in diagnostic_events]
    assert "year_start" in event_names
    assert "sessions_loaded" in event_names
    assert "event_details_loaded" in event_names
    assert "download_complete" in event_names
    assert event_names[-1] == "year_complete"


def test_downloader_overlaps_requests_within_concurrency_limit(tmp_path: Path):
    base = "https://digitalcommons.usu.edu/smallsat/"
    client = TrackingClient(
        text={
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_ONE_HTML,
            f"{base}2025/all2025/2/": EVENT_TWO_HTML,
        }
    )
    downloader = SmallSatDownloader(
        client=client,
        concurrency=2,
        delay=0,
        log=lambda _: None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert client.max_active_requests == 2


def test_pdf_downloads_are_serial():
    urls = [f"https://example.test/{index}.pdf" for index in range(4)]
    client = TrackingClient(text={}, content={url: b"%PDF" for url in urls})
    downloader = SmallSatDownloader(
        client=client,
        concurrency=8,
        delay=0,
        downloads_per_minute=1_000_000,
        log=lambda _: None,
    )

    async def fetch_all():
        await asyncio.gather(
            *(
                downloader._get_bytes_once(
                    url,
                    DownloadStats(),
                    retry_phase=False,
                )
                for url in urls
            )
        )

    asyncio.run(fetch_all())

    assert client.max_active_requests == 1


def test_first_pass_defers_rate_limit_and_keeps_moving(monkeypatch, tmp_path: Path):
    async def no_wait(_delay: float) -> None:
        pass

    monkeypatch.setattr("smallsat_dl.scraper.asyncio.sleep", no_wait)
    base = "https://digitalcommons.usu.edu/smallsat/"
    paper_one_url = f"{base}2025/all2025/1/paper.pdf"
    paper_two_url = f"{base}2025/all2025/2/paper.pdf"
    client = DeferredThenSuccessClient(
        text={
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_ONE_PAPER_ONLY_HTML,
            f"{base}2025/all2025/2/": EVENT_TWO_HTML,
        },
        outcomes={
            paper_one_url: [403, b"paper one"],
            paper_two_url: [b"paper two"],
        },
    )
    downloader = SmallSatDownloader(
        client=client,
        concurrency=2,
        retry_rounds=1,
        delay=0,
        downloads_per_minute=1_000_000,
        retry_backoff=0,
        log=lambda _: None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path))

    assert client.download_calls == [paper_one_url, paper_one_url, paper_two_url]
    assert stats.first_pass_downloaded == 0
    assert stats.first_pass_deferred == 2
    assert stats.retry_recovered == 2
    assert stats.downloaded == 2
    assert stats.retry_rounds == 1
    assert not stats.failures


def test_deferred_pdf_retries_in_later_rounds(monkeypatch, tmp_path: Path):
    url = "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=1&context=smallsat"
    request = httpx.Request("GET", url)
    initial_response = httpx.Response(403, request=request)
    initial_error = httpx.HTTPStatusError(
        "forbidden",
        request=request,
        response=initial_response,
    )
    async_client = SequencedAsyncClient(
        [
            httpx.Response(403, request=request),
            httpx.Response(
                200,
                request=request,
                headers={"content-type": "application/pdf"},
                content=b"%PDF",
            ),
        ]
    )
    delays: list[float] = []
    progress_updates: list[tuple[str, int, int]] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("smallsat_dl.scraper.asyncio.sleep", fake_sleep)
    client = HttpClient(client=async_client)  # type: ignore[arg-type]
    downloader = SmallSatDownloader(
        client=client,
        concurrency=1,
        retry_rounds=2,
        delay=0,
        downloads_per_minute=1_000_000,
        retry_backoff=0,
        log=lambda _: None,
        progress=lambda title, completed, total: progress_updates.append(
            (title, completed, total)
        ),
    )

    stats = DownloadStats()
    deferred = [
        DeferredDownload(
            DownloadJob(url, tmp_path / "paper.pdf"),
            initial_error,
        )
    ]
    asyncio.run(downloader._retry_deferred_downloads(deferred, stats))

    assert async_client.calls == 2
    assert not delays
    assert stats.retry_attempts == 2
    assert stats.retry_recovered == 1
    assert stats.retry_rounds == 2
    assert not stats.failures
    assert (tmp_path / "paper.pdf").read_bytes() == b"%PDF"
    assert progress_updates[-1] == ("Complete", 1, 1)


def test_polite_delay_adds_up_to_full_random_jitter(monkeypatch):
    bounds: list[tuple[float, float]] = []

    def fake_uniform(lower: float, upper: float) -> float:
        bounds.append((lower, upper))
        return 0

    monkeypatch.setattr("smallsat_dl.scraper.random.uniform", fake_uniform)
    downloader = SmallSatDownloader(client=FakeClient({}), delay=0.25, log=lambda _: None)

    asyncio.run(downloader._polite_delay())

    assert bounds == [(0.25, 0.5)]


def test_default_download_meter_has_no_burst_and_two_second_interval():
    downloader = SmallSatDownloader(client=FakeClient({}), log=lambda _: None)

    assert downloader.downloads_per_minute == 30
    assert downloader._download_meter.max_rate == 1
    assert downloader._download_meter.time_period == 2


def test_download_meter_has_no_burst_and_fixed_rate():
    downloader = SmallSatDownloader(
        client=FakeClient({}),
        downloads_per_minute=15,
        log=lambda _: None,
    )

    assert downloader._download_meter.max_rate == 1
    assert downloader._download_meter.time_period == 4


def test_retry_rounds_use_fixed_jittered_cooldown(monkeypatch):
    monkeypatch.setattr(
        "smallsat_dl.scraper.random.uniform",
        lambda lower, upper: (lower + upper) / 2,
    )
    downloader = SmallSatDownloader(
        client=FakeClient({}),
        retry_backoff=60,
        log=lambda _: None,
    )

    assert downloader._retry_round_delay() == 60
    assert downloader._retry_round_delay() == 60


def test_missing_pdf_is_counted_as_unavailable_not_failure(tmp_path: Path):
    base = "https://digitalcommons.usu.edu/smallsat/"
    client = FakeClient(
        text={
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_WITHOUT_PDF_HTML,
            f"{base}2025/all2025/2/": EVENT_TWO_HTML,
        }
    )
    downloader = SmallSatDownloader(client=client, delay=0, log=lambda _: None)

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert stats.unavailable == 1
    assert stats.file_targets == 1
    assert stats.dry_run == 1
    assert not stats.failures


def test_presentation_is_downloaded_when_paper_pdf_is_unavailable(tmp_path: Path):
    detail = EventDetail(
        title="Presentation Only",
        pdf_url=None,
        presentation_url="https://example.test/presentation.pdf",
    )
    downloader = SmallSatDownloader(client=FakeClient({}), delay=0, log=lambda _: None)

    stats, deferred = asyncio.run(
        downloader._download_event_files(
            2025,
            Link("Flash Talk Session 5", "https://example.test/session"),
            detail,
            tmp_path,
            dry_run=True,
        )
    )

    assert stats.unavailable == 1
    assert stats.file_targets == 1
    assert stats.dry_run == 1
    assert not stats.failures
    assert not deferred


def test_duplicate_titles_in_same_session_are_scheduled_once(tmp_path: Path):
    base = "https://digitalcommons.usu.edu/smallsat/"
    session_url = f"{base}2025/RA-S7-2025/"
    first_event_url = f"{base}2025/RA-S7-2025/1"
    second_event_url = f"{base}2025/all2025/11"
    duplicate_session_html = f"""
    <table>
      <tr><td class="event"><p class="summary"><a href="{first_event_url}">Same Paper</a></p></td></tr>
      <tr><td class="event"><p class="summary"><a href="{second_event_url}">Same Paper</a></p></td></tr>
    </table>
    """
    client = FakeClient(
        text={
            f"{base}2025/": f"""
                <dl id="events-listing">
                  <a title="Research Session" href="{session_url}">Research Session</a>
                </dl>
            """,
            session_url: duplicate_session_html,
            first_event_url: EVENT_TWO_HTML,
        }
    )
    downloader = SmallSatDownloader(client=client, delay=0, log=lambda _: None)

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert stats.file_targets == 1
    assert stats.dry_run == 1
    assert not stats.failures


def test_discover_latest_year_skips_newer_archive_without_pdfs(monkeypatch):
    class AugustFirst2026(dt.date):
        @classmethod
        def today(cls):
            return cls(2026, 8, 1)

    monkeypatch.setattr("smallsat_dl.scraper.dt.date", AugustFirst2026)
    base = "https://digitalcommons.usu.edu/smallsat/"
    client = TrackingClient(
        text={
            base: "<html><body>No year links here</body></html>",
            f"{base}2026/": YEAR_2026_HTML,
            f"{base}2026/all2026/": EMPTY_SESSION_HTML,
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_ONE_HTML,
        }
    )
    downloader = SmallSatDownloader(client=client, delay=0, log=lambda _: None)

    assert asyncio.run(downloader.discover_latest_year()) == 2025
    assert client.max_active_requests >= 2


def test_discover_latest_year_skips_current_year_before_august(monkeypatch):
    class July2026(dt.date):
        @classmethod
        def today(cls):
            return cls(2026, 7, 31)

    monkeypatch.setattr("smallsat_dl.scraper.dt.date", July2026)
    base = "https://digitalcommons.usu.edu/smallsat/"
    client = TrackingClient(
        text={
            f"{base}2025/": YEAR_HTML,
            f"{base}2025/all2025/": SESSION_HTML,
            f"{base}2025/all2025/1/": EVENT_ONE_HTML,
        }
    )
    downloader = SmallSatDownloader(client=client, delay=0, log=lambda _: None)

    assert asyncio.run(downloader.discover_latest_year()) == 2025
    assert f"{base}2026/" not in client.requested_urls
