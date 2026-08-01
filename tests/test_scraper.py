import asyncio
import datetime as dt
from pathlib import Path

import httpx

from smallsat_dl.scraper import (
    AdaptiveConcurrencyLimiter,
    DeferredDownload,
    DownloadJob,
    DownloadStats,
    HttpClient,
    Link,
    SmallSatDownloader,
    parse_available_years,
    parse_event_detail,
    parse_event_links,
    parse_session_links,
    prefer_categorized_sessions,
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

EVENT_WITHOUT_PDF_HTML = "<html><body><p>No paper file</p></body></html>"

YEAR_2026_HTML = """
<dl id="events-listing">
  <a title="All 2026 Content" href="all2026/">All 2026 Content</a>
</dl>
"""

EMPTY_SESSION_HTML = "<table></table>"


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


def test_prefer_categorized_sessions_drops_aggregate_listing():
    sessions = [
        Link("All 2025 Content", "https://example.test/all"),
        Link("Keynote 2025", "https://example.test/keynote"),
    ]

    assert prefer_categorized_sessions(sessions) == [sessions[1]]


def test_parse_event_links_extracts_summary_links():
    events = parse_event_links(SESSION_HTML, "https://digitalcommons.usu.edu/smallsat/2025/all2025/")
    assert [event.title for event in events] == ["Paper One", "Paper Two"]
    assert events[0].url == "https://digitalcommons.usu.edu/smallsat/2025/all2025/1/"


def test_parse_event_detail_handles_missing_presentation():
    detail = parse_event_detail(
        EVENT_TWO_HTML,
        "https://digitalcommons.usu.edu/smallsat/2025/all2025/2/",
        "Paper Two",
        "https://digitalcommons.usu.edu/smallsat/2025/all2025/2/",
    )
    assert detail.pdf_url == "https://digitalcommons.usu.edu/smallsat/2025/all2025/2/paper.pdf"
    assert detail.presentation_url is None


def test_downloader_skips_existing_files_and_downloads_missing_targets(tmp_path: Path):
    base = "https://digitalcommons.usu.edu/smallsat/"
    progress_updates: list[tuple[str, int, int]] = []
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
        download_delay=0,
        log=lambda _: None,
        progress=lambda title, completed, total: progress_updates.append(
            (title, completed, total)
        ),
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path))

    assert stats.downloaded == 2
    assert stats.skipped == 1
    assert not stats.failures
    assert (tmp_path / "2025" / "All 2025 Content" / "presentations" / "Paper One.pdf").read_bytes() == b"slides"
    assert (tmp_path / "2025" / "All 2025 Content" / "Paper Two.pdf").read_bytes() == b"paper two"
    assert not (tmp_path / "2025" / "All 2025 Content" / "presentations" / "Paper Two.pdf").exists()
    assert progress_updates[0] == ("Inspecting paper pages...", 0, 2)
    assert [update[1] for update in progress_updates[1:]] == [1, 2]
    assert {update[0] for update in progress_updates[1:]} == {"Paper One", "Paper Two"}


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


def test_pdf_downloads_use_separate_lower_concurrency_limit():
    urls = [f"https://example.test/{index}.pdf" for index in range(4)]
    client = TrackingClient(text={}, content={url: b"%PDF" for url in urls})
    downloader = SmallSatDownloader(
        client=client,
        concurrency=8,
        download_concurrency=2,
        delay=0,
        download_delay=0,
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

    assert client.max_active_requests == 2


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
        download_concurrency=1,
        retry_rounds=1,
        delay=0,
        download_delay=0,
        retry_backoff=0,
        log=lambda _: None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path))

    assert client.download_calls == [paper_one_url, paper_two_url, paper_one_url]
    assert stats.first_pass_downloaded == 1
    assert stats.first_pass_deferred == 1
    assert stats.retry_recovered == 1
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

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("smallsat_dl.scraper.asyncio.sleep", fake_sleep)
    client = HttpClient(client=async_client)  # type: ignore[arg-type]
    downloader = SmallSatDownloader(
        client=client,
        concurrency=1,
        download_concurrency=1,
        retry_rounds=2,
        delay=0,
        download_delay=0,
        retry_backoff=0,
        log=lambda _: None,
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
    assert len(delays) == 2
    assert stats.retryable_errors == 1
    assert stats.rate_limits == 1
    assert stats.retry_attempts == 2
    assert stats.retry_recovered == 1
    assert stats.retry_rounds == 2
    assert not stats.failures
    assert (tmp_path / "paper.pdf").read_bytes() == b"%PDF"


def test_adaptive_download_concurrency_uses_aimd():
    changes: list[tuple[int, str]] = []
    limiter = AdaptiveConcurrencyLimiter(
        8,
        increase_after=2,
        decrease_cooldown=0,
        on_change=lambda limit, direction: changes.append((limit, direction)),
    )

    async def exercise_limiter():
        await limiter.record_rate_limit()
        await limiter.record_success()
        await limiter.record_success()

    asyncio.run(exercise_limiter())

    assert limiter.limit == 5
    assert changes == [(4, "decrease"), (5, "increase")]


def test_polite_delay_adds_up_to_full_random_jitter(monkeypatch):
    bounds: list[tuple[float, float]] = []

    def fake_uniform(lower: float, upper: float) -> float:
        bounds.append((lower, upper))
        return 0

    monkeypatch.setattr("smallsat_dl.scraper.random.uniform", fake_uniform)
    downloader = SmallSatDownloader(client=FakeClient({}), delay=0.25, log=lambda _: None)

    asyncio.run(downloader._polite_delay())

    assert bounds == [(0.25, 0.5)]


def test_download_delay_is_slower_and_jittered(monkeypatch):
    bounds: list[tuple[float, float]] = []

    def fake_uniform(lower: float, upper: float) -> float:
        bounds.append((lower, upper))
        return 0

    monkeypatch.setattr("smallsat_dl.scraper.random.uniform", fake_uniform)
    downloader = SmallSatDownloader(
        client=FakeClient({}),
        download_delay=1.0,
        log=lambda _: None,
    )

    asyncio.run(downloader._download_polite_delay())

    assert bounds == [(1.0, 2.0)]


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
