import asyncio
import datetime as dt
from pathlib import Path

import httpx
import pytest

import smallsat_dl.cli as cli
import smallsat_dl.scraper as scraper
from smallsat_dl.scraper import HttpClient, SmallSatDownloader, parse_event_links


BASE = "https://archive.test/smallsat/"
YEAR_URL = f"{BASE}2025/"
CATEGORY_URL = f"{YEAR_URL}science/"
AGGREGATE_URL = f"{YEAR_URL}all2025/"
SHARED_URL = f"{AGGREGATE_URL}1/"
AGGREGATE_ONLY_URL = f"{AGGREGATE_URL}2/"
YEAR_HTML = """
<dl id="events-listing">
  <a href="all2025/">All 2025 Content</a>
  <a href="science/">Science</a>
</dl>
"""
EMPTY_SESSION_HTML = '<table class="vcalendar"><caption>Schedule</caption></table>'
CHALLENGE_HTML = "<html><title>Just a moment</title><p>Verify you are human</p></html>"


def session_html(*events: tuple[str, str]) -> str:
    return '<table class="vcalendar">' + "".join(
        f'<tr><td class="event"><p class="summary"><a href="{url}">{title}</a></p></td></tr>'
        for url, title in events
    ) + "</table>"


def event_html(name: str) -> str:
    return f'<a id="pdf" href="/{name}.pdf">Download</a>'


class FakeClient:
    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages
        self.requests: list[str] = []

    async def get_text(self, url: str) -> str:
        self.requests.append(url)
        return self.pages[url]


def downloader_for(pages: dict[str, str]) -> SmallSatDownloader:
    return SmallSatDownloader(
        client=FakeClient(pages),
        base_url=BASE,
        delay=0,
        retry_rounds=0,
        log=lambda _: None,
    )


def test_aggregate_adds_missing_events_and_keeps_shared_event_in_category(tmp_path: Path):
    client = FakeClient({
        YEAR_URL: YEAR_HTML,
        CATEGORY_URL: session_html((SHARED_URL, "Shared Paper")),
        AGGREGATE_URL: session_html(
            (SHARED_URL.rstrip("/"), "Shared Paper"),
            (AGGREGATE_ONLY_URL, "Aggregate Only Paper"),
        ),
        SHARED_URL: event_html("shared"),
        AGGREGATE_ONLY_URL: event_html("aggregate-only"),
    })
    events: list[dict[str, object]] = []
    downloader = SmallSatDownloader(
        client=client,
        base_url=BASE,
        delay=0,
        retry_rounds=0,
        log=lambda _: None,
        diagnostic=lambda event, fields: events.append(fields)
        if event == "download_dry_run" else None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.dry_run == 2
    assert client.requests.count(SHARED_URL) == 1
    assert SHARED_URL.rstrip("/") not in client.requests
    assert {Path(event["destination"]).relative_to(tmp_path) for event in events} == {
        Path("2025/Science/Shared Paper.pdf"),
        Path("2025/All 2025 Content/Aggregate Only Paper.pdf"),
    }


def test_latest_year_includes_aggregate_only_pdfs(monkeypatch):
    class August2025(dt.date):
        @classmethod
        def today(cls):
            return cls(2025, 8, 1)

    monkeypatch.setattr(scraper.dt, "date", August2025)
    downloader = downloader_for({
        YEAR_URL: YEAR_HTML,
        CATEGORY_URL: EMPTY_SESSION_HTML,
        AGGREGATE_URL: session_html((AGGREGATE_ONLY_URL, "Aggregate Only Paper")),
        AGGREGATE_ONLY_URL: event_html("aggregate-only"),
    })

    assert asyncio.run(downloader.discover_latest_year()) == 2025


@pytest.mark.parametrize("same_files", [True, False])
def test_aggregate_alias_uses_file_identity_instead_of_title(tmp_path: Path, same_files: bool):
    category_event_url = f"{CATEGORY_URL}1/"
    client = FakeClient({
        YEAR_URL: YEAR_HTML,
        CATEGORY_URL: session_html((category_event_url, "Same Title")),
        AGGREGATE_URL: session_html((AGGREGATE_ONLY_URL, "Same Title")),
        category_event_url: event_html("category"),
        AGGREGATE_ONLY_URL: event_html("category" if same_files else "different"),
    })
    destinations: list[Path] = []
    downloader = SmallSatDownloader(
        client=client,
        base_url=BASE,
        delay=0,
        retry_rounds=0,
        log=lambda _: None,
        diagnostic=lambda event, fields: destinations.append(Path(fields["destination"]))
        if event == "download_dry_run" else None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.dry_run == (1 if same_files else 2)
    assert destinations[0] == tmp_path / "2025/Science/Same Title.pdf"
    if not same_files:
        assert destinations[1] == tmp_path / "2025/All 2025 Content/Same Title.pdf"


def test_recognized_empty_session_is_valid(tmp_path: Path):
    downloader = downloader_for({
        YEAR_URL: YEAR_HTML,
        CATEGORY_URL: EMPTY_SESSION_HTML,
        AGGREGATE_URL: EMPTY_SESSION_HTML,
    })

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert parse_event_links(EMPTY_SESSION_HTML, CATEGORY_URL) == []
    assert not stats.failures
    assert stats.file_targets == stats.unavailable == 0


def test_recognized_empty_year_is_valid(tmp_path: Path):
    downloader = downloader_for({YEAR_URL: '<dl id="events-listing"></dl>'})

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.file_targets == 0


@pytest.mark.parametrize("stage", ["year", "session", "event"])
def test_http_200_challenge_causes_cli_failure(monkeypatch, tmp_path: Path, capsys, stage: str):
    pages = {
        BASE: f'<a href="{YEAR_URL}">2025</a>',
        YEAR_URL: '<dl id="events-listing"><a href="science/">Science</a></dl>',
        CATEGORY_URL: session_html((SHARED_URL, "Shared Paper")),
        SHARED_URL: event_html("shared"),
    }
    failed_url = {"year": YEAR_URL, "session": CATEGORY_URL, "event": SHARED_URL}[stage]
    pages[failed_url] = CHALLENGE_HTML

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=pages[str(request.url)], request=request)

    monkeypatch.setattr(scraper, "HttpClient", lambda **kwargs: HttpClient(
        client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
    ))

    result = asyncio.run(cli._async_main([
        "--year", "2025", "--base-url", BASE, "--output", str(tmp_path),
        "--dry-run", "--delay", "0", "--retry-rounds", "0",
    ]))

    output = capsys.readouterr()
    error_text = " ".join(output.err.split())
    assert result == 1
    assert f"Unrecognized SmallSat {stage} page" in error_text
    assert failed_url in error_text
    assert "1 failure(s)" in output.out


def test_missing_downloads_on_a_recognized_event_remain_unavailable(tmp_path: Path):
    downloader = downloader_for({
        YEAR_URL: '<dl id="events-listing"><a href="science/">Science</a></dl>',
        CATEGORY_URL: session_html((SHARED_URL, "Shared Paper")),
        SHARED_URL: '<meta name="bepress_citation_title" content="Shared Paper">',
    })

    stats = asyncio.run(downloader.download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.unavailable == 1


@pytest.mark.parametrize("stage", ["year", "session", "event"])
def test_latest_does_not_hide_unrecognized_pages(monkeypatch, stage: str):
    class August2025(dt.date):
        @classmethod
        def today(cls):
            return cls(2025, 8, 1)

    monkeypatch.setattr(scraper.dt, "date", August2025)
    pages = {
        YEAR_URL: '<dl id="events-listing"><a href="science/">Science</a></dl>',
        CATEGORY_URL: session_html((SHARED_URL, "Shared Paper")),
        SHARED_URL: event_html("shared"),
        f"{BASE}2024/": '<dl id="events-listing"><a href="all2024/">All 2024 Content</a></dl>',
        f"{BASE}2024/all2024/": session_html((f"{BASE}2024/all2024/1/", "Older Paper")),
        f"{BASE}2024/all2024/1/": event_html("older"),
    }
    pages[{"year": YEAR_URL, "session": CATEGORY_URL, "event": SHARED_URL}[stage]] = CHALLENGE_HTML

    with pytest.raises(scraper.InvalidArchivePage, match=f"Unrecognized SmallSat {stage} page"):
        asyncio.run(downloader_for(pages).discover_latest_year())
