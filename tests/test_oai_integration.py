import asyncio
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

import httpx
import pytest

import smallsat_dl.cli as cli
import smallsat_dl.scraper as scraper
from smallsat_dl.cache import MetadataCache
from smallsat_dl.oai import OaiProtocolError, OaiRecord, OaiUnavailable, records_from_json
from smallsat_dl.scraper import Link, SmallSatDownloader


BASE = "https://archive.test/smallsat/"
ENDPOINT = "https://archive.test/do/oai/"
YEAR_URL = f"{BASE}2025/"
CATEGORY_URL = f"{YEAR_URL}science/"
AGGREGATE_URL = f"{YEAR_URL}all2025/"
EVENT_URL = f"{AGGREGATE_URL}1"
EXTRA_URL = f"{AGGREGATE_URL}2"
SNAPSHOT_KEY = f"{ENDPOINT}#smallsat-dl:oai-v1:publication:smallsat/2025"
YEAR_HTML = '<dl id="events-listing"><a href="all2025/">All 2025 Content</a><a href="science/">Science</a></dl>'
CHALLENGE_HTML = "<html><title>Verify you are human</title></html>"


def make_record(number=1, *, title="Listed Paper", event_url=EVENT_URL,
                files=True, supplemental=False):
    return OaiRecord(
        identifier=f"oai:archive.test:smallsat-{number}", title=title,
        event_url=event_url, year=2025,
        primary_url=f"https://archive.test/context/smallsat/article/{number}/viewcontent/Paper.pdf"
        if files else None,
        supplemental_urls=(
            f"https://archive.test/context/smallsat/article/{number}/filename/0/"
            "type/additional/viewcontent/Mission%20Slides.PPTX",
        ) if supplemental else (),
        formats=("application/pdf",) if files else (),
    )


def oai_xml(records, *, token=None, size=None):
    body = []
    for record in records:
        urls = [record.event_url]
        if record.primary_url is not None:
            urls.append(record.primary_url)
        urls.extend(record.supplemental_urls)
        identifiers = "".join(f"<dc:identifier>{escape(url)}</dc:identifier>" for url in urls)
        body.append(
            f"<record><header><identifier>{record.identifier}</identifier></header><metadata>"
            '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/">'
            f"<dc:title>{escape(record.title)}</dc:title>{identifiers}</oai_dc:dc></metadata></record>"
        )
    size_attr = f' completeListSize="{size}"' if size is not None else ""
    resumption = f"<resumptionToken{size_attr}>{escape(token)}</resumptionToken>" if token is not None else ""
    return (
        '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        f'<ListRecords>{"".join(body)}{resumption}</ListRecords></OAI-PMH>'
    )


def no_records_xml():
    return (
        '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        '<error code="noRecordsMatch">No matching records</error></OAI-PMH>'
    )


def session_html(events):
    return '<table class="vcalendar">' + "".join(
        f'<tr><td class="event"><p class="summary"><a href="{url}">{title}</a></p></td></tr>'
        for url, title in events
    ) + "</table>"


def html_pages(*, title="Listed Paper"):
    listing = session_html([(EVENT_URL, title)])
    return {
        BASE: f'<a href="{YEAR_URL}">2025 archive</a>',
        YEAR_URL: YEAR_HTML,
        CATEGORY_URL: listing,
        AGGREGATE_URL: listing,
        EVENT_URL: '<a id="pdf" href="https://archive.test/html-paper.pdf">Download</a>',
    }


class FakeClient:
    def __init__(self, *, html=None, pages=None):
        self.html = html or {}
        self.pages = pages or {}
        self.requests: list[str] = []
        self.closed = False

    async def get_text(self, url):
        self.requests.append(url)
        if urlsplit(url).path == "/do/oai/":
            token = parse_qs(urlsplit(url).query).get("resumptionToken", ["initial"])[0]
            result = self.pages[token]
        else:
            result = self.html[url]
        if isinstance(result, Exception):
            raise result
        return result

    async def get_bytes(self, url):
        raise AssertionError("these discovery tests must not download files")

    async def close(self):
        self.closed = True

    @property
    def oai_requests(self):
        return [url for url in self.requests if urlsplit(url).path == "/do/oai/"]


def downloader_for(client, *, cache=None, discovery="oai", diagnostics=None):
    return SmallSatDownloader(
        client=client, base_url=BASE, delay=0, retry_rounds=0,
        discovery=discovery, cache=cache, log=lambda _: None,
        diagnostic=(lambda event, fields: diagnostics.append((event, fields)))
        if diagnostics is not None else None,
    )


def test_year_feed_paginates_and_avoids_event_html_while_preserving_paths(tmp_path: Path):
    listed = make_record(supplemental=True)
    extra = make_record(2, title="OAI Only Paper", event_url=EXTRA_URL)
    client = FakeClient(html=html_pages(), pages={
        "initial": oai_xml([listed], token="next/page", size=2),
        "next/page": oai_xml([extra], token="", size=2),
    })
    diagnostics = []

    stats = asyncio.run(downloader_for(client, diagnostics=diagnostics).download_year(
        2025, tmp_path, dry_run=True,
    ))

    assert not stats.failures
    assert stats.dry_run == 3
    assert EVENT_URL not in client.requests and EXTRA_URL not in client.requests
    assert len(client.oai_requests) == 2
    assert parse_qs(urlsplit(client.oai_requests[0]).query)["set"] == ["publication:smallsat/2025"]
    assert parse_qs(urlsplit(client.oai_requests[1]).query) == {
        "verb": ["ListRecords"], "resumptionToken": ["next/page"],
    }
    destinations = {
        Path(fields["destination"]).relative_to(tmp_path)
        for event, fields in diagnostics if event == "download_dry_run"
    }
    assert destinations == {
        Path("2025/Science/Listed Paper.pdf"),
        Path("2025/Science/presentations/Listed Paper.pptx"),
        Path("2025/All 2025 Content/OAI Only Paper.pdf"),
    }


@pytest.mark.parametrize("missing", [True, False])
def test_missing_or_insufficient_oai_metadata_uses_event_html(tmp_path: Path, missing):
    client = FakeClient(html=html_pages(), pages={
        "initial": no_records_xml() if missing else oai_xml([make_record(files=False)]),
    })

    stats = asyncio.run(downloader_for(client).download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.dry_run == 1
    assert EVENT_URL in client.requests


@pytest.mark.parametrize("unique_title", [True, False])
def test_unmatched_alias_titles_use_html_but_exact_url_keeps_its_record(unique_title):
    second = make_record(2, title="Listed Paper", event_url=EXTRA_URL)
    alias_url = f"{CATEGORY_URL}99"
    client = FakeClient(html={alias_url: '<a id="pdf" href="/alias-correct.pdf">Download</a>'}, pages={
        "initial": oai_xml([make_record()] if unique_title else [make_record(), second]),
    })
    downloader = downloader_for(client)

    async def inspect():
        await downloader._records_for_year(2025)
        exact = await downloader._load_event_detail(Link("Listed Paper", EVENT_URL))
        alias = await downloader._load_event_detail(Link("Listed Paper", alias_url))
        return exact, alias

    exact, alias = asyncio.run(inspect())

    assert exact.pdf_url == make_record().primary_url
    assert alias.pdf_url == "https://archive.test/alias-correct.pdf"
    assert EVENT_URL not in client.requests
    assert alias_url in client.requests


def test_distinct_same_title_html_paper_missing_from_oai_keeps_its_own_file(tmp_path: Path):
    pages = html_pages()
    pages[AGGREGATE_URL] = session_html([
        (EVENT_URL, "Listed Paper"), (EXTRA_URL, "Listed Paper"),
    ])
    distinct_pdf_url = "https://archive.test/distinct-paper.pdf"
    pages[EXTRA_URL] = f'<a id="pdf" href="{distinct_pdf_url}">Download</a>'
    contents = {
        make_record().primary_url: b"%PDF-1.7\nOAI paper\n%%EOF",
        distinct_pdf_url: b"%PDF-1.7\ndistinct HTML paper\n%%EOF",
    }

    class FileClient(FakeClient):
        async def get_bytes(self, url):
            return contents[url]

    client = FileClient(html=pages, pages={"initial": oai_xml([make_record()])})
    downloader = SmallSatDownloader(
        client=client, base_url=BASE, discovery="oai", delay=0,
        downloads_per_minute=1_000_000, retry_rounds=0, log=lambda _: None,
    )

    stats = asyncio.run(downloader.download_year(2025, tmp_path))

    assert not stats.failures
    assert stats.file_targets == stats.downloaded == 2
    assert EVENT_URL not in client.requests
    assert EXTRA_URL in client.requests
    assert (tmp_path / "2025/Science/Listed Paper.pdf").read_bytes() == contents[make_record().primary_url]
    assert (tmp_path / "2025/All 2025 Content/Listed Paper.pdf").read_bytes() == contents[distinct_pdf_url]


def test_oai_only_distinct_same_title_paper_is_not_lost(tmp_path: Path):
    client = FakeClient(html=html_pages(), pages={
        "initial": oai_xml([make_record(), make_record(2, event_url=EXTRA_URL)]),
    })

    stats = asyncio.run(downloader_for(client).download_year(2025, tmp_path, dry_run=True))

    assert not stats.failures
    assert stats.dry_run == 2
    assert EVENT_URL not in client.requests and EXTRA_URL not in client.requests


def test_complete_snapshot_and_html_metadata_are_reused_without_network(tmp_path: Path):
    path = tmp_path / "cache.sqlite3"
    first = FakeClient(html=html_pages(), pages={"initial": oai_xml([make_record()])})
    cache = MetadataCache(path)
    first_stats = asyncio.run(downloader_for(first, cache=cache).download_year(
        2025, tmp_path / "output", dry_run=True,
    ))
    assert not first_stats.failures
    snapshot = cache.get(SNAPSHOT_KEY)
    assert snapshot is not None
    assert len(records_from_json(snapshot)) == 1
    assert all(cache.get(url) is None for url in first.oai_requests)
    cache.close()

    second = FakeClient()
    second_cache = MetadataCache(path)
    stats = asyncio.run(downloader_for(second, cache=second_cache).download_year(
        2025, tmp_path / "output", dry_run=True,
    ))
    second_cache.close()

    assert not stats.failures
    assert stats.dry_run == 1
    assert second.requests == []


@pytest.mark.parametrize("failure", ["transport", "premature_end"])
def test_partial_oai_pages_never_become_a_cached_snapshot(tmp_path: Path, failure):
    cache = MetadataCache(tmp_path / "cache.sqlite3")
    pages = {"initial": oai_xml([make_record()], token="next", size=2)}
    pages["next"] = TimeoutError("interrupted harvest") if failure == "transport" else oai_xml([])
    client = FakeClient(pages=pages)
    downloader = downloader_for(client, cache=cache)

    with pytest.raises((OaiUnavailable, OaiProtocolError)):
        asyncio.run(downloader._records_for_year(2025))

    assert cache.get(SNAPSHOT_KEY) is None
    assert all(cache.get(url) is None for url in client.oai_requests)
    retry = FakeClient(pages={
        "initial": oai_xml([make_record()], token="next", size=2),
        "next": oai_xml([make_record(2, event_url=EXTRA_URL)], token="", size=2),
    })
    records = asyncio.run(downloader_for(retry, cache=cache)._records_for_year(2025))
    assert len(records) == 2
    assert len(retry.oai_requests) == 2
    assert "resumptionToken" not in parse_qs(urlsplit(retry.oai_requests[0]).query)
    assert len(records_from_json(cache.get(SNAPSHOT_KEY))) == 2
    cache.close()


@pytest.mark.parametrize("http_error", [False, True])
def test_challenge_or_http_error_is_never_cached_as_event_metadata(tmp_path: Path, http_error):
    cache = MetadataCache(tmp_path / "cache.sqlite3")
    response = httpx.Response(403, request=httpx.Request("GET", EVENT_URL))
    error = httpx.HTTPStatusError("forbidden", request=response.request, response=response)
    client = FakeClient(html={EVENT_URL: error if http_error else CHALLENGE_HTML})
    downloader = downloader_for(client, cache=cache, discovery="html")

    async def inspect_twice():
        for _ in range(2):
            with pytest.raises((scraper.InvalidArchivePage, httpx.HTTPStatusError)):
                await downloader._load_event_detail(Link("Paper", EVENT_URL))

    asyncio.run(inspect_twice())

    assert client.requests == [EVENT_URL, EVENT_URL]
    assert cache.get(EVENT_URL) is None
    cache.close()


def test_corrupt_snapshot_is_invalidated_and_refetched_in_strict_mode(tmp_path: Path):
    cache = MetadataCache(tmp_path / "cache.sqlite3")
    cache.put(SNAPSHOT_KEY, '{"version": 1, "records": ["partial"]}')
    client = FakeClient(pages={"initial": oai_xml([make_record()])})

    records = asyncio.run(downloader_for(client, cache=cache)._records_for_year(2025))

    assert len(records) == 1
    assert len(client.oai_requests) == 1
    assert records_from_json(cache.get(SNAPSHOT_KEY)) == records
    cache.close()


def test_mixed_year_feed_keeps_complete_cache_but_only_schedules_requested_year(tmp_path: Path):
    foreign_url = f"{BASE}2026/all2026/134"
    foreign = OaiRecord(
        identifier="oai:archive.test:smallsat-6547", title="Moved to 2026",
        event_url=foreign_url, year=2026,
        primary_url="https://archive.test/context/smallsat/article/6547/viewcontent/SSC26.pdf",
    )
    cache_path = tmp_path / "cache.sqlite3"
    first = FakeClient(html=html_pages(), pages={
        "initial": oai_xml([make_record(), foreign], token="", size=2),
    })
    cache = MetadataCache(cache_path)
    first_stats = asyncio.run(downloader_for(first, cache=cache).download_year(
        2025, tmp_path / "output", dry_run=True,
    ))

    assert not first_stats.failures
    assert first_stats.dry_run == 1
    assert foreign_url not in first.requests
    assert {record.year for record in records_from_json(cache.get(SNAPSHOT_KEY))} == {2025, 2026}
    cache.close()

    second = FakeClient()
    cache = MetadataCache(cache_path)
    second_stats = asyncio.run(downloader_for(second, cache=cache).download_year(
        2025, tmp_path / "output", dry_run=True,
    ))

    assert not second_stats.failures
    assert second_stats.dry_run == 1
    assert second.requests == []
    cache.close()


@pytest.mark.parametrize("mode", ["auto", "oai", "html"])
def test_cli_auto_fallback_strict_oai_failure_and_html_only(monkeypatch, tmp_path: Path, capsys, mode):
    client = FakeClient(html=html_pages(), pages={"initial": CHALLENGE_HTML})
    monkeypatch.setattr(scraper, "HttpClient", lambda **kwargs: client)

    result = asyncio.run(cli._async_main([
        "--year", "2025", "--base-url", BASE, "--output", str(tmp_path),
        "--dry-run", "--no-cache", "--delay", "0", "--discovery", mode,
    ]))

    output = capsys.readouterr()
    assert result == (1 if mode == "oai" else 0)
    assert client.closed
    if mode == "html":
        assert not client.oai_requests
    else:
        assert len(client.oai_requests) == 1
    if mode == "auto":
        assert "using HTML discovery" in " ".join(output.out.split())
    if mode == "oai":
        assert "OAI-PMH" in output.err
        assert EVENT_URL not in client.requests
