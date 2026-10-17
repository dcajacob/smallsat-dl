import asyncio
import builtins
import datetime as dt
import hashlib
import json
import sys
from dataclasses import asdict, replace
from types import SimpleNamespace
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
import pytest

from scripts import compare_transports as comparison
from scripts import probe_curl_rates as short_probe
from scripts import probe_curl_sustained as sustained


def pdf(number):
    return f"%PDF-1.7\nPDF #{number:06d}\n%%EOF\n".encode()


def targets():
    return [sustained.PdfTarget(
        f"https://archive.test/context/smallsat/article/{number}/viewcontent/paper{number}.pdf",
        len(pdf(number)), hashlib.sha256(pdf(number)).hexdigest(), f"Paper {number}",
    ) for number in range(150)]


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds
        await asyncio.sleep(0)

    def timestamp(self):
        return (dt.datetime(2026, 10, 17, tzinfo=dt.UTC) + dt.timedelta(seconds=self.now)).isoformat()


class Client:
    def __init__(self, clock, outcomes=None, duration=0.2):
        self.clock = clock
        self.outcomes = outcomes or {}
        self.duration = duration
        self.calls = []
        self.active = 0
        self.max_active = 0

    async def request(self, url, max_bytes):
        self.calls.append((url, self.clock.now, max_bytes))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await self.clock.sleep(self.duration)
            outcome = self.outcomes.get(len(self.calls))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome or comparison.SampleResponse(200, {
                "content-type": "application/pdf", "server": "example", "age": "3",
                "cf-cache-status": "HIT", "x-cache": "HIT", "cache-control": "max-age=60",
                "set-cookie": "private-cookie", "authorization": "private-auth",
            }, pdf(len(self.calls) - 1))
        finally:
            self.active -= 1


def run(client, pdf_targets=None, **options):
    return asyncio.run(sustained.run_probe(
        pdf_targets or targets(), client, monotonic=client.clock.monotonic,
        sleep=client.clock.sleep, timestamp=client.clock.timestamp, **options,
    ))


def test_manifest_accepts_vetted_primary_urls_and_expected_metadata_aliases():
    rows = [asdict(target) for target in targets()]
    assert sustained.parse_manifest({"pdfs": rows}) == targets()
    assert sustained.parse_manifest(rows) == targets()
    aliases = [{"pdf_url": row["url"], "bytes": row["expected_bytes"],
                "sha256": row["expected_sha256"], "title": row["title"]} for row in rows]
    assert sustained.parse_manifest({"pdfs": aliases}) == targets()


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"url": None}, "URL string"),
        ({"url": "http://archive.test/file.pdf"}, "HTTPS"),
        ({"url": "https://user:password@archive.test/file.pdf"}, "credentials"),
        ({"url": "https://archive.test/file.pdf#other"}, "fragments"),
        ({"expected_bytes": 0}, "expected_bytes"),
        ({"expected_bytes": True}, "expected_bytes"),
        ({"expected_bytes": comparison.MAX_BYTES + 1}, "expected_bytes"),
        ({"expected_sha256": "missing"}, "expected_sha256"),
        ({"expected_sha256": None}, "expected_sha256"),
    ],
)
def test_invalid_manifest_metadata_is_rejected_before_execution(changes, message):
    rows = [asdict(target) for target in targets()]
    rows[0].update(changes)
    with pytest.raises(ValueError, match=message):
        sustained.parse_manifest({"pdfs": rows})


def test_manifest_requires_150_distinct_urls_and_hashes():
    values = targets()
    with pytest.raises(ValueError, match="exactly 150"):
        sustained.validate_targets(values[:-1])
    with pytest.raises(ValueError, match="duplicate PDF URLs"):
        sustained.validate_targets([*values[:-1], replace(values[-1], url=values[0].url)])
    with pytest.raises(ValueError, match="duplicate PDF hashes"):
        sustained.validate_targets([
            *values[:-1], replace(values[-1], expected_sha256=values[0].expected_sha256.upper()),
        ])
    values[0] = replace(values[0], url="https://archive.test/file.pdf?context=smallsat&article=42")
    values[-1] = replace(values[-1], url="https://ARCHIVE.test:443/file.pdf?article=42&context=smallsat")
    with pytest.raises(ValueError, match="duplicate PDF URLs"):
        sustained.validate_targets(values)


def test_all_150_expected_pdfs_require_298_seconds_of_global_start_spacing():
    client = Client(Clock())
    emitted = []

    records, summary = run(client, emit=emitted.append)

    assert len(records) == len(client.calls) == 150
    assert client.max_active == 1
    assert [record["started_seconds"] for record in records] == [2 * index for index in range(150)]
    assert {record["gap_since_previous_start_seconds"] for record in records[1:]} == {2}
    assert {record["sha256"] for record in records} == {target.expected_sha256 for target in targets()}
    assert all(record["bytes"] == target.expected_bytes for record, target in zip(records, targets()))
    assert emitted == records
    assert summary["accepted_pdfs"] == 150
    assert summary["actual_requests"] == 150
    assert summary["request_start_span_seconds"] == 298
    assert summary["observed_start_rate_per_minute"] == 30
    assert summary["minimum_start_gap_seconds"] == 2
    assert summary["acceptance"]["technical_acceptance_passed"] is True
    assert summary["elapsed_seconds"] == 298.2
    assert records[0]["cache_headers"] == {
        "age": "3", "cache-control": "max-age=60", "cf-cache-status": "HIT", "x-cache": "HIT",
    }
    assert "private-cookie" not in json.dumps(summary)
    assert "private-auth" not in json.dumps(records)


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [
        (comparison.SampleResponse(403, {"retry-after": "180"}, b""), "http_403"),
        (comparison.SampleResponse(429, {}, b""), "http_429"),
        (comparison.SampleResponse(302, {}, b""), "unexpected_http_302"),
        (comparison.SampleResponse(500, {}, b""), "unexpected_http_500"),
        (comparison.SampleResponse(200, {"content-type": "text/html"}, b"challenge"), "html_response"),
        (comparison.SampleResponse(200, {}, b"<html>challenge</html>"), "html_response"),
        (comparison.SampleResponse(200, {}, b"%PDF-1.7\npartial"), "invalid_pdf"),
        (comparison.SampleResponse(200, {}, b"", stop_reason="size_limit"), "size_limit"),
        (httpx.ConnectError("private transport details"), "transport_error:ConnectError"),
    ],
)
def test_first_problem_stops_all_remaining_requests_without_retry(outcome, reason):
    client = Client(Clock(), outcomes={43: outcome})

    records, summary = run(client)

    assert len(client.calls) == len(records) == summary["actual_requests"] == 43
    assert summary["accepted_pdfs"] == 42
    assert summary["stop_reason"] == reason
    assert summary["stopping_sample"] == records[-1]
    assert summary["acceptance"]["technical_acceptance_passed"] is False
    assert "private transport" not in json.dumps(records)


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("/cgi/viewcontent.cgi?article=6704&context=smallsat",
         "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=6704&context=smallsat"),
        ("https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=6704&context=smallsat",
         "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=6704&context=smallsat"),
        ("http://example.org/paper.pdf", "http://example.org/paper.pdf"),
        ("https://user:secret@example.org/paper.pdf", None),
        ("ftp://example.org/paper.pdf", None),
        ("javascript:alert(1)", None),
        ("http://localhost/paper.pdf", None),
        ("http://127.0.0.1/paper.pdf", None),
        ("http://169.254.169.254/paper.pdf", None),
        ("http://10.0.0.1/paper.pdf", None),
        ("http://[::1]/paper.pdf", None),
        ("http://service.internal/paper.pdf", None),
        ("https://example.org:broken/paper.pdf", None),
    ],
)
def test_redirect_location_is_limited_to_public_http_urls(location, expected):
    assert sustained.safe_response_location(location, "https://digitalcommons.usu.edu/old.pdf") == expected


def test_redirect_is_recorded_without_following_or_logging_other_headers():
    location = "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=6704&context=smallsat"
    client = Client(Clock(), outcomes={1: comparison.SampleResponse(302, {
        "location": location, "set-cookie": "private-cookie", "server": "cloudflare",
    }, b"")})

    records, summary = run(client)

    assert len(client.calls) == 1
    assert records[0]["response_location"] == location
    assert summary["stopping_sample"]["response_location"] == location
    assert summary["stop_reason"] == "unexpected_http_302"
    assert "private-cookie" not in json.dumps(summary)
    summary["runtime"] = {"python": "test", "curl_cffi": "test"}
    assert location in sustained.markdown_summary(summary)


@pytest.mark.parametrize(
    ("body", "reason"),
    [(pdf(0) + b"\n", "expected_size_mismatch"), (pdf(999999), "expected_hash_mismatch")],
)
def test_expected_bytes_or_hash_mismatch_stops_after_one_pdf(body, reason):
    client = Client(Clock(), outcomes={1: comparison.SampleResponse(200, {}, body)})

    records, summary = run(client)

    assert len(client.calls) == 1
    assert summary["stop_reason"] == reason
    assert records[0]["valid_pdf"] is True
    assert records[0]["accepted_sample"] is False
    assert summary["accepted_pdfs"] == summary["total_accepted_bytes"] == 0
    assert summary["failed_checks"] == 1


def test_budget_caps_all_requests_and_partial_pass_cannot_qualify():
    client = Client(Clock())

    records, summary = run(client, max_requests=7)

    assert len(client.calls) == len(records) == 7
    assert summary["stop_reason"] == "request_budget_reached"
    assert summary["accepted_pdfs"] == 7
    assert summary["acceptance"]["technical_acceptance_passed"] is False


@pytest.mark.parametrize("max_requests", [0, 151, 1000])
def test_budget_above_150_is_rejected_before_requests(max_requests):
    client = Client(Clock())
    with pytest.raises(ValueError, match="request budget"):
        run(client, max_requests=max_requests)
    assert not client.calls


def test_slow_responses_cannot_qualify_as_target_cadence():
    client = Client(Clock(), duration=3)
    records, summary = run(client)

    assert summary["accepted_pdfs"] == 150
    assert summary["observed_start_rate_per_minute"] == 20
    assert summary["minimum_start_gap_seconds"] == 3
    assert summary["acceptance"]["all_150_expected_pdfs"] is True
    assert summary["acceptance"]["technical_acceptance_passed"] is False
    assert len(summary["slow_response_indices"]) == 150
    assert all(record["response_exceeded_interval"] for record in records)


def test_acceptance_tolerance_and_minimum_cadence_are_explicit():
    records, _ = run(Client(Clock()))
    records[1]["gap_since_previous_start_seconds"] = 1.999
    assert sustained.summarize(records, stop_reason="completed")["acceptance"]["technical_acceptance_passed"] is True
    records[1]["gap_since_previous_start_seconds"] = 1.9989
    assert sustained.summarize(records, stop_reason="completed")["acceptance"]["technical_acceptance_passed"] is False
    records[1]["gap_since_previous_start_seconds"] = 2
    records[-1]["started_seconds"] = 149 * 60 / 29.4
    assert sustained.summarize(records, stop_reason="completed")["acceptance"]["technical_acceptance_passed"] is False


def test_existing_short_caps_are_unchanged():
    assert comparison.MAX_REQUESTS == 24
    assert short_probe.MAX_REQUESTS == 18
    assert sustained.REQUEST_LIMIT == 150


def test_cli_plan_only_never_imports_curl_or_creates_results(monkeypatch, tmp_path, capsys):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"pdfs": [asdict(target) for target in targets()]}))
    original_import = builtins.__import__

    def import_without_curl(name, *args, **kwargs):
        if name == "curl_cffi" or name.startswith("curl_cffi."):
            raise AssertionError("curl imported during plan-only operation")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_curl)

    assert sustained.main(["--manifest", str(manifest)]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 150
    assert list(tmp_path.iterdir()) == [manifest]


def test_execute_precommits_manifest_uses_streaming_session_and_saves_results(monkeypatch, tmp_path):
    settings = []
    closed = []
    proxy_option = object()
    prefix = tmp_path / "sustained-probe"

    class Session:
        def __init__(self, **options):
            settings.append(options)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append(True)

    monkeypatch.setitem(sys.modules, "curl_cffi", SimpleNamespace(
        AsyncSession=Session, CurlOpt=SimpleNamespace(PROXY=proxy_option),
    ))
    original_run = sustained.run_probe

    async def fake_run(pdf_targets, client, *, emit, emit_http, allow_archive_redirect):
        assert isinstance(client, comparison.CurlClient)
        initial = json.loads(prefix.with_suffix(".json").read_text())
        assert len(initial["plan"]) == 150
        assert initial["stop_reason"] == "not_finished"
        assert initial["config"]["request_budget"] == 150
        fake = Client(Clock(), outcomes={1: comparison.SampleResponse(
            403, {"content-type": "text/html", "server": "cloudflare"}, b"",
        )})
        return await original_run(
            pdf_targets, fake, monotonic=fake.clock.monotonic, sleep=fake.clock.sleep,
            timestamp=fake.clock.timestamp, emit=emit, emit_http=emit_http,
            allow_archive_redirect=allow_archive_redirect,
        )

    monkeypatch.setattr(sustained, "run_probe", fake_run)
    manifest = {"path": "/tmp/vetted-manifest.json", "sha256": "a" * 64}

    summary = asyncio.run(sustained.execute(
        targets(), prefix, context_notes=["No other known website traffic during the run."], manifest=manifest,
    ))

    assert len(settings) == 1
    assert settings[0]["impersonate"] == "chrome"
    assert settings[0]["allow_redirects"] is False
    assert settings[0]["trust_env"] is False
    assert settings[0]["max_clients"] == 1
    assert settings[0]["curl_options"] == {proxy_option: ""}
    assert "cache" not in settings[0]
    assert closed == [True]
    assert summary["actual_requests"] == 1
    saved = json.loads(prefix.with_suffix(".json").read_text())
    assert saved["manifest"] == manifest
    assert saved["config"]["max_bytes"] == comparison.MAX_BYTES
    assert saved["config"]["default_download_rate_changed"] is False
    assert len(saved["plan"]) == 150
    assert saved["samples"][0]["status_code"] == 403
    events = [json.loads(line) for line in prefix.with_suffix(".jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == ["sustained_curl_http_request", "sustained_curl_sample"]
    assert all(event["status_code"] == 403 for event in events)
    markdown = prefix.with_suffix(".md").read_text()
    assert "Accepted PDFs: **0/150**" in markdown
    assert "No other known website traffic" in markdown
    assert "observed start cadence at least 29.5/minute" in markdown
    assert "Traffic isolation must be checked outside this script" in markdown
    assert "Stop" in markdown


def archive_targets():
    return [replace(target, url=target.url.replace("archive.test", "digitalcommons.usu.edu")) for target in targets()]


def archive_location(number, **changes):
    query = {"params": f"/context/smallsat/article/{number}/", "path_info": f"paper{number}.pdf"}
    query.update(changes)
    return "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?" + urlencode(query)


class RedirectClient(Client):
    async def request(self, url, max_bytes):
        self.calls.append((url, self.clock.now, max_bytes))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await self.clock.sleep(self.duration)
            outcome = self.outcomes.get(len(self.calls))
            if isinstance(outcome, Exception):
                raise outcome
            if outcome:
                return outcome
            parsed = urlsplit(url)
            if parsed.path.startswith("/context/"):
                number = int(parsed.path.split("/")[-3])
                return comparison.SampleResponse(302, {"location": archive_location(number)}, b"")
            number = int(parse_qs(parsed.query)["params"][0].rstrip("/").split("/")[-1])
            return comparison.SampleResponse(200, {"content-type": "application/pdf"}, pdf(number))
        finally:
            self.active -= 1


def test_opt_in_validated_archive_redirects_have_150_downloads_and_300_gets():
    client = RedirectClient(Clock())
    physical = []

    records, summary = run(client, archive_targets(), allow_archive_redirect=True, emit_http=physical.append)

    assert len(records) == summary["actual_downloads"] == 150
    assert len(client.calls) == len(physical) == summary["actual_http_gets"] == 300
    assert client.max_active == 1
    assert summary["maximum_http_gets"] == 300
    assert summary["acceptance"]["technical_acceptance_passed"] is True
    assert [record["started_seconds"] for record in records] == [2 * index for index in range(150)]
    assert [hop["status_code"] for hop in physical] == [302, 200] * 150
    assert [hop["index"] for hop in physical] == list(range(1, 301))
    assert all(len(record["http_hops"]) == 2 for record in records)
    assert records[0]["archive_redirect_url"] == archive_location(0)
    assert records[0]["http_hops"][1]["started_seconds"] == 0.2
    assert records[1]["http_hops"][0]["started_seconds"] == 2


@pytest.mark.parametrize("blocked_hop", [43, 44])
def test_block_on_either_hop_stops_immediately_at_physical_request(blocked_hop):
    client = RedirectClient(Clock(), outcomes={blocked_hop: comparison.SampleResponse(403, {}, b"")})

    records, summary = run(client, archive_targets(), allow_archive_redirect=True)

    assert len(client.calls) == summary["actual_http_gets"] == blocked_hop
    assert summary["actual_downloads"] == 22
    assert summary["accepted_pdfs"] == 21
    assert summary["stop_reason"] == "http_403"
    assert records[-1]["http_hops"][-1]["stop_reason"] == "http_403"
    assert summary["acceptance"]["technical_acceptance_passed"] is False


@pytest.mark.parametrize(
    "location",
    [
        archive_location(0).replace("digitalcommons.usu.edu", "example.org"),
        archive_location(0).replace("https://", "http://"),
        archive_location(0).replace("digitalcommons.usu.edu", "digitalcommons.usu.edu:444"),
        archive_location(99),
        archive_location(0, path_info="wrong.pdf"),
        archive_location(0) + "&unexpected=value",
        archive_location(0) + "&params=duplicate",
        archive_location(0) + "#fragment",
        archive_location(0).replace("digitalcommons.usu.edu", "user:secret@digitalcommons.usu.edu"),
    ],
)
def test_unexpected_archive_redirect_never_gets_followup_request(location):
    client = RedirectClient(Clock(), outcomes={1: comparison.SampleResponse(302, {"location": location}, b"")})

    _, summary = run(client, archive_targets(), allow_archive_redirect=True)

    assert len(client.calls) == summary["actual_http_gets"] == 1
    assert summary["stop_reason"] == "unexpected_archive_redirect"


def test_redirect_requires_decoded_filename_and_article_to_match():
    source = "https://digitalcommons.usu.edu/context/smallsat/article/6704/viewcontent/My%20Paper.pdf"
    destination = "https://digitalcommons.usu.edu/cgi/viewcontent.cgi?params=/context/smallsat/article/6704/&path_info=My+Paper.pdf"
    assert sustained.archive_redirect_url(source, destination) == destination
    assert sustained.archive_redirect_url(source, destination.replace("6704", "6705")) is None
    assert sustained.archive_redirect_url(source, destination.replace("My+Paper", "Other+Paper")) is None


def test_extra_redirect_is_not_followed():
    client = RedirectClient(Clock(), outcomes={2: comparison.SampleResponse(302, {"location": archive_location(0)}, b"")})

    _, summary = run(client, archive_targets(), allow_archive_redirect=True)

    assert len(client.calls) == summary["actual_http_gets"] == 2
    assert summary["actual_downloads"] == 1
    assert summary["stop_reason"] == "extra_archive_redirect"


def test_physical_request_budget_prevents_redirect_hop_from_exceeding_cap():
    client = RedirectClient(Clock())

    _, summary = run(client, archive_targets(), allow_archive_redirect=True, max_http_requests=3)

    assert len(client.calls) == summary["actual_http_gets"] == 3
    assert summary["actual_downloads"] == 2
    assert summary["accepted_pdfs"] == 1
    assert summary["stop_reason"] == "http_request_budget_reached"


@pytest.mark.parametrize("allow_redirect,budget", [(False, 151), (True, 301), (True, 0)])
def test_physical_request_budget_above_mode_limit_is_rejected(allow_redirect, budget):
    client = RedirectClient(Clock())
    with pytest.raises(ValueError, match="HTTP request budget"):
        run(client, archive_targets(), allow_archive_redirect=allow_redirect, max_http_requests=budget)
    assert not client.calls


def test_default_mode_still_refuses_known_archive_redirect():
    client = RedirectClient(Clock())

    _, summary = run(client, archive_targets())

    assert len(client.calls) == summary["actual_http_gets"] == 1
    assert summary["maximum_http_gets"] == 150
    assert summary["allow_archive_redirect"] is False
    assert summary["stop_reason"] == "unexpected_http_302"
