import asyncio
import builtins
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from scripts import compare_transports as comparison


URLS = [f"https://archive.test/paper{number}.pdf" for number in range(3)]
PDF = b"%PDF-1.7\nsmall complete test file\n%%EOF\n"


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


class FakeClients:
    def __init__(self, clock, outcomes=None, duration=0.25):
        self.clock = clock
        self.outcomes = outcomes or {}
        self.duration = duration
        self.calls = []
        self.active = 0
        self.max_active = 0

    def clients(self):
        owner = self

        class Client:
            def __init__(self, transport):
                self.transport = transport

            async def request(self, url, max_bytes):
                owner.calls.append((self.transport, url, owner.clock.now))
                owner.active += 1
                owner.max_active = max(owner.max_active, owner.active)
                try:
                    await owner.clock.sleep(owner.duration)
                    outcome = owner.outcomes.get(len(owner.calls))
                    if isinstance(outcome, Exception):
                        raise outcome
                    return outcome or comparison.SampleResponse(
                        200, {"content-type": "application/pdf", "set-cookie": "private-secret"}, PDF,
                    )
                finally:
                    owner.active -= 1

        return {transport: Client(transport) for transport in comparison.TRANSPORTS}


def run(plan, fake, **options):
    return asyncio.run(comparison.run_comparison(
        plan, fake.clients(), monotonic=fake.clock.monotonic, sleep=fake.clock.sleep,
        timestamp=lambda: "2026-10-17T00:00:00+00:00", **options,
    ))


def test_plan_counterbalances_order_and_matches_urls_across_both_tiers():
    plan = comparison.build_plan(URLS, include_20=True)

    assert len(plan) == 24
    assert Counter(item.rate_per_minute for item in plan) == {15: 12, 20: 12}
    assert Counter((item.rate_per_minute, item.transport) for item in plan) == {
        (rate, transport): 6 for rate in (15, 20) for transport in comparison.TRANSPORTS
    }
    for offset in range(0, 24, 4):
        block = plan[offset:offset + 4]
        assert len({item.url for item in block}) == 1
        expected = ["httpx", "curl_cffi", "curl_cffi", "httpx"]
        if offset // 4 % 2:
            expected = ["curl_cffi", "httpx", "httpx", "curl_cffi"]
        assert [item.transport for item in block] == expected
    assert [item.url for item in plan[:12]] == [item.url for item in plan[12:]]


def test_one_global_limiter_persists_across_clients_and_rate_tiers():
    clock = Clock()
    fake = FakeClients(clock)
    plan = comparison.build_plan(URLS[:1], include_20=True, requests_per_tier=4)
    emitted = []

    records, summary = run(plan, fake, emit=emitted.append)

    assert [round(start - 100, 6) for _, _, start in fake.calls] == [0, 4, 8, 12, 15, 18, 21, 24]
    assert fake.max_active == 1
    assert summary["stop_reason"] == "completed"
    assert summary["actual_requests"] == 8
    assert summary["same_url_hashes_match"] is True
    assert summary["paired_urls"] == 1
    assert records == emitted
    assert {record["sha256"] for record in records} == {hashlib.sha256(PDF).hexdigest()}
    assert "private-secret" not in json.dumps(records)
    assert "private-secret" not in json.dumps(summary)
    assert all(record["duration_seconds"] == 0.25 for record in records)
    assert all(group["successful"] == 2 for group in summary["groups"])


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [
        (comparison.SampleResponse(403, {"retry-after": "120"}, b""), "http_403"),
        (comparison.SampleResponse(429, {}, b""), "http_429"),
        (comparison.SampleResponse(302, {}, b""), "unexpected_http_302"),
        (comparison.SampleResponse(200, {"content-type": "text/html"}, PDF), "html_response"),
        (comparison.SampleResponse(200, {}, b" <html>challenge</html>"), "html_response"),
        (comparison.SampleResponse(200, {}, b"not a PDF"), "invalid_pdf"),
        (comparison.SampleResponse(200, {}, b"%PDF-1.7\npartial"), "invalid_pdf"),
        (comparison.SampleResponse(200, {}, b"", stop_reason="size_limit"), "size_limit"),
        (httpx.ConnectError("do not log arbitrary private exception text"), "transport_error:ConnectError"),
    ],
)
def test_first_problem_stops_every_transport_without_retry_or_higher_tier(outcome, reason):
    fake = FakeClients(Clock(), outcomes={2: outcome})
    plan = comparison.build_plan(URLS, include_20=True)

    records, summary = run(plan, fake)

    assert len(fake.calls) == len(records) == summary["actual_requests"] == 2
    assert summary["stop_reason"] == records[-1]["stop_reason"] == reason
    assert all(record["rate_per_minute"] == 15 for record in records)
    assert summary["same_url_hashes_match"] is None
    assert "the transports cannot be compared" in comparison.markdown_summary(summary)
    assert summary["retries"] == 0
    assert "private exception" not in json.dumps(records)
    if reason == "http_403":
        assert records[-1]["retry_after"] == "120"


def test_request_budget_caps_all_clients_combined():
    fake = FakeClients(Clock())
    plan = comparison.build_plan(URLS, include_20=True)

    records, summary = run(plan, fake, max_requests=5)

    assert len(records) == len(fake.calls) == 5
    assert summary["stop_reason"] == "request_budget_reached"
    assert summary["planned_requests"] == 24
    assert summary["actual_requests"] == 5


@pytest.mark.parametrize("max_requests", [0, 25, 100])
def test_budget_outside_hard_cap_is_rejected_before_requests(max_requests):
    fake = FakeClients(Clock())

    with pytest.raises(ValueError, match="request budget"):
        run(comparison.build_plan(URLS), fake, max_requests=max_requests)

    assert not fake.calls


def test_slow_responses_do_not_add_an_unnecessary_full_interval():
    fake = FakeClients(Clock(), duration=5)
    run(comparison.build_plan(URLS[:1], requests_per_tier=4), fake)

    assert [start - 100 for _, _, start in fake.calls] == [0, 5, 10, 15]


@pytest.mark.parametrize("declared", [True, False])
def test_httpx_stream_stops_oversized_files_and_closes_response(declared):
    responses = []

    def respond(request):
        response = httpx.Response(200, headers={
            "content-type": "application/pdf",
            "content-length": "1000" if declared else "0",
        }, content=PDF * 30)
        responses.append(response)
        return response

    async def sample():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as session:
            return await comparison.HttpxClient(session).request(URLS[0], 64)

    response = asyncio.run(sample())

    assert response.stop_reason == "size_limit"
    assert len(response.body) <= 64
    assert responses[0].is_closed


@pytest.mark.parametrize("failed", [False, True])
def test_curl_stream_disables_redirects_and_always_closes(failed):
    class Response:
        status_code = 200
        headers = {"content-type": "application/pdf"}
        closed = False

        async def aiter_content(self):
            if failed:
                raise OSError("disconnected")
            yield PDF

        async def aclose(self):
            self.closed = True

    class Session:
        async def get(self, url, **options):
            assert url == URLS[0]
            assert options == {"stream": True, "allow_redirects": False}
            return response

    response = Response()
    if failed:
        with pytest.raises(OSError):
            asyncio.run(comparison.CurlClient(Session()).request(URLS[0], 1000))
    else:
        sample = asyncio.run(comparison.CurlClient(Session()).request(URLS[0], 1000))
        assert sample.body == PDF
    assert response.closed


def test_default_cli_only_prints_plan_and_never_imports_optional_transport(monkeypatch, tmp_path, capsys):
    urls = tmp_path / "urls.txt"
    urls.write_text("\n".join(URLS))
    original_import = builtins.__import__

    def import_without_curl(name, *args, **kwargs):
        if name == "curl_cffi" or name.startswith("curl_cffi."):
            raise AssertionError("optional transport imported before execution")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_curl)

    result = comparison.main(["--urls", str(urls), "--include-20"])

    assert result == 0
    assert len(json.loads(capsys.readouterr().out)) == 24
    assert list(tmp_path.iterdir()) == [urls]


def test_summary_reports_changed_content_and_short_sample_limitations():
    fake = FakeClients(Clock(), outcomes={2: comparison.SampleResponse(
        200, {}, b"%PDF-1.7\ndifferent valid content\n%%EOF",
    )})
    _, summary = run(comparison.build_plan(URLS[:1], requests_per_tier=4), fake)

    markdown = comparison.markdown_summary(summary)

    assert summary["same_url_hashes_match"] is False
    assert "Rates are aggregate across both clients; each receives half the requests." in markdown
    assert "does not test either client independently at 15 or 20/min" in summary["limitation"]
    assert "does not establish a sustained site quota" in markdown
    assert "httpx" in markdown and "curl_cffi" in markdown
    assert "2/2" in markdown


def test_execute_uses_fresh_persistent_direct_clients_and_writes_reports(monkeypatch, tmp_path):
    options = {}
    closed = []
    proxy_option = object()

    class Session:
        def __init__(self, transport, **settings):
            self.transport = transport
            options[transport] = settings

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append(self.transport)

    monkeypatch.setattr(comparison.httpx, "AsyncClient", lambda **settings: Session("httpx", **settings))
    monkeypatch.setitem(sys.modules, "curl_cffi", SimpleNamespace(
        AsyncSession=lambda **settings: Session("curl_cffi", **settings),
        CurlOpt=SimpleNamespace(PROXY=proxy_option),
    ))

    async def fake_run(plan, clients, *, emit):
        assert set(clients) == set(comparison.TRANSPORTS)
        assert clients["httpx"].session.transport == "httpx"
        assert clients["curl_cffi"].session.transport == "curl_cffi"
        emit({"index": 1, "transport": "httpx", "rate_per_minute": 15,
              "status_code": 403, "bytes": 0, "duration_seconds": 0.1,
              "stop_reason": "http_403"})
        return [], comparison.summarize([], planned=len(plan), stop_reason="http_403")

    monkeypatch.setattr(comparison, "run_comparison", fake_run)
    prefix = tmp_path / "sample"

    summary = asyncio.run(comparison.execute(
        comparison.build_plan(URLS), prefix, ["Earlier acquisition activity may share the public IP."],
    ))

    assert options["httpx"]["trust_env"] is False
    assert options["httpx"]["follow_redirects"] is False
    assert options["httpx"]["limits"].keepalive_expiry == 120
    assert options["curl_cffi"]["trust_env"] is False
    assert options["curl_cffi"]["allow_redirects"] is False
    assert options["curl_cffi"]["curl_options"] == {proxy_option: ""}
    assert options["curl_cffi"]["impersonate"] == "chrome"
    assert "cache" not in options["curl_cffi"]
    assert closed == ["curl_cffi", "httpx"]
    assert summary["stop_reason"] == "http_403"
    assert json.loads(prefix.with_suffix(".json").read_text())["stop_reason"] == "http_403"
    assert json.loads(prefix.with_suffix(".jsonl").read_text())["status_code"] == 403
    assert "does not establish a sustained site quota" in prefix.with_suffix(".md").read_text()
    assert "Earlier acquisition activity may share the public IP." in prefix.with_suffix(".md").read_text()
