import asyncio
import builtins
import json
import sys
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest

from scripts import compare_transports as comparison
from scripts import probe_curl_rates as probe


URLS = [f"https://archive.test/paper{number}.pdf" for number in range(3)]
PDF = b"%PDF-1.7\nsmall complete test file\n%%EOF\n"


class Clock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds
        await asyncio.sleep(0)


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
                "content-type": "application/pdf", "age": "42", "cf-cache-status": "HIT",
                "cache-control": "max-age=3600", "x-cache": "HIT", "etag": '"public-file"',
                "set-cookie": "private-secret", "authorization": "private-auth",
            }, PDF)
        finally:
            self.active -= 1


def run(client, plan=None, **options):
    return asyncio.run(probe.run_probe(
        plan or probe.build_plan(URLS), client,
        monotonic=client.clock.monotonic, sleep=client.clock.sleep,
        timestamp=lambda: "2026-10-17T07:00:00+00:00", **options,
    ))


@pytest.mark.parametrize("requests_per_tier", [4, 6])
def test_plan_has_fixed_ascending_tiers_and_reuses_vetted_urls(requests_per_tier):
    plan = probe.build_plan(URLS, requests_per_tier=requests_per_tier)

    assert len(plan) == 3 * requests_per_tier <= 18
    assert Counter(item.rate_per_minute for item in plan) == {rate: requests_per_tier for rate in (15, 20, 30)}
    for tier, rate in enumerate((15, 20, 30), start=1):
        samples = [item for item in plan if item.tier == tier]
        assert {item.rate_per_minute for item in samples} == {rate}
        assert [item.position_in_tier for item in samples] == list(range(1, requests_per_tier + 1))
        assert [item.url for item in samples] == [URLS[position % 3] for position in range(requests_per_tier)]


def test_pacing_is_shared_across_tier_boundaries_and_never_parallel():
    client = Client(Clock())
    emitted = []

    records, summary = run(client, emit=emitted.append)

    assert [round(start - 100, 6) for _, start, _ in client.calls] == [
        0, 4, 8, 12, 16, 20, 23, 26, 29, 32, 35, 38, 40, 42, 44, 46, 48, 50,
    ]
    assert client.max_active == 1
    assert len(records) == summary["actual_requests"] == 18
    assert summary["stop_reason"] == "completed"
    assert emitted == records
    assert [tier["observed_start_rate_per_minute"] for tier in summary["tiers"]] == [15, 20, 30]
    assert [tier["start_span_seconds"] for tier in summary["tiers"]] == [20, 15, 10]
    assert summary["same_url_content_consistent"] is True
    assert summary["repeated_urls_checked"] == 3
    assert records[6]["gap_since_previous_start_seconds"] == 3
    assert records[12]["gap_since_previous_start_seconds"] == 2
    assert records[0]["cache_headers"] == {
        "age": "42", "cache-control": "max-age=3600", "cf-cache-status": "HIT",
        "x-cache": "HIT", "etag": '"public-file"',
    }
    assert "private-secret" not in json.dumps(records)
    assert "private-auth" not in json.dumps(summary)


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [
        (comparison.SampleResponse(403, {"retry-after": "180"}, b""), "http_403"),
        (comparison.SampleResponse(429, {}, b""), "http_429"),
        (comparison.SampleResponse(302, {}, b""), "unexpected_http_302"),
        (comparison.SampleResponse(503, {}, b""), "unexpected_http_503"),
        (comparison.SampleResponse(200, {"content-type": "text/html"}, b"challenge"), "html_response"),
        (comparison.SampleResponse(200, {}, b"<html>challenge</html>"), "html_response"),
        (comparison.SampleResponse(200, {}, b"%PDF-1.7\ntruncated"), "invalid_pdf"),
        (comparison.SampleResponse(200, {}, b"not a PDF"), "invalid_pdf"),
        (comparison.SampleResponse(200, {}, b"", stop_reason="size_limit"), "size_limit"),
        (httpx.ReadError("private exception details"), "transport_error:ReadError"),
    ],
)
def test_every_problem_stops_all_remaining_requests_without_retry(outcome, reason):
    client = Client(Clock(), outcomes={7: outcome})

    records, summary = run(client)

    assert len(client.calls) == len(records) == summary["actual_requests"] == 7
    assert summary["stop_reason"] == reason
    assert summary["stopping_sample"] == records[-1]
    assert [tier["attempted"] for tier in summary["tiers"]] == [6, 1, 0]
    assert "private exception" not in json.dumps(records)
    if reason == "http_403":
        assert records[-1]["retry_after"] == "180"


@pytest.mark.parametrize(
    ("changed", "reason"),
    [(PDF + b"\n", "same_url_size_changed"),
     (PDF.replace(b"test", b"diff"), "same_url_hash_changed")],
)
def test_changed_repeated_url_content_stops_probe(changed, reason):
    client = Client(Clock(), outcomes={4: comparison.SampleResponse(200, {}, changed)})

    records, summary = run(client)

    assert len(client.calls) == 4
    assert summary["stop_reason"] == reason
    assert summary["same_url_content_consistent"] is False
    assert records[-1]["valid_pdf"] is True
    assert records[-1]["accepted_sample"] is False
    assert summary["tiers"][0]["accepted_pdfs"] == 3
    assert summary["tiers"][0]["failed_checks"] == 1
    assert summary["tiers"][0]["total_bytes"] == 3 * len(PDF)
    assert records[-1]["expected_sha256"] == records[0]["sha256"]


def test_budget_caps_total_requests_even_if_supplied_plan_is_longer():
    client = Client(Clock())
    records, summary = run(client, plan=probe.build_plan(URLS) * 2)

    assert len(client.calls) == len(records) == 18
    assert summary["stop_reason"] == "request_budget_reached"


@pytest.mark.parametrize("max_requests", [0, 19, 24])
def test_budget_above_hard_cap_is_rejected_before_requests(max_requests):
    client = Client(Clock())
    with pytest.raises(ValueError, match="request budget"):
        run(client, max_requests=max_requests)
    assert not client.calls


def test_existing_comparison_rejects_30_minute_rate_unchanged():
    client = Client(Clock())
    with pytest.raises(ValueError, match="15/minute and 20/minute"):
        asyncio.run(comparison.run_comparison(
            [comparison.PlannedRequest("curl_cffi", URLS[0], 30, 0)], {"curl_cffi": client},
        ))
    assert not client.calls


def test_slow_response_limits_cadence_without_extra_fixed_delay():
    client = Client(Clock(), duration=5)
    records, summary = run(client, max_requests=3)

    assert [record["started_seconds"] for record in records] == [0, 5, 10]
    assert summary["tiers"][0]["observed_start_rate_per_minute"] == 12


def test_cli_is_plan_only_and_does_not_import_curl(monkeypatch, tmp_path, capsys):
    urls = tmp_path / "urls.txt"
    urls.write_text("\n".join(URLS))
    original_import = builtins.__import__

    def import_without_curl(name, *args, **kwargs):
        if name == "curl_cffi" or name.startswith("curl_cffi."):
            raise AssertionError("optional transport imported for plan-only operation")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_curl)
    assert probe.main(["--urls", str(urls)]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 18
    assert list(tmp_path.iterdir()) == [urls]


def test_execute_uses_one_direct_streaming_session_and_saves_manifest(monkeypatch, tmp_path):
    settings = []
    closed = []
    proxy_option = object()

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

    async def fake_probe(plan, client, *, emit):
        assert isinstance(client, comparison.CurlClient)
        assert isinstance(client.session, Session)
        fake = Client(Clock(), outcomes={1: comparison.SampleResponse(
            403, {"content-type": "text/html", "server": "cloudflare"}, b"",
        )})
        return await probe.run_probe_original(
            plan, fake, monotonic=fake.clock.monotonic, sleep=fake.clock.sleep, emit=emit,
        )

    monkeypatch.setattr(probe, "run_probe_original", probe.run_probe, raising=False)
    monkeypatch.setattr(probe, "run_probe", fake_probe)
    prefix = tmp_path / "rate-probe"

    summary = asyncio.run(probe.execute(probe.build_plan(URLS), prefix, ["Prior IP history exists."]))

    assert len(settings) == 1
    assert settings[0]["impersonate"] == "chrome"
    assert settings[0]["allow_redirects"] is False
    assert settings[0]["trust_env"] is False
    assert settings[0]["max_clients"] == 1
    assert settings[0]["curl_options"] == {proxy_option: ""}
    assert "cache" not in settings[0]
    assert closed == [True]
    assert summary["actual_requests"] == 1
    assert summary["config"]["request_budget"] == 18
    assert summary["config"]["default_download_rate_changed"] is False
    saved = json.loads(prefix.with_suffix(".json").read_text())
    assert len(saved["plan"]) == 18
    assert saved["samples"][0]["status_code"] == 403
    assert saved["runtime"]["curl_cffi"]
    assert json.loads(prefix.with_suffix(".jsonl").read_text())["config"]["max_bytes"] == 2 * 1024 * 1024
    markdown = prefix.with_suffix(".md").read_text()
    assert "Prior IP history exists." in markdown
    assert "not a sustained-rate or safe-quota test" in markdown
    assert "later block includes earlier-tier" in markdown
    assert "server/CDN caches" in markdown
    assert "Stopped at request 1" in markdown
