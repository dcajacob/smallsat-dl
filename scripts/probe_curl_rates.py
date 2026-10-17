"""Plan or run a small curl-only rate probe, stopping on the first problem."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import importlib.metadata
import json
import platform
import statistics
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

try:
    from scripts import compare_transports as comparison
except ModuleNotFoundError as exc:
    if exc.name != "scripts":
        raise
    import compare_transports as comparison


MAX_REQUESTS = 18
RATES = (15, 20, 30)
CACHE_HEADERS = ("age", "cache-control", "cf-cache-status", "x-cache", "etag", "last-modified")
LIMITATION = (
    "This is a short access and request-spacing probe, not a sustained-rate or safe-quota test. "
    "Even six requests span only 20 seconds at 15/min, 15 seconds at 20/min, and 10 seconds at 30/min. "
    "The tiers run consecutively, so a later block includes earlier-tier and prior shared-IP "
    "traffic; it cannot identify an independent rate threshold. Repeating three small URLs "
    "can warm server/CDN caches and does not represent a full archive. The application's "
    "default rate is unchanged by this probe."
)


@dataclass(frozen=True)
class PlannedRequest:
    url: str
    rate_per_minute: int
    tier: int
    position_in_tier: int


def build_plan(urls: Sequence[str], *, requests_per_tier: int = 6) -> list[PlannedRequest]:
    if not 1 <= len(urls) <= 3:
        raise ValueError("provide one to three vetted small PDF URLs")
    if requests_per_tier not in {4, 6}:
        raise ValueError("requests per tier must be 4 or 6")
    for url in urls:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("PDF URLs must use HTTPS and must not contain credentials")
    return [
        PlannedRequest(urls[position % len(urls)], rate, tier, position + 1)
        for tier, rate in enumerate(RATES, start=1)
        for position in range(requests_per_tier)
    ]


async def run_probe(
    plan: Sequence[PlannedRequest],
    client: comparison.SampleClient,
    *,
    max_requests: int = MAX_REQUESTS,
    max_bytes: int = comparison.MAX_BYTES,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timestamp: Callable[[], str] = lambda: dt.datetime.now(dt.UTC).isoformat(),
    emit: Callable[[dict[str, Any]], None] = lambda _: None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not plan:
        raise ValueError("a nonempty request plan is required")
    if not 1 <= max_requests <= MAX_REQUESTS:
        raise ValueError(f"request budget must be between 1 and {MAX_REQUESTS}")
    if not 1 <= max_bytes <= comparison.MAX_BYTES:
        raise ValueError(f"maximum file size must be between 1 and {comparison.MAX_BYTES} bytes")
    if any(item.rate_per_minute not in RATES for item in plan):
        raise ValueError("only the controlled 15, 20, and 30/minute rates are supported")

    records: list[dict[str, Any]] = []
    expected_content: dict[str, tuple[int, str]] = {}
    started = monotonic()
    started_timestamp = timestamp()
    previous_start: float | None = None
    stop_reason = "completed"
    for item in plan:
        if len(records) >= max_requests:
            stop_reason = "request_budget_reached"
            break
        if previous_start is not None:
            # Retain the same request-start clock when increasing tiers.
            remaining = previous_start + 60 / item.rate_per_minute - monotonic()
            while remaining > 0:
                await sleep(remaining)
                remaining = previous_start + 60 / item.rate_per_minute - monotonic()
        request_started = monotonic()
        record: dict[str, Any] = {
            "event": "curl_rate_sample", "index": len(records) + 1,
            "timestamp": timestamp(), "started_seconds": round(request_started - started, 6),
            "gap_since_previous_start_seconds": round(request_started - previous_start, 6)
            if previous_start is not None else None,
            "transport": "curl_cffi", **asdict(item),
            "status_code": None, "bytes": 0, "sha256": None, "valid_pdf": False,
            "accepted_sample": False,
            "content_type": None, "retry_after": None, "server": None,
            "http_version": None, "cache_headers": {}, "stop_reason": None,
        }
        previous_start = request_started
        try:
            response = await client.request(item.url, max_bytes)
            digest = hashlib.sha256(response.body).hexdigest() if response.body else None
            reason = comparison._response_stop_reason(response)
            record.update(
                status_code=response.status_code, bytes=len(response.body), sha256=digest,
                content_type=response.headers.get("content-type"),
                retry_after=response.headers.get("retry-after"), server=response.headers.get("server"),
                http_version=response.http_version, valid_pdf=reason is None,
                cache_headers={key: response.headers[key] for key in CACHE_HEADERS if key in response.headers},
            )
            if reason is None:
                current_content = (len(response.body), str(digest))
                previous_content = expected_content.setdefault(item.url, current_content)
                record["expected_bytes"], record["expected_sha256"] = previous_content
                if current_content[0] != previous_content[0]:
                    reason = "same_url_size_changed"
                elif current_content[1] != previous_content[1]:
                    reason = "same_url_hash_changed"
            record["stop_reason"] = reason
            record["accepted_sample"] = reason is None
        except Exception as exc:
            # Do not persist arbitrary exception text, response bodies, or cookies.
            record["stop_reason"] = f"transport_error:{type(exc).__name__}"
        record["duration_seconds"] = round(monotonic() - request_started, 6)
        record["finished_timestamp"] = timestamp()
        records.append(record)
        emit(record)
        if record["stop_reason"]:
            stop_reason = str(record["stop_reason"])
            break

    summary = summarize(records, planned=len(plan), stop_reason=stop_reason)
    summary.update(
        started_timestamp=started_timestamp, finished_timestamp=timestamp(),
        elapsed_seconds=round(monotonic() - started, 6),
    )
    return records, summary


def summarize(
    records: Sequence[dict[str, Any]], *, planned: int, stop_reason: str
) -> dict[str, Any]:
    tiers = []
    content_by_url: dict[str, list[tuple[int, str]]] = {}
    for rate in RATES:
        samples = [record for record in records if record["rate_per_minute"] == rate]
        valid = [record for record in samples if record["valid_pdf"]]
        accepted = [record for record in samples if record["accepted_sample"]]
        start_span = samples[-1]["started_seconds"] - samples[0]["started_seconds"] if samples else 0
        tiers.append({
            "rate_per_minute": rate, "attempted": len(samples), "valid_pdfs": len(valid),
            "accepted_pdfs": len(accepted),
            "failed_checks": sum(record["stop_reason"] is not None for record in samples),
            "total_bytes": sum(record["bytes"] for record in accepted),
            "start_span_seconds": round(start_span, 6) if samples else None,
            "observed_start_rate_per_minute": round(60 * (len(samples) - 1) / start_span, 3)
            if len(samples) > 1 and start_span > 0 else None,
            "median_duration_seconds": statistics.median(record["duration_seconds"] for record in accepted)
            if accepted else None,
        })
    for record in records:
        if record["valid_pdf"]:
            content_by_url.setdefault(record["url"], []).append((record["bytes"], record["sha256"]))
    compared = [values for values in content_by_url.values() if len(values) > 1]
    return {
        "experiment": "curl-only controlled rate probe", "planned_requests": planned,
        "actual_requests": len(records), "stop_reason": stop_reason, "tiers": tiers,
        "same_url_content_consistent": all(len(set(values)) == 1 for values in compared) if compared else None,
        "repeated_urls_checked": len(compared), "samples": list(records),
        "stopping_sample": records[-1] if records and records[-1]["stop_reason"] else None,
        "limitation": LIMITATION,
    }


def markdown_summary(summary: Mapping[str, Any]) -> str:
    runtime = summary["runtime"]
    config = summary["config"]
    lines = [
        "# Controlled curl_cffi rate probe", "",
        "Generated by `scripts/probe_curl_rates.py`.", "",
        f"Requests: {summary['actual_requests']}/{summary['planned_requests']}; "
        f"stop reason: `{summary['stop_reason']}`.", "",
        f"UTC: {summary['started_timestamp']} to {summary['finished_timestamp']}.", "",
        f"Python {runtime['python']}; curl_cffi {runtime['curl_cffi']}; Chrome impersonation.", "",
        f"One persistent direct session, sequential GETs, {config['requests_per_tier']} requests/tier, "
        f"{config['request_budget']} total maximum, {config['max_bytes']} bytes/file maximum. "
        "No redirects, retries, response caching, proxies, or uncounted warmups. "
        "A single request-start clock continues across all tiers.", "",
        "| Target/min | Accepted PDFs/attempted | Failed checks | Span between starts (s) | Observed starts/min | Median request (s) |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for tier in summary["tiers"]:
        def displayed(value: Any) -> str:
            return f"{value:.3f}" if value is not None else "—"
        lines.append(
            f"| {tier['rate_per_minute']} | {tier['accepted_pdfs']}/{tier['attempted']} | {tier['failed_checks']} | "
            f"{displayed(tier['start_span_seconds'])} | "
            f"{displayed(tier['observed_start_rate_per_minute'])} | "
            f"{displayed(tier['median_duration_seconds'])} |"
        )
    stopping = summary["stopping_sample"]
    if stopping:
        lines.extend(["", f"Stopped at request {stopping['index']}, target {stopping['rate_per_minute']}/min: "
                      f"HTTP {stopping['status_code']}, `{stopping['stop_reason']}`. "
                      f"Server: `{stopping['server']}`; Content-Type: `{stopping['content_type']}`; "
                      f"Retry-After: `{stopping['retry_after']}`."])
    consistency = summary["same_url_content_consistent"]
    lines.extend(["", "Repeated-URL bytes and SHA-256 checks: "
                  + ("not assessed (no repeated valid URL)." if consistency is None else str(consistency) + ".")])
    if summary.get("context_notes"):
        lines.extend(["", "Observed context:", ""])
        lines.extend(f"- {note}" for note in summary["context_notes"])
    lines.extend(["", LIMITATION, ""])
    return "\n".join(lines)


async def execute(
    plan: Sequence[PlannedRequest], output_prefix: Path, context_notes: Sequence[str] = ()
) -> dict[str, Any]:
    try:
        from curl_cffi import AsyncSession, CurlOpt
    except ImportError as exc:
        raise RuntimeError("curl_cffi is required to execute this probe") from exc
    try:
        curl_version = importlib.metadata.version("curl-cffi")
    except importlib.metadata.PackageNotFoundError:
        curl_version = "unknown"
    runtime = {"python": platform.python_version(), "curl_cffi": curl_version}
    config = {
        "rates_per_minute": list(RATES), "requests_per_tier": sum(item.tier == 1 for item in plan),
        "request_budget": MAX_REQUESTS, "max_bytes": comparison.MAX_BYTES,
        "transport": "curl_cffi", "browser_profile": "chrome", "max_active_requests": 1,
        "timeout_seconds": 20, "redirects": False, "retries": 0, "proxies": False,
        "client_response_cache": False, "warmup_requests": 0, "global_start_pacing": True,
        "default_download_rate_changed": False,
    }
    paths = {extension: Path(f"{output_prefix}.{extension}") for extension in ("jsonl", "json", "md")}
    if any(path.exists() for path in paths.values()):
        raise ValueError("probe result files already exist; choose a fresh output prefix")
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    with paths["jsonl"].open("x", encoding="utf-8") as stream:
        def emit(record: dict[str, Any]) -> None:
            stream.write(json.dumps({"runtime": runtime, "config": config, **record}, sort_keys=True) + "\n")
            stream.flush()
            print(f"{record['index']}/{len(plan)} curl_cffi {record['rate_per_minute']}/min "
                  f"HTTP {record['status_code']} {record['bytes']} bytes "
                  f"{record['duration_seconds']:.3f}s {record['stop_reason'] or 'valid PDF'}", flush=True)

        async with AsyncSession(
            impersonate="chrome", timeout=20, allow_redirects=False, trust_env=False,
            max_clients=1, curl_options={CurlOpt.PROXY: ""},
        ) as session:
            _, summary = await run_probe(plan, comparison.CurlClient(session), emit=emit)
    summary.update(runtime=runtime, config=config, context_notes=list(context_notes),
                   plan=[asdict(item) for item in plan])
    paths["json"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["md"].write_text(markdown_summary(summary), encoding="utf-8")
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--urls", type=Path, required=True, help="one to three vetted small PDF URLs, one per line")
    parser.add_argument("--requests-per-tier", type=int, choices=(4, 6), default=6)
    parser.add_argument("--output-prefix", type=Path, default=Path("curl-rate-probe"))
    parser.add_argument("--context-note", action="append", default=[])
    parser.add_argument("--execute", action="store_true", help="execute the plan; otherwise print it without networking")
    args = parser.parse_args(argv)
    try:
        urls = [line.strip() for line in args.urls.read_text().splitlines()
                if line.strip() and not line.lstrip().startswith("#")]
        plan = build_plan(urls, requests_per_tier=args.requests_per_tier)
        if not args.execute:
            print(json.dumps([asdict(item) for item in plan], indent=2))
            return 0
        summary = asyncio.run(execute(plan, args.output_prefix, args.context_note))
        print(f"Stopped: {summary['stop_reason']}. {LIMITATION}")
        return 0 if summary["stop_reason"] == "completed" else 1
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
