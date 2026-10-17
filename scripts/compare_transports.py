"""Compare two persistent clients using a small, globally paced PDF sample.

This deliberately does not use the application's cache or retry machinery.
Without --execute it only prints the proposed request plan, without networking.
"""

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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx


MAX_REQUESTS = 24
MAX_BYTES = 2 * 1024 * 1024
TRANSPORTS = ("httpx", "curl_cffi")
LIMITATION = (
    "Rates are aggregate across both clients; each receives half the requests. "
    "This does not test either client independently at 15 or 20/min. "
    "This short, sequential sample does not establish a sustained site quota or "
    "prove that either transport can safely download an archive faster. Server "
    "caches, shared IP history, connection warmup, and time/order effects can affect results."
)


@dataclass(frozen=True)
class PlannedRequest:
    transport: str
    url: str
    rate_per_minute: int
    block: int


@dataclass(frozen=True)
class SampleResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    http_version: str | None = None
    stop_reason: str | None = None


class SampleClient(Protocol):
    async def request(self, url: str, max_bytes: int) -> SampleResponse: ...


def build_plan(
    urls: Sequence[str], *, include_20: bool = False, requests_per_tier: int = 12
) -> list[PlannedRequest]:
    if not urls:
        raise ValueError("at least one vetted small PDF URL is required")
    if requests_per_tier not in {4, 8, 12}:
        raise ValueError("requests per tier must be 4, 8, or 12")
    for url in urls:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("PDF URLs must be public HTTPS URLs without credentials")
    plan = []
    block = 0
    for rate in ([15, 20] if include_20 else [15]):
        for _ in range(requests_per_tier // 4):
            # Alternate ABBA/BAAB to counterbalance both clients' positions.
            order = ("httpx", "curl_cffi", "curl_cffi", "httpx")
            if block % 2:
                order = ("curl_cffi", "httpx", "httpx", "curl_cffi")
            url = urls[block % len(urls)]
            plan.extend(PlannedRequest(transport, url, rate, block) for transport in order)
            block += 1
    assert len(plan) <= MAX_REQUESTS
    return plan


async def _read_sample(
    status_code: int,
    headers: Mapping[str, str],
    chunks: AsyncIterator[bytes],
    max_bytes: int,
    http_version: str | None = None,
) -> SampleResponse:
    selected = {str(key).lower(): str(value) for key, value in headers.items()}
    if status_code != 200:
        return SampleResponse(status_code, selected, b"", http_version)
    if "html" in selected.get("content-type", "").casefold():
        return SampleResponse(status_code, selected, b"", http_version, "html_response")
    try:
        declared_length = int(selected.get("content-length", "0"))
    except ValueError:
        declared_length = 0
    if declared_length > max_bytes:
        return SampleResponse(status_code, selected, b"", http_version, "size_limit")
    body = bytearray()
    async for chunk in chunks:
        if len(body) + len(chunk) > max_bytes:
            body.extend(chunk[: max_bytes - len(body)])
            return SampleResponse(status_code, selected, bytes(body), http_version, "size_limit")
        body.extend(chunk)
    return SampleResponse(status_code, selected, bytes(body), http_version)


class HttpxClient:
    def __init__(self, session: httpx.AsyncClient) -> None:
        self.session = session

    async def request(self, url: str, max_bytes: int) -> SampleResponse:
        async with self.session.stream("GET", url, follow_redirects=False) as response:
            return await _read_sample(
                response.status_code, response.headers, response.aiter_bytes(),
                max_bytes, response.http_version,
            )


class CurlClient:
    def __init__(self, session: Any) -> None:
        self.session = session

    async def request(self, url: str, max_bytes: int) -> SampleResponse:
        response = await self.session.get(url, stream=True, allow_redirects=False)
        try:
            return await _read_sample(
                response.status_code, response.headers, response.aiter_content(),
                max_bytes, str(getattr(response, "http_version", "")) or None,
            )
        finally:
            await response.aclose()


def _response_stop_reason(response: SampleResponse) -> str | None:
    if response.status_code in {403, 429}:
        return f"http_{response.status_code}"
    if response.status_code != 200:
        return f"unexpected_http_{response.status_code}"
    if response.stop_reason:
        return response.stop_reason
    prefix = response.body[:1024].lstrip().lower()
    if "html" in response.headers.get("content-type", "").casefold() or prefix.startswith(
        (b"<!doctype html", b"<html")
    ):
        return "html_response"
    if not response.body.startswith(b"%PDF-") or not response.body.rstrip().endswith(b"%%EOF"):
        return "invalid_pdf"
    return None


async def run_comparison(
    plan: Sequence[PlannedRequest],
    clients: Mapping[str, SampleClient],
    *,
    max_requests: int = MAX_REQUESTS,
    max_bytes: int = MAX_BYTES,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timestamp: Callable[[], str] = lambda: dt.datetime.now(dt.UTC).isoformat(),
    emit: Callable[[dict[str, Any]], None] = lambda _: None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not 1 <= max_requests <= MAX_REQUESTS:
        raise ValueError(f"request budget must be between 1 and {MAX_REQUESTS}")
    if not 1 <= max_bytes <= MAX_BYTES:
        raise ValueError(f"maximum file size must be between 1 and {MAX_BYTES} bytes")
    if any(item.rate_per_minute not in {15, 20} for item in plan):
        raise ValueError("only the controlled 15/minute and 20/minute tiers are supported")
    if any(item.transport not in clients for item in plan):
        raise ValueError("a persistent client is required for every planned transport")
    records: list[dict[str, Any]] = []
    started = monotonic()
    started_timestamp = timestamp()
    last_request_started: float | None = None
    stop_reason = "completed"
    for item in plan:
        if len(records) >= max_requests:
            stop_reason = "request_budget_reached"
            break
        if last_request_started is not None:
            # One shared clock persists across transport switches and rate tiers.
            remaining = last_request_started + 60 / item.rate_per_minute - monotonic()
            while remaining > 0:
                await sleep(remaining)
                remaining = last_request_started + 60 / item.rate_per_minute - monotonic()
        request_started = monotonic()
        last_request_started = request_started
        record: dict[str, Any] = {
            "event": "transport_sample", "index": len(records) + 1,
            "timestamp": timestamp(), "started_seconds": round(request_started - started, 6),
            **asdict(item), "status_code": None, "bytes": 0, "sha256": None,
            "content_type": None, "retry_after": None, "server": None,
            "http_version": None, "stop_reason": None,
        }
        try:
            response = await clients[item.transport].request(item.url, max_bytes)
            record.update(
                status_code=response.status_code, bytes=len(response.body),
                sha256=hashlib.sha256(response.body).hexdigest() if response.body else None,
                content_type=response.headers.get("content-type"),
                retry_after=response.headers.get("retry-after"),
                server=response.headers.get("server"), http_version=response.http_version,
                stop_reason=_response_stop_reason(response),
            )
        except Exception as exc:
            # Error class is sufficient for classification; do not persist arbitrary
            # exception text that could contain headers, cookies, or credentials.
            record["stop_reason"] = f"transport_error:{type(exc).__name__}"
        record["duration_seconds"] = round(monotonic() - request_started, 6)
        record["finished_timestamp"] = timestamp()
        records.append(record)
        emit(record)
        if record["stop_reason"]:
            stop_reason = str(record["stop_reason"])
            break
    summary = summarize(records, planned=len(plan), stop_reason=stop_reason)
    summary["elapsed_seconds"] = round(monotonic() - started, 6)
    summary["started_timestamp"] = started_timestamp
    summary["finished_timestamp"] = timestamp()
    return records, summary


def summarize(
    records: Sequence[dict[str, Any]], *, planned: int, stop_reason: str
) -> dict[str, Any]:
    groups = []
    for rate in sorted({record["rate_per_minute"] for record in records}):
        for transport in TRANSPORTS:
            samples = [record for record in records
                       if record["transport"] == transport and record["rate_per_minute"] == rate]
            successful = [record for record in samples if record["stop_reason"] is None]
            groups.append({
                "transport": transport, "rate_per_minute": rate, "requests": len(samples),
                "successful": len(successful), "failures": len(samples) - len(successful),
                "median_duration_seconds": statistics.median(
                    record["duration_seconds"] for record in successful
                ) if successful else None,
                "total_bytes": sum(record["bytes"] for record in successful),
            })
    hashes = {}
    transports_by_url = {}
    for record in records:
        if record["stop_reason"] is None:
            hashes.setdefault(record["url"], set()).add(record["sha256"])
            transports_by_url.setdefault(record["url"], set()).add(record["transport"])
    paired_urls = [url for url, transports in transports_by_url.items() if len(transports) == 2]
    return {
        "planned_requests": planned, "actual_requests": len(records), "stop_reason": stop_reason,
        "groups": groups, "paired_urls": len(paired_urls),
        "same_url_hashes_match": all(len(hashes[url]) == 1 for url in paired_urls)
        if paired_urls else None,
        "client_caches": "disabled", "redirects": "disabled", "retries": 0,
        "proxies": "disabled", "warmup_requests": 0, "global_pacing": True,
        "limitation": LIMITATION,
    }


def markdown_summary(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Controlled transport comparison", "",
        "Generated by `scripts/compare_transports.py`.", "",
        f"Requests: {summary['actual_requests']}/{summary['planned_requests']}. "
        f"Stop reason: `{summary['stop_reason']}`.", "",
        "All request starts share one global rate limiter; clients run sequentially. "
        "Automatic redirects, retries, local response caches, and proxies are disabled. "
        "Persistent sessions start cold; no uncounted warmup requests are sent.", "",
        "| Transport | Global requests/min | Successful/attempted | Median request seconds | Bytes |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    if "runtime" in summary:
        runtime = summary["runtime"]
        lines[4:4] = [
            f"Python {runtime['python']}; HTTPX {runtime['httpx']}; "
            f"curl_cffi {runtime['curl_cffi']} (Chrome impersonation).", "",
            f"UTC: {summary.get('started_timestamp', 'unknown')} to "
            f"{summary.get('finished_timestamp', 'unknown')}.", "",
        ]
    for group in summary["groups"]:
        median = group["median_duration_seconds"]
        displayed_median = f"{median:.4f}" if median is not None else "—"
        lines.append(
            f"| {group['transport']} | {group['rate_per_minute']} | "
            f"{group['successful']}/{group['requests']} | "
            f"{displayed_median} | {group['total_bytes']} |"
        )
    if summary["same_url_hashes_match"] is None:
        lines.extend(["", "No completed same-URL pair was collected; the transports cannot be compared."])
    else:
        lines.extend(["", f"Same-URL response hashes match: {summary['same_url_hashes_match']}."])
    if summary.get("context_notes"):
        lines.extend(["", "Observed experiment context:", ""])
        lines.extend(f"- {note}" for note in summary["context_notes"])
    lines.extend(["", LIMITATION, ""])
    return "\n".join(lines)


async def execute(
    plan: Sequence[PlannedRequest], output_prefix: Path, context_notes: Sequence[str] = ()
) -> dict[str, Any]:
    # Keep the optional browser-compatible transport out of base installations.
    try:
        from curl_cffi import AsyncSession, CurlOpt
    except ImportError as exc:
        raise RuntimeError("Install the browser optional dependency before executing this comparison") from exc
    try:
        curl_version = importlib.metadata.version("curl-cffi")
    except importlib.metadata.PackageNotFoundError:
        curl_version = "unknown"
    runtime = {"python": platform.python_version(), "httpx": httpx.__version__, "curl_cffi": curl_version}
    paths = {extension: Path(f"{output_prefix}.{extension}") for extension in ["jsonl", "json", "md"]}
    if any(path.exists() for path in paths.values()):
        raise ValueError("comparison output files already exist; choose a fresh output prefix")
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    with paths["jsonl"].open("x", encoding="utf-8") as stream:
        def emit(record: dict[str, Any]) -> None:
            stream.write(json.dumps({"runtime": runtime, **record}, sort_keys=True) + "\n")
            stream.flush()
            print(f"{record['index']}/{len(plan)} {record['transport']} "
                  f"{record['rate_per_minute']}/min HTTP {record['status_code']} "
                  f"{record['bytes']} bytes {record['duration_seconds']:.3f}s "
                  f"{record['stop_reason'] or 'valid PDF'}", flush=True)

        async with AsyncExitStack() as stack:
            httpx_session = await stack.enter_async_context(httpx.AsyncClient(
                timeout=20, follow_redirects=False, trust_env=False,
                # Interleaving clients makes each one's idle gaps longer than
                # HTTPX's default 5s, unlike the application's single-client run.
                limits=httpx.Limits(
                    max_connections=1, max_keepalive_connections=1, keepalive_expiry=120,
                ),
            ))
            curl_session = await stack.enter_async_context(AsyncSession(
                impersonate="chrome", timeout=20, allow_redirects=False,
                trust_env=False, max_clients=1,
                # libcurl otherwise discovers proxies from the environment;
                # enforce direct requests even in versions ignoring trust_env.
                curl_options={CurlOpt.PROXY: ""},
            ))
            _, summary = await run_comparison(
                plan, {"httpx": HttpxClient(httpx_session), "curl_cffi": CurlClient(curl_session)},
                emit=emit,
            )
    summary["runtime"] = runtime
    summary["httpx_keepalive_expiry_seconds"] = 120
    summary["context_notes"] = list(context_notes)
    summary["plan"] = [asdict(item) for item in plan]
    paths["json"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["md"].write_text(markdown_summary(summary), encoding="utf-8")
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--urls", type=Path, required=True, help="newline-separated vetted small PDF URLs")
    parser.add_argument("--include-20", action="store_true", help="try 12 requests at 20/min only if 15/min is clean")
    parser.add_argument("--requests-per-tier", type=int, choices=(4, 8, 12), default=12)
    parser.add_argument("--output-prefix", type=Path, default=Path("transport-comparison"))
    parser.add_argument("--context-note", action="append", default=[], help="record observed experiment context in the summary")
    parser.add_argument("--execute", action="store_true", help="execute the plan; otherwise print it without networking")
    args = parser.parse_args(argv)
    try:
        urls = [line.strip() for line in args.urls.read_text().splitlines()
                if line.strip() and not line.lstrip().startswith("#")]
        plan = build_plan(urls, include_20=args.include_20, requests_per_tier=args.requests_per_tier)
        if not args.execute:
            print(json.dumps([asdict(item) for item in plan], indent=2))
            return 0
        summary = asyncio.run(execute(plan, args.output_prefix, args.context_note))
        print(f"Stopped: {summary['stop_reason']}. {LIMITATION}")
        return 0 if summary["stop_reason"] == "completed" else 1
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
