"""Plan or run 150 distinct, verified PDF requests at a fixed 30 starts/minute."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import importlib.metadata
import ipaddress
import json
import platform
import re
import statistics
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urljoin, urlsplit

try:
    from scripts import compare_transports as comparison
except ModuleNotFoundError as exc:
    if exc.name != "scripts":
        raise
    import compare_transports as comparison


REQUEST_LIMIT = 150
RATE_PER_MINUTE = 30
INTERVAL_SECONDS = 2.0
CADENCE_MINIMUM = 29.5
GAP_TOLERANCE_SECONDS = 0.001
CACHE_HEADERS = ("age", "cache-control", "cf-cache-status", "x-cache", "etag", "last-modified")
LIMITATION = (
    "This controlled sample covers 150 distinct known PDFs for about five minutes; 150 "
    "logical download starts span at least 298 seconds. An opt-in validated archive "
    "redirect can add one immediate HTTP GET per download, up to 300 GETs for 150 downloads; "
    "30 downloads/minute can therefore involve about 60 GETs/minute. Passing demonstrates the observed run, "
    "not a permanent safe quota or future availability. Prior shared-IP traffic, server "
    "state, and server/CDN caching can affect the outcome. Traffic isolation must be "
    "checked outside this script. The probe never changes the application's default rate."
)


@dataclass(frozen=True)
class PdfTarget:
    url: str
    expected_bytes: int
    expected_sha256: str
    title: str = ""


def _url_identity(url: str) -> tuple[Any, ...]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https" or not parsed.hostname
        or parsed.username or parsed.password or parsed.fragment
    ):
        raise ValueError("PDF URLs must use HTTPS without credentials or fragments")
    return (parsed.hostname.casefold(), parsed.port or 443, parsed.path,
            tuple(sorted(parse_qsl(parsed.query, keep_blank_values=True))))


def safe_response_location(value: str | None, request_url: str) -> str | None:
    """Retain a public HTTP(S) redirect URL without exposing credentials."""
    if not value:
        return None
    try:
        location = urljoin(request_url, value)
        parsed = urlsplit(location)
        host = (parsed.hostname or "").casefold().rstrip(".")
        if (
            parsed.scheme not in {"http", "https"} or not host
            or parsed.username is not None or parsed.password is not None
            or host == "localhost" or host.endswith((".localhost", ".local", ".internal"))
        ):
            return None
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if "." not in host:
                return None
        else:
            if not address.is_global:
                return None
        # Reject malformed ports without resolving the host or following a URL.
        parsed.port
        return location
    except ValueError:
        return None


def archive_redirect_url(source_url: str, value: str | None) -> str | None:
    """Recognize only the archive's documented-by-observation single CGI hop."""
    location = safe_response_location(value, source_url)
    if location is None:
        return None
    try:
        source = urlsplit(source_url)
        destination = urlsplit(location)
        for parsed in (source, destination):
            if (
                parsed.scheme != "https" or parsed.hostname != "digitalcommons.usu.edu"
                or parsed.port not in {None, 443} or parsed.username is not None
                or parsed.password is not None or parsed.fragment
            ):
                return None
        match = re.fullmatch(r"/context/smallsat/article/([0-9]+)/viewcontent/([^/]+)", source.path)
        if match is None or source.query or destination.path != "/cgi/viewcontent.cgi":
            return None
        article, raw_filename = match.groups()
        filename = unquote(raw_filename, errors="strict")
        if not filename or any(character in filename for character in ("/", "\\", "\x00")):
            return None
        arguments = parse_qsl(destination.query, keep_blank_values=True, errors="strict")
        if len(arguments) != 2 or {key for key, _ in arguments} != {"params", "path_info"}:
            return None
        if dict(arguments) != {"params": f"/context/smallsat/article/{article}/", "path_info": filename}:
            return None
        return location
    except (ValueError, UnicodeError):
        return None


def validate_targets(targets: Sequence[PdfTarget]) -> None:
    if len(targets) != REQUEST_LIMIT:
        raise ValueError(f"the manifest must contain exactly {REQUEST_LIMIT} distinct PDFs")
    identities = set()
    hashes = set()
    for target in targets:
        if not isinstance(target.url, str):
            raise ValueError("each target needs a URL string")
        identity = _url_identity(target.url)
        if identity in identities:
            raise ValueError("the manifest contains duplicate PDF URLs")
        identities.add(identity)
        if (
            not isinstance(target.expected_bytes, int) or isinstance(target.expected_bytes, bool)
            or not 1 <= target.expected_bytes <= comparison.MAX_BYTES
        ):
            raise ValueError(f"expected_bytes must be a positive integer no larger than {comparison.MAX_BYTES}")
        if not isinstance(target.expected_sha256, str) or re.fullmatch(r"[0-9a-fA-F]{64}", target.expected_sha256) is None:
            raise ValueError("each target needs a valid 64-digit expected_sha256")
        digest = target.expected_sha256.casefold()
        if digest in hashes:
            raise ValueError("the manifest contains duplicate PDF hashes")
        hashes.add(digest)


def parse_manifest(payload: Any) -> list[PdfTarget]:
    rows = payload.get("pdfs") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("manifest must be a PDF list or an object containing a pdfs list")
    targets = [
        PdfTarget(
            url=row.get("url", row.get("pdf_url")),
            expected_bytes=row.get("expected_bytes", row.get("bytes")),
            expected_sha256=row.get("expected_sha256", row.get("sha256")),
            title=str(row.get("title", "")),
        )
        for row in rows
    ]
    validate_targets(targets)
    return targets


async def run_probe(
    targets: Sequence[PdfTarget],
    client: comparison.SampleClient,
    *,
    max_requests: int = REQUEST_LIMIT,
    allow_archive_redirect: bool = False,
    max_http_requests: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timestamp: Callable[[], str] = lambda: dt.datetime.now(dt.UTC).isoformat(),
    emit: Callable[[dict[str, Any]], None] = lambda _: None,
    emit_http: Callable[[dict[str, Any]], None] = lambda _: None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_targets(targets)
    if not 1 <= max_requests <= REQUEST_LIMIT:
        raise ValueError(f"request budget must be between 1 and {REQUEST_LIMIT}")
    http_limit = REQUEST_LIMIT * (2 if allow_archive_redirect else 1)
    http_budget = http_limit if max_http_requests is None else max_http_requests
    if not 1 <= http_budget <= http_limit:
        raise ValueError(f"HTTP request budget must be between 1 and {http_limit}")
    started = monotonic()
    started_timestamp = timestamp()
    previous_start: float | None = None
    records: list[dict[str, Any]] = []
    http_count = 0
    stop_reason = "completed"

    async def request_hop(url: str, download_index: int, hop_number: int):
        nonlocal http_count
        http_count += 1
        hop_started = monotonic()
        hop: dict[str, Any] = {
            "event": "sustained_curl_http_request", "index": http_count,
            "download_index": download_index, "hop": hop_number, "url": url,
            "timestamp": timestamp(), "started_seconds": round(hop_started - started, 6),
            "transport": "curl_cffi", "status_code": None, "bytes": 0, "sha256": None,
            "content_type": None, "retry_after": None, "server": None, "http_version": None,
            "response_location": None, "cache_headers": {}, "stop_reason": None,
        }
        response = None
        try:
            response = await client.request(url, comparison.MAX_BYTES)
            hop.update(
                status_code=response.status_code, bytes=len(response.body),
                sha256=hashlib.sha256(response.body).hexdigest() if response.body else None,
                content_type=response.headers.get("content-type"), retry_after=response.headers.get("retry-after"),
                server=response.headers.get("server"), http_version=response.http_version,
                response_location=safe_response_location(response.headers.get("location"), url),
                cache_headers={key: response.headers[key] for key in CACHE_HEADERS if key in response.headers},
            )
            if response.status_code == 302:
                if hop_number > 1:
                    hop["stop_reason"] = "extra_archive_redirect"
                elif not allow_archive_redirect:
                    hop["stop_reason"] = "unexpected_http_302"
                elif archive_redirect_url(url, response.headers.get("location")) is None:
                    hop["stop_reason"] = "unexpected_archive_redirect"
            else:
                hop["stop_reason"] = comparison._response_stop_reason(response)
        except Exception as exc:
            hop["stop_reason"] = f"transport_error:{type(exc).__name__}"
        hop["duration_seconds"] = round(monotonic() - hop_started, 6)
        hop["finished_timestamp"] = timestamp()
        emit_http(hop)
        return response, hop

    for target in targets:
        if len(records) >= max_requests:
            stop_reason = "request_budget_reached"
            break
        if http_count >= http_budget:
            stop_reason = "http_request_budget_reached"
            break
        if previous_start is not None:
            remaining = previous_start + INTERVAL_SECONDS - monotonic()
            while remaining > 0:
                await sleep(remaining)
                remaining = previous_start + INTERVAL_SECONDS - monotonic()
        request_started = monotonic()
        record: dict[str, Any] = {
            "event": "sustained_curl_sample", "index": len(records) + 1,
            "timestamp": timestamp(), "transport": "curl_cffi", "rate_per_minute": RATE_PER_MINUTE,
            "started_seconds": round(request_started - started, 6),
            "gap_since_previous_start_seconds": round(request_started - previous_start, 6)
            if previous_start is not None else None,
            **asdict(target), "status_code": None, "bytes": 0, "sha256": None,
            "content_type": None, "retry_after": None, "server": None, "http_version": None,
            "response_location": None,
            "cache_headers": {}, "valid_pdf": False, "accepted_sample": False,
            "stop_reason": None, "http_hops": [], "archive_redirect_url": None,
        }
        previous_start = request_started
        response, hop = await request_hop(target.url, record["index"], 1)
        record["http_hops"].append(hop)
        reason = hop["stop_reason"]
        if response is not None and response.status_code == 302 and allow_archive_redirect:
            redirect_url = archive_redirect_url(target.url, response.headers.get("location"))
            if redirect_url is None:
                reason = "unexpected_archive_redirect"
            elif http_count >= http_budget:
                reason = "http_request_budget_reached"
            else:
                record["archive_redirect_url"] = redirect_url
                response, hop = await request_hop(redirect_url, record["index"], 2)
                record["http_hops"].append(hop)
                reason = hop["stop_reason"]
                if response is not None and response.status_code == 302:
                    reason = "extra_archive_redirect"
        if response is not None and reason is None:
            reason = comparison._response_stop_reason(response)
        if response is not None:
            record.update(
                **{key: hop[key] for key in ("status_code", "bytes", "sha256", "content_type", "retry_after",
                                            "server", "http_version", "response_location", "cache_headers")},
                valid_pdf=reason is None, final_response_url=hop["url"],
            )
            if reason is None:
                if len(response.body) != target.expected_bytes:
                    reason = "expected_size_mismatch"
                elif hop["sha256"] != target.expected_sha256.casefold():
                    reason = "expected_hash_mismatch"
        else:
            record["final_response_url"] = hop["url"]
        record["stop_reason"] = reason
        record["accepted_sample"] = reason is None
        record["duration_seconds"] = round(monotonic() - request_started, 6)
        record["response_exceeded_interval"] = record["duration_seconds"] > INTERVAL_SECONDS
        record["finished_timestamp"] = timestamp()
        records.append(record)
        emit(record)
        if record["stop_reason"]:
            stop_reason = str(record["stop_reason"])
            break
    summary = summarize(records, stop_reason=stop_reason)
    summary.update(
        started_timestamp=started_timestamp, finished_timestamp=timestamp(),
        elapsed_seconds=round(monotonic() - started, 6),
        allow_archive_redirect=allow_archive_redirect, maximum_http_gets=http_limit,
    )
    return records, summary


def summarize(records: Sequence[dict[str, Any]], *, stop_reason: str) -> dict[str, Any]:
    accepted = [record for record in records if record["accepted_sample"]]
    gaps = [record["gap_since_previous_start_seconds"] for record in records[1:]]
    span = records[-1]["started_seconds"] - records[0]["started_seconds"] if records else 0
    observed_rate = 60 * (len(records) - 1) / span if len(records) > 1 and span > 0 else None
    minimum_gap = min(gaps) if gaps else None
    all_accepted = len(accepted) == REQUEST_LIMIT and stop_reason == "completed"
    gap_passed = minimum_gap is not None and minimum_gap >= INTERVAL_SECONDS - GAP_TOLERANCE_SECONDS
    cadence_passed = observed_rate is not None and observed_rate >= CADENCE_MINIMUM
    return {
        "experiment": "150 distinct PDFs at fixed 30/minute", "planned_requests": REQUEST_LIMIT,
        "actual_requests": len(records), "accepted_pdfs": len(accepted),
        "actual_downloads": len(records), "actual_http_gets": sum(len(record["http_hops"]) for record in records),
        "valid_pdfs": sum(record["valid_pdf"] for record in records),
        "failed_checks": sum(record["stop_reason"] is not None for record in records),
        "stop_reason": stop_reason, "request_start_span_seconds": round(span, 6),
        "observed_start_rate_per_minute": round(observed_rate, 6) if observed_rate is not None else None,
        "minimum_start_gap_seconds": minimum_gap, "maximum_start_gap_seconds": max(gaps) if gaps else None,
        "median_duration_seconds": statistics.median(record["duration_seconds"] for record in accepted)
        if accepted else None,
        "total_accepted_bytes": sum(record["bytes"] for record in accepted),
        "slow_response_indices": [record["index"] for record in records if record["response_exceeded_interval"]],
        "acceptance": {
            "all_150_expected_pdfs": all_accepted,
            "all_start_gaps_at_least_2s_with_1ms_tolerance": gap_passed,
            "observed_start_rate_at_least_29_5_per_minute": cadence_passed,
            "technical_acceptance_passed": all_accepted and gap_passed and cadence_passed,
            "external_requirement": "Confirm no other website traffic during the experiment; this script cannot verify it.",
        },
        "samples": list(records),
        "stopping_sample": records[-1] if records and records[-1]["stop_reason"] else None,
        "limitation": LIMITATION,
    }


def markdown_summary(summary: Mapping[str, Any]) -> str:
    runtime = summary["runtime"]
    accepted = summary["acceptance"]
    def displayed(value: Any) -> str:
        return f"{value:.6f}" if value is not None else "not measured"
    lines = [
        "# Controlled sustained curl_cffi probe", "",
        "Generated by `scripts/probe_curl_sustained.py`.", "",
        f"Accepted PDFs: **{summary['accepted_pdfs']}/{REQUEST_LIMIT}**; "
        f"logical downloads attempted: {summary['actual_downloads']}; "
        f"HTTP GETs: {summary['actual_http_gets']}/{summary['maximum_http_gets']}; "
        f"stop reason: `{summary['stop_reason']}`.", "",
        f"UTC: {summary['started_timestamp']} to {summary['finished_timestamp']}.", "",
        f"Python {runtime['python']}; curl_cffi {runtime['curl_cffi']}; Chrome impersonation.", "",
        "One persistent direct session, one active GET, fixed 30 logical download starts/minute, "
        "150 downloads maximum, 2 MiB/file maximum. Automatic redirects remain disabled. "
        f"Opt-in single validated archive redirect: {summary['allow_archive_redirect']}. "
        "No retries, proxies, local response caching, or uncounted warmups. "
        "Every accepted response is HTTP 200, a complete PDF, "
        "and matches its precommitted manifest byte count and SHA-256.", "",
        "| Measurement | Observed |",
        "| --- | ---: |",
        f"| Logical download-start span (s) | {displayed(summary['request_start_span_seconds'])} |",
        f"| Logical download cadence (starts/min) | {displayed(summary['observed_start_rate_per_minute'])} |",
        f"| Minimum logical start gap (s) | {displayed(summary['minimum_start_gap_seconds'])} |",
        f"| Maximum logical start gap (s) | {displayed(summary['maximum_start_gap_seconds'])} |",
        f"| Median accepted request (s) | {displayed(summary['median_duration_seconds'])} |",
        f"| Responses longer than 2 s | {len(summary['slow_response_indices'])} |",
        f"| Accepted bytes | {summary['total_accepted_bytes']} |",
        "",
        f"Technical acceptance passed: **{accepted['technical_acceptance_passed']}**. "
        "Requires all 150 expected PDFs, every gap at least 2 s with 1 ms rounding tolerance, "
        "and observed start cadence at least 29.5/minute. " + accepted["external_requirement"],
    ]
    stopping = summary["stopping_sample"]
    if stopping:
        lines.extend(["", f"Stopped at request {stopping['index']}: HTTP {stopping['status_code']}, "
                      f"`{stopping['stop_reason']}`; server `{stopping['server']}`; "
                      f"Content-Type `{stopping['content_type']}`; Retry-After `{stopping['retry_after']}`."])
        if stopping.get("response_location"):
            lines.extend(["", f"Last response Location: `{stopping['response_location']}`. "
                          "Every physical hop is recorded separately; no further redirect was followed."])
    if summary.get("context_notes"):
        lines.extend(["", "Observed context:", ""])
        lines.extend(f"- {note}" for note in summary["context_notes"])
    manifest = summary.get("manifest")
    if manifest:
        lines.extend(["", f"Manifest: `{manifest['path']}`; SHA-256 `{manifest['sha256']}`."])
    lines.extend(["", LIMITATION, ""])
    return "\n".join(lines)


async def execute(
    targets: Sequence[PdfTarget], output_prefix: Path, *,
    context_notes: Sequence[str] = (), manifest: Mapping[str, str] | None = None,
    allow_archive_redirect: bool = False,
) -> dict[str, Any]:
    validate_targets(targets)
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
        "target_rate_per_minute": RATE_PER_MINUTE, "minimum_start_interval_seconds": INTERVAL_SECONDS,
        "request_budget": REQUEST_LIMIT, "max_bytes": comparison.MAX_BYTES,
        "download_budget": REQUEST_LIMIT, "http_get_budget": REQUEST_LIMIT * (2 if allow_archive_redirect else 1),
        "allow_archive_redirect": allow_archive_redirect,
        "transport": "curl_cffi", "browser_profile": "chrome", "max_active_requests": 1,
        "timeout_seconds": 20, "redirects": False, "retries": 0, "proxies": False,
        "client_response_cache": False, "warmup_requests": 0,
        "default_download_rate_changed": False, "expected_content_checks": ["bytes", "sha256"],
    }
    paths = {extension: Path(f"{output_prefix}.{extension}") for extension in ("jsonl", "json", "md")}
    if any(path.exists() for path in paths.values()):
        raise ValueError("probe result files already exist; choose a fresh output prefix")
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    # Save the full manifest before any request so an interrupted run retains its
    # precommitted plan, runtime, and hard limits alongside flushed sample rows.
    initial = {
        "experiment": "150 distinct PDFs at fixed 30/minute", "runtime": runtime, "config": config,
        "plan": [asdict(target) for target in targets], "manifest": dict(manifest) if manifest else None,
        "context_notes": list(context_notes), "stop_reason": "not_finished", "actual_requests": 0,
    }
    paths["json"].write_text(json.dumps(initial, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with paths["jsonl"].open("x", encoding="utf-8") as stream:
        def write_event(record: dict[str, Any]) -> None:
            stream.write(json.dumps({"runtime": runtime, "config": config, **record}, sort_keys=True) + "\n")
            stream.flush()

        def emit(record: dict[str, Any]) -> None:
            write_event(record)
            print(f"{record['index']}/{REQUEST_LIMIT} curl_cffi 30/min HTTP {record['status_code']} "
                  f"{record['bytes']} bytes {record['duration_seconds']:.3f}s "
                  f"{len(record['http_hops'])} GET(s) {record['stop_reason'] or 'expected PDF'}", flush=True)

        async with AsyncSession(
            impersonate="chrome", timeout=20, allow_redirects=False, trust_env=False,
            max_clients=1, curl_options={CurlOpt.PROXY: ""},
        ) as session:
            _, summary = await run_probe(
                targets, comparison.CurlClient(session), emit=emit, emit_http=write_event,
                allow_archive_redirect=allow_archive_redirect,
            )
    summary.update(runtime=runtime, config=config, plan=initial["plan"],
                   manifest=initial["manifest"], context_notes=list(context_notes))
    paths["json"].write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["md"].write_text(markdown_summary(summary), encoding="utf-8")
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True, help="150 vetted distinct PDFs with expected bytes and SHA-256")
    parser.add_argument("--output-prefix", type=Path, default=Path("curl-sustained-probe"))
    parser.add_argument("--context-note", action="append", default=[])
    parser.add_argument("--allow-archive-redirect", action="store_true",
                        help="allow one explicitly validated USU archive-to-CGI redirect; at most 300 GETs")
    parser.add_argument("--execute", action="store_true", help="execute the plan; otherwise print it without networking")
    args = parser.parse_args(argv)
    try:
        manifest_bytes = args.manifest.read_bytes()
        targets = parse_manifest(json.loads(manifest_bytes))
        if not args.execute:
            print(json.dumps([asdict(target) for target in targets], indent=2))
            return 0
        summary = asyncio.run(execute(
            targets, args.output_prefix, context_notes=args.context_note,
            manifest={"path": str(args.manifest.resolve()), "sha256": hashlib.sha256(manifest_bytes).hexdigest()},
            allow_archive_redirect=args.allow_archive_redirect,
        ))
        print(f"Stopped: {summary['stop_reason']}. Technical acceptance: "
              f"{summary['acceptance']['technical_acceptance_passed']}. {LIMITATION}")
        return 0 if summary["acceptance"]["technical_acceptance_passed"] else 1
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
