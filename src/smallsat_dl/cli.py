"""Command-line interface for the SmallSat downloader."""

from __future__ import annotations

import argparse
import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TypeVar

from rich.console import Console
from rich.markup import escape
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.table import Column

from .cache import DEFAULT_CACHE_TTL, MetadataCache
from .conversion import PresentationConversionQueue
from .diagnostics import JsonlDiagnostics
from .scraper import DEFAULT_BASE_URL, SmallSatDownloader

console = Console()
error_console = Console(stderr=True)
T = TypeVar("T")
PAPER_DESCRIPTION_WIDTH = 60
MAX_DISPLAYED_FAILURES = 20


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smallsat-dl",
        description="Download Small Satellite Conference papers from Digital Commons.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="download the newest archive year containing at least one paper PDF",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="prompt for latest, specific years, a range, or all years",
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        dest="years",
        help="download a specific year; may be repeated",
    )
    parser.add_argument("--from-year", type=int, help="first year in a range")
    parser.add_argument("--to-year", type=int, help="last year in a range")
    parser.add_argument("--all", action="store_true", help="download all discovered years")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.cwd() / "downloads",
        help="download root directory; defaults to ./downloads",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be downloaded without writing PDFs",
    )
    parser.add_argument(
        "--discovery",
        choices=("auto", "oai", "html"),
        default="auto",
        help="metadata discovery method; defaults to auto (OAI with HTML fallback)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="disable metadata cache reads and writes",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="metadata cache directory; defaults to OUTPUT/.smallsat-cache",
    )
    parser.add_argument(
        "--cache-ttl",
        type=float,
        default=DEFAULT_CACHE_TTL,
        help="metadata cache lifetime in seconds; defaults to 86400 (24 hours)",
    )
    parser.add_argument(
        "--transport",
        choices=("httpx", "curl-cffi"),
        default="curl-cffi",
        help="HTTP transport; defaults to curl-cffi; httpx is also available",
    )
    parser.add_argument(
        "--no-convert",
        action="store_true",
        help="keep original presentation files without generating PDF copies",
    )
    parser.add_argument(
        "--conversion-workers",
        type=int,
        choices=(1, 2),
        default=1,
        help="maximum concurrent background PDF conversions; defaults to 1",
    )
    parser.add_argument(
        "--diagnostics",
        type=Path,
        help="write structured JSON Lines runtime diagnostics",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="base request delay with up to 100%% random jitter; defaults to 0.25",
    )
    parser.add_argument(
        "--downloads-per-minute",
        type=float,
        default=30.0,
        help="strict file request rate with no initial burst; defaults to 30",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="maximum concurrent network requests; defaults to 8",
    )
    parser.add_argument(
        "--retry-rounds",
        type=int,
        default=20,
        help="deferred download retry rounds; defaults to 20",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=60.0,
        help="base cooldown seconds between retry rounds; defaults to 60",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=argparse.SUPPRESS,
    )
    return parser


def selected_years_from_args(args: argparse.Namespace, available_years: list[int]) -> list[int] | None:
    validate_year_selector_shape(args)

    if args.all:
        return sorted(available_years, reverse=True)
    if args.years:
        return normalize_requested_years(args.years, available_years)
    if args.from_year is not None and args.to_year is not None:
        start, end = sorted((args.from_year, args.to_year))
        return normalize_requested_years(list(range(start, end + 1)), available_years)
    return None


def defaults_to_latest(args: argparse.Namespace) -> bool:
    return not any(
        [
            args.interactive,
            args.latest,
            args.years,
            args.all,
            args.from_year is not None,
            args.to_year is not None,
        ]
    )


def validate_year_selector_shape(args: argparse.Namespace) -> None:
    selector_count = sum(
        [
            bool(args.interactive),
            bool(args.latest),
            bool(args.years),
            bool(args.all),
            args.from_year is not None or args.to_year is not None,
        ]
    )
    if selector_count > 1:
        raise ValueError(
            "choose only one year selector: --latest, --interactive, --year, "
            "--from-year/--to-year, or --all"
        )

    if args.from_year is None and args.to_year is not None:
        raise ValueError("--to-year requires --from-year")
    if args.from_year is not None and args.to_year is None:
        raise ValueError("--from-year requires --to-year")


def normalize_requested_years(requested_years: list[int], available_years: list[int]) -> list[int]:
    available = set(available_years)
    missing = sorted(set(requested_years) - available)
    if missing:
        missing_list = ", ".join(str(year) for year in missing)
        raise ValueError(f"requested year(s) are not available: {missing_list}")
    return sorted(set(requested_years), reverse=True)


def prompt_for_years(available_years: list[int], latest_year_with_pdf: int) -> list[int]:
    earliest = min(available_years)
    newest_archive = max(available_years)

    console.print("[bold]Select SmallSat years to download:[/bold]")
    console.print(f"  1. Latest year containing a paper PDF ({latest_year_with_pdf})")
    console.print("  2. Specific year(s)")
    console.print(f"  3. Year range ({earliest}-{newest_archive})")
    console.print(f"  4. All available years ({earliest}-{newest_archive})")

    while True:
        choice = input("Choice [1]: ").strip() or "1"
        if choice == "1":
            return [latest_year_with_pdf]
        if choice == "2":
            raw = input("Year(s), separated by spaces or commas: ").strip()
            years = parse_year_list(raw)
            return normalize_requested_years(years, available_years)
        if choice == "3":
            from_year = int(input("From year: ").strip())
            to_year = int(input("To year: ").strip())
            start, end = sorted((from_year, to_year))
            return normalize_requested_years(list(range(start, end + 1)), available_years)
        if choice == "4":
            return sorted(available_years, reverse=True)
        console.print("[yellow]Please choose 1, 2, 3, or 4.[/yellow]")


def parse_year_list(raw: str) -> list[int]:
    values = [item for item in raw.replace(",", " ").split() if item]
    if not values:
        raise ValueError("at least one year is required")
    return [int(value) for value in values]


async def search_archive(message: str, operation: Callable[[], Awaitable[T]]) -> T:
    console.print(f"[bold cyan]{message}[/bold cyan]")
    with console.status("[cyan]Checking Digital Commons archive pages...[/cyan]", spinner="dots"):
        return await operation()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv))


async def _async_main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_year_selector_shape(args)
    except ValueError as exc:
        parser.error(str(exc))
    if not math.isfinite(args.delay) or args.delay < 0:
        parser.error("--delay must be a finite number zero or greater")
    if not math.isfinite(args.downloads_per_minute) or args.downloads_per_minute <= 0:
        parser.error("--downloads-per-minute must be a finite number greater than zero")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.retry_rounds < 0:
        parser.error("--retry-rounds must be zero or greater")
    if not math.isfinite(args.retry_backoff) or args.retry_backoff < 0:
        parser.error("--retry-backoff must be a finite number zero or greater")
    if not math.isfinite(args.cache_ttl) or args.cache_ttl < 0:
        parser.error("--cache-ttl must be a finite number zero or greater")

    paper_progress = Progress(
        SpinnerColumn(),
        TextColumn(
            "[bold cyan]{task.description}[/bold cyan]",
            table_column=Column(
                width=PAPER_DESCRIPTION_WIDTH,
                no_wrap=True,
                overflow="ellipsis",
            ),
        ),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    )
    paper_task = paper_progress.add_task("Preparing downloads...", total=None)
    diagnostics = JsonlDiagnostics(args.diagnostics) if args.diagnostics else None
    if diagnostics is not None:
        diagnostics.record(
            "run_start",
            arguments=vars(args),
        )

    def update_paper_progress(title: str, completed: int, total: int) -> None:
        paper_progress.update(
            paper_task,
            description=f"Paper: {escape(title)}",
            completed=completed,
            total=total,
        )

    diagnostic = (
        (lambda event, fields: diagnostics.record(event, **fields))
        if diagnostics is not None
        else None
    )
    cache = None
    conversions = None
    downloader = None
    try:
        if not args.no_cache:
            cache_dir = args.cache_dir if args.cache_dir is not None else args.output / ".smallsat-cache"
            cache = MetadataCache(
                cache_dir / "metadata.sqlite3",
                ttl=args.cache_ttl,
                log=lambda message: console.print(message, markup=False),
            )
        if not args.no_convert and not args.dry_run:
            conversions = PresentationConversionQueue(
                workers=args.conversion_workers,
                log=lambda message: console.print(message, markup=False),
                diagnostic=diagnostic,
            )
        try:
            downloader = SmallSatDownloader(
                base_url=args.base_url,
                delay=args.delay,
                downloads_per_minute=args.downloads_per_minute,
                concurrency=args.concurrency,
                retry_rounds=args.retry_rounds,
                retry_backoff=args.retry_backoff,
                log=lambda message: console.print(message, markup=False),
                progress=update_paper_progress,
                diagnostic=diagnostic,
                file_ready=conversions.submit if conversions is not None else None,
                discovery=args.discovery,
                cache=cache,
                transport=args.transport,
            )
        except RuntimeError as exc:
            error_console.print(f"Error: {exc}", markup=False)
            return 1
        result = await _run_downloader(args, parser, downloader, paper_progress)
        if conversions is not None:
            console.print("[bold cyan]Finishing presentation PDF conversions...[/bold cyan]")
            conversion_stats = await conversions.finish()
            console.print(
                "[bold]PDF conversions:[/bold] "
                f"{conversion_stats.converted} converted, "
                f"{conversion_stats.skipped} already available, "
                f"{len(conversion_stats.failures)} failure(s)."
            )
            for failure in conversion_stats.failures[:MAX_DISPLAYED_FAILURES]:
                error_console.print(f"  {failure.source}: {failure.message}", markup=False)
            if len(conversion_stats.failures) > MAX_DISPLAYED_FAILURES:
                error_console.print(
                    f"  ... and {len(conversion_stats.failures) - MAX_DISPLAYED_FAILURES} "
                    "more conversion failure(s)."
                )
            if conversion_stats.failures:
                result = 1
        return result
    finally:
        try:
            if conversions is not None:
                await conversions.close()
        finally:
            try:
                if downloader is not None:
                    await downloader.close()
            finally:
                try:
                    if cache is not None:
                        cache.close()
                finally:
                    if diagnostics is not None:
                        diagnostics.record("run_end")
                        diagnostics.close()


async def _run_downloader(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    downloader: SmallSatDownloader,
    paper_progress: Progress,
) -> int:
    try:
        validate_year_selector_shape(args)
        if args.latest or defaults_to_latest(args):
            latest_year = await search_archive(
                "Searching for the latest year containing a paper PDF...",
                downloader.discover_latest_year,
            )
            years = [latest_year]
        else:
            available_years = await search_archive(
                "Searching for available SmallSat archive years...",
                downloader.discover_available_years,
            )
            years = selected_years_from_args(args, available_years)
        if args.interactive:
            latest_year = await search_archive(
                "Finding the newest archive year containing a paper PDF...",
                downloader.discover_latest_year,
            )
            years = prompt_for_years(available_years, latest_year)
    except ValueError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        error_console.print("\n[yellow]Cancelled.[/yellow]")
        return 130
    except Exception as exc:  # noqa: BLE001 - command boundary prints a concise failure.
        error_console.print(f"[bold red]Error:[/bold red] {exc}")
        return 1

    console.print(
        f"[bold]Downloading years:[/bold] {', '.join(str(year) for year in years)}"
    )
    console.print(f"[bold]Output directory:[/bold] {args.output}")
    if args.diagnostics:
        console.print(f"[bold]Diagnostics:[/bold] {args.diagnostics}")

    console.print("[bold cyan]Processing papers...[/bold cyan]")
    download_started = time.monotonic()
    with paper_progress:
        stats = await downloader.download_years(years, args.output, dry_run=args.dry_run)
    elapsed = time.monotonic() - download_started
    console.print(
        "[bold green]Done:[/bold green] "
        f"{stats.file_targets} total file(s), "
        f"{stats.downloaded} downloaded, "
        f"{stats.skipped} skipped, "
        f"{stats.dry_run} dry-run target(s), "
        f"{stats.unavailable} unavailable, "
        f"{len(stats.failures)} failure(s), "
        f"{stats.first_pass_deferred} first-pass deferred, "
        f"{stats.retry_recovered} retry recovered, "
        f"{stats.retry_exhausted} retry exhausted, "
        f"{stats.first_pass_rate_limits}/{stats.retry_rate_limits} "
        "first-pass/retry rate limit(s), "
        f"{stats.retry_rounds} retry round(s), "
        f"{elapsed:.1f}s elapsed."
    )

    if stats.failures:
        error_console.print("[bold red]Failures:[/bold red]")
        for failure in stats.failures[:MAX_DISPLAYED_FAILURES]:
            error_console.print(f"  {failure.url}: {failure.message}", markup=False)
        hidden_failures = len(stats.failures) - MAX_DISPLAYED_FAILURES
        if hidden_failures > 0:
            error_console.print(f"  ... and {hidden_failures} more failure(s).")
        return 1

    return 0
