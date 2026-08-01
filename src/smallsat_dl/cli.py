"""Command-line interface for the SmallSat downloader."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
import time
from typing import TypeVar

from rich.console import Console
from rich.markup import escape
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Column

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
        default=Path.cwd(),
        help="download root directory; defaults to the current directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be downloaded without writing PDFs",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="base request delay with up to 100%% random jitter; defaults to 0.25",
    )
    parser.add_argument(
        "--download-delay",
        type=float,
        default=1.0,
        help="base PDF delay with up to 100%% random jitter; defaults to 1.0",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="maximum concurrent network requests; defaults to 8",
    )
    parser.add_argument(
        "--download-concurrency",
        type=int,
        default=1,
        help="adaptive maximum concurrent PDF downloads; defaults to 1",
    )
    parser.add_argument(
        "--retry-rounds",
        type=int,
        default=5,
        help="deferred download retry rounds; defaults to 5",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=60.0,
        help="base seconds between deferred retry rounds; defaults to 60",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=argparse.SUPPRESS,
    )
    return parser


def selected_years_from_args(args: argparse.Namespace, available_years: list[int]) -> list[int] | None:
    validate_year_selector_shape(args)

    if args.latest:
        return [max(available_years)]
    if args.all:
        return sorted(available_years, reverse=True)
    if args.years:
        return normalize_requested_years(args.years, available_years)
    if args.from_year is not None and args.to_year is not None:
        start, end = sorted((args.from_year, args.to_year))
        return normalize_requested_years(list(range(start, end + 1)), available_years)
    return None


def validate_year_selector_shape(args: argparse.Namespace) -> None:
    selector_count = sum(
        [
            bool(args.latest),
            bool(args.years),
            bool(args.all),
            args.from_year is not None or args.to_year is not None,
        ]
    )
    if selector_count > 1:
        raise ValueError("choose only one year selector: --latest, --year, --from-year/--to-year, or --all")

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

    if args.delay < 0:
        parser.error("--delay must be zero or greater")
    if args.download_delay < 0:
        parser.error("--download-delay must be zero or greater")
    if args.concurrency < 1:
        parser.error("--concurrency must be at least 1")
    if args.download_concurrency < 1:
        parser.error("--download-concurrency must be at least 1")
    if args.retry_rounds < 0:
        parser.error("--retry-rounds must be zero or greater")
    if args.retry_backoff < 0:
        parser.error("--retry-backoff must be zero or greater")

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

    def update_paper_progress(title: str, completed: int, total: int) -> None:
        paper_progress.update(
            paper_task,
            description=f"Paper: {escape(title)}",
            completed=completed,
            total=total,
        )

    downloader = SmallSatDownloader(
        base_url=args.base_url,
        delay=args.delay,
        download_delay=args.download_delay,
        concurrency=args.concurrency,
        download_concurrency=args.download_concurrency,
        retry_rounds=args.retry_rounds,
        retry_backoff=args.retry_backoff,
        log=lambda message: console.print(message, markup=False),
        progress=update_paper_progress,
    )

    try:
        return await _run_downloader(args, parser, downloader, paper_progress)
    finally:
        await downloader.close()


async def _run_downloader(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    downloader: SmallSatDownloader,
    paper_progress: Progress,
) -> int:
    try:
        validate_year_selector_shape(args)
        if args.latest:
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
        if years is None:
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
    console.print(
        f"[bold]Concurrent requests:[/bold] {args.concurrency}"
    )
    console.print(
        f"[bold]Adaptive PDF concurrency maximum:[/bold] {args.download_concurrency}"
    )
    console.print(f"[bold]PDF request delay:[/bold] {args.download_delay:.2f}s + jitter")
    console.print(f"[bold]Deferred retry rounds:[/bold] {args.retry_rounds}")
    console.print(f"[bold]Retry round base backoff:[/bold] {args.retry_backoff:.1f}s")

    console.print("[bold cyan]Processing papers...[/bold cyan]")
    download_started = time.monotonic()
    with paper_progress:
        stats = await downloader.download_years(years, args.output, dry_run=args.dry_run)
    elapsed = time.monotonic() - download_started
    console.print(
        "[bold green]Done:[/bold green] "
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
