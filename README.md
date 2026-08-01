# smallsat-dl

CLI downloader for the Small Satellite Conference proceedings hosted on Utah
State Digital Commons.

The tool ports the old notebook workflow into an installable uv project. It
downloads papers into the same basic layout:

```text
YEAR/
  SESSION/
    PAPER TITLE.pdf
    presentations/
      PAPER TITLE.pdf
```

Existing files are skipped so repeated runs can resume a partial download.
Archive searches and downloads show Rich status output, including an immediate
message before the potentially slow latest-year search begins and a per-session
paper progress bar with the current title.

When categorized sessions are available, the duplicate `All YEAR Content`
listing is ignored. Entries without a paper PDF are reported as unavailable,
not as download failures.

Page and PDF requests use bounded asynchronous concurrency. The default is
eight simultaneous page requests and one PDF download at a time. These can be
adjusted independently with `--concurrency` and
`--download-concurrency`.
Latest-year discovery checks only the current and previous year, scanning their
session and event pages concurrently. Before August 1 it skips the current year,
since that conference archive is not expected to contain papers yet.

Page requests wait for a randomized interval between `--delay` and twice
`--delay` (0.25–0.5 seconds by default). PDF transfers use the slower
`--download-delay` interval (1–2 seconds by default), avoiding sustained
download bursts while retaining concurrent page discovery.

Rate-limited PDF responses are deferred immediately so the first pass keeps
moving. Deferred files are retried in later globally paced rounds with jittered
exponential backoff starting at 60 seconds. The number of rounds and base
cooldown are configurable with `--retry-rounds` and `--retry-backoff`.

Download concurrency follows additive-increase/multiplicative-decrease (AIMD):
403/429 responses halve the active limit, while sustained successful downloads
add slots back one at a time up to the configured maximum. The default maximum
is one because Digital Commons consistently throttles parallel PDF transfers;
higher values are opt-in.

Event pages are inspected concurrently, then file transfers run through a
bounded worker queue. This prevents large years from creating hundreds of
simultaneous retry loops. First-pass and each retry-round outcomes are logged,
and the final summary includes recovered, exhausted, rate-limit, failure, and
elapsed-time diagnostics.

## Setup

```bash
uv sync
```

Run the CLI through uv:

```bash
uv run smallsat-dl --help
```

You can also run it as a module:

```bash
uv run python -m smallsat_dl --help
```

## Usage

With no year selector, `smallsat-dl` prompts for what to download:

```bash
uv run smallsat-dl
```

Download the newest archive year containing at least one paper PDF. This skips
an upcoming conference year whose archive exists but does not contain papers
yet:

```bash
uv run smallsat-dl --latest
```

Download one or more specific years:

```bash
uv run smallsat-dl --year 2025 --year 2024
```

Download a range:

```bash
uv run smallsat-dl --from-year 2020 --to-year 2025
```

Download every discovered archive year:

```bash
uv run smallsat-dl --all
```

Choose a destination directory instead of the current directory:

```bash
uv run smallsat-dl --latest --output /path/to/downloads
```

Preview target files without downloading PDFs:

```bash
uv run smallsat-dl --latest --dry-run
```

Adjust network concurrency:

```bash
uv run smallsat-dl --latest --concurrency 12
```

Set the maximum adaptive PDF download concurrency:

```bash
uv run smallsat-dl --latest --download-concurrency 12
```

Change the number of deferred retry rounds:

```bash
uv run smallsat-dl --latest --retry-rounds 3
```

Tune PDF pacing and retry cooldown:

```bash
uv run smallsat-dl --latest --download-delay 1.5 --retry-backoff 90
```

## Development

```bash
uv run pytest
```
