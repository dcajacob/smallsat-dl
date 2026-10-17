# smallsat-dl

CLI downloader for the Small Satellite Conference proceedings hosted on Utah
State [Digital Commons](https://digitalcommons.usu.edu/smallsat/).

Downloads are resumable: existing files are skipped, unavailable papers are
reported, and rate-limited files are retried automatically.
Files are published only after a complete write, so failed writes can be retried
on the next run. File downloads are strictly paced at 30 per minute by default.
Downloads stream to temporary files, including large keynote videos, so file size
does not determine memory usage.

Discovery uses each year's OAI-PMH metadata feed, with HTML session listings for
folder names. Missing or ambiguous metadata falls back to paper pages. If the
OAI endpoint is unavailable, automatic discovery falls back to HTML crawling.
Only complete OAI harvests and validated HTML pages enter the persistent cache;
PDFs, challenge pages, and failed responses are never cached.

## Setup

```bash
uv sync
uv run smallsat-dl --help
```

Automatic presentation-to-PDF conversion uses LibreOffice (`libreoffice` or
`soffice` on `PATH`). Downloads continue while a background queue runs one
conversion at a time. Use `--conversion-workers 2` to allow two at once, or
`--no-convert` to download originals only.

## Usage

By default, download the latest year containing papers:

```bash
uv run smallsat-dl
```

Files are written under `./downloads` unless `--output` is provided.

Open the interactive year-selection prompt:

```bash
uv run smallsat-dl --interactive
```

Select years or a range:

```bash
uv run smallsat-dl --year 2025 --year 2024
uv run smallsat-dl --from-year 2020 --to-year 2025
```

Other useful options:

```bash
uv run smallsat-dl --all
uv run smallsat-dl --latest --output /path/to/downloads
uv run smallsat-dl --latest --dry-run
uv run smallsat-dl --latest --diagnostics run.jsonl
uv run smallsat-dl --latest --conversion-workers 2
uv run smallsat-dl --latest --no-convert
uv run smallsat-dl --latest --discovery html
uv run smallsat-dl --latest --no-cache
```

Metadata is cached for 24 hours in `OUTPUT/.smallsat-cache/metadata.sqlite3`.
Use `--cache-dir PATH` to share a cache between output directories, or
`--cache-ttl SECONDS` to change its lifetime (`0` forces fresh metadata).
`--discovery oai` reports an OAI failure instead of switching to HTML; individual
records with insufficient metadata still use their paper pages. Dry runs may
populate the metadata cache but do not download or convert files.

The default transport is `curl_cffi` with its Chrome browser profile, installed
by `uv sync`. HTTPX remains selectable without changing the request limiter:

```bash
uv run smallsat-dl --transport httpx
```

Files are organized by year and session:

```text
downloads/
  YEAR/
    SESSION/
      PAPER TITLE.pdf
      presentations/
        PAPER TITLE.pptx
        PAPER TITLE.pdf
```

Attachments keep the extension supplied by their original filename or download
URL, including `.pdf`, `.pptx`, images, videos, and subtitles. If neither provides
a format, the extension is `.bin`. Rerunning downloads missing originals under
the corrected names. Files from older versions are retained unless an invalid
presentation `.pdf` is replaced by a successfully converted PDF copy.
All attachments are included; additional attachments receive ` - Attachment 2`,
` - Attachment 3`, etc. in their names to avoid collisions. Primary media also
keeps its original extension, such as `.mp4` for keynote videos.
Older attachments stored under `.pdf` names are reused without downloading when
their actual format can be verified. The correctly named original is preserved
if its old name is replaced by a converted PDF.

PowerPoint and OpenDocument presentations also receive a PDF copy beside the
original. Originals remain intact, and existing valid PDF copies are skipped.
Conversions use isolated LibreOffice profiles and temporary output directories;
a failed conversion leaves the original available for a retry on the next run.
The command waits for the queue before exiting and reports conversion failures
with a nonzero exit status. Images, videos, and subtitles retain their original
format. Dry runs do not start conversions.

## Development

```bash
uv run pytest
```

The controlled transport-comparison script prints its request plan by default:

```bash
printf '%s\n' 'https://digitalcommons.usu.edu/cgi/viewcontent.cgi?article=6425&context=smallsat' > urls.txt
uv run python scripts/compare_transports.py \
  --urls urls.txt --include-20
```

Add `--execute --output-prefix PATH` to run it. It makes at most 24 requests,
uses one shared rate limit across both clients, and stops on the first block or
invalid response. It disables response caching, redirects, proxies, and retries.
A short successful sample does not establish a sustainable archive-download rate.

The curl-only rate probe prints a capped plan for 15, 20, and 30 requests/minute:

```bash
uv run python scripts/probe_curl_rates.py \
  --urls urls.txt
```

It allows at most 18 sequential PDF requests, stops at the first block or invalid
response, and requires `--execute --output-prefix PATH` to send requests. Repeated
URLs may benefit from server caches, and a short passing tier cannot establish a
sustained quota.

The default is 30 file downloads/minute after a longer controlled curl_cffi run
accepted all 150 distinct PDFs with matching expected sizes and SHA-256 hashes.
See [the validation summary](docs/validation.md) for results and limits.
Use `--downloads-per-minute 15` for a lower rate.

The longer probe prints its request plan without sending traffic:

```bash
uv run python scripts/probe_curl_sustained.py \
  --manifest manifest.json \
  --allow-archive-redirect
```

Execution requires `--execute --output-prefix PATH`. It runs sequentially at
30 downloads/minute, checks each PDF against the manifest, and stops at the first
block, invalid response, or content mismatch. It allows one explicit validated
archive redirect per file, with every HTTP request counted. Automatic redirects,
retries, proxies, and response caching are disabled.

Supply a manifest containing exactly 150 distinct, previously verified PDFs,
each no larger than 2 MiB. Its JSON format is a list of objects with `url`,
`expected_bytes`, and `expected_sha256` fields. The manifest and raw experiment
logs are local artifacts and are not included in the repository.
