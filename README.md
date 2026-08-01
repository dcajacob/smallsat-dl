# smallsat-dl

CLI downloader for the Small Satellite Conference proceedings hosted on Utah
State [Digital Commons](https://digitalcommons.usu.edu/smallsat/).

Downloads are resumable: existing files are skipped, unavailable papers are
reported, and rate-limited files are retried automatically.

## Setup

```bash
uv sync
uv run smallsat-dl --help
```

## Usage

By default, download the latest year containing papers:

```bash
uv run smallsat-dl
```

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
```

Files are organized by year and session:

```text
YEAR/
  SESSION/
    PAPER TITLE.pdf
    presentations/
      PAPER TITLE.pdf
```

## Development

```bash
uv run pytest
```
