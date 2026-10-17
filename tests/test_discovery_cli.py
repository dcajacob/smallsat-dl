import asyncio
import json
from pathlib import Path

import pytest

import smallsat_dl.cli as cli
from smallsat_dl.conversion import ConversionStats
from smallsat_dl.scraper import DownloadStats


def install_pipeline(monkeypatch, *, constructor_error=None, close_error=None):
    events = []
    caches = []
    downloader_options = []

    class FakeCache:
        def __init__(self, path, *, ttl, log):
            self.path = path
            self.ttl = ttl
            self.closed = False
            caches.append(self)

        def close(self):
            self.closed = True
            events.append("cache_closed")

    class FakeQueue:
        def __init__(self, **kwargs):
            events.append("queue_created")

        def submit(self, path):
            pass

        async def finish(self):
            events.append("queue_finished")
            return ConversionStats()

        async def close(self):
            events.append("queue_closed")

    class FakeDownloader:
        def __init__(self, **kwargs):
            downloader_options.append(kwargs)
            events.append("downloader_created")
            if constructor_error is not None:
                raise constructor_error

        async def discover_latest_year(self):
            events.append("discovery")
            return 2025

        async def download_years(self, years, output_dir, *, dry_run=False):
            events.append("download")
            return DownloadStats()

        async def close(self):
            events.append("downloader_closed")
            if close_error is not None:
                raise close_error

    monkeypatch.setattr(cli, "MetadataCache", FakeCache)
    monkeypatch.setattr(cli, "PresentationConversionQueue", FakeQueue)
    monkeypatch.setattr(cli, "SmallSatDownloader", FakeDownloader)
    return events, caches, downloader_options


def test_discovery_cache_and_transport_parser_defaults():
    parsed = cli.build_parser().parse_args([])

    assert parsed.discovery == "auto"
    assert parsed.transport == "curl-cffi"
    assert parsed.no_cache is False
    assert parsed.cache_dir is None
    assert parsed.cache_ttl == 86400
    assert parsed.downloads_per_minute == 30


@pytest.mark.parametrize(("discovery", "transport"), [("oai", "curl-cffi"), ("html", "httpx")])
def test_explicit_discovery_and_transport_are_forwarded(monkeypatch, tmp_path, discovery, transport):
    events, _, options = install_pipeline(monkeypatch)

    result = asyncio.run(cli._async_main([
        "--output", str(tmp_path), "--discovery", discovery, "--transport", transport,
    ]))

    assert result == 0
    assert options[0]["discovery"] == discovery
    assert options[0]["transport"] == transport
    assert "queue_finished" in events
    assert "queue_closed" in events


@pytest.mark.parametrize(("rate_args", "expected"), [
    ([], 30.0), (["--downloads-per-minute", "15"], 15.0),
])
def test_default_file_rate_and_explicit_override_are_forwarded(monkeypatch, tmp_path, rate_args, expected):
    _, _, options = install_pipeline(monkeypatch)

    result = asyncio.run(cli._async_main(["--output", str(tmp_path), *rate_args]))

    assert result == 0
    assert options[0]["downloads_per_minute"] == expected


def test_cache_defaults_beneath_selected_output_and_closes(monkeypatch, tmp_path):
    events, caches, options = install_pipeline(monkeypatch)
    output = tmp_path / "downloads"

    result = asyncio.run(cli._async_main(["--output", str(output)]))

    assert result == 0
    assert len(caches) == 1
    assert caches[0].path == output / ".smallsat-cache" / "metadata.sqlite3"
    assert caches[0].ttl == 86400
    assert options[0]["cache"] is caches[0]
    assert options[0]["transport"] == "curl-cffi"
    assert caches[0].closed
    assert events.index("downloader_closed") < events.index("cache_closed")
    assert not output.exists()


@pytest.mark.parametrize("ttl", ["0", "12.5"])
def test_custom_cache_directory_and_ttl_are_forwarded(monkeypatch, tmp_path, ttl):
    _, caches, options = install_pipeline(monkeypatch)
    cache_dir = tmp_path / "custom cache"

    result = asyncio.run(cli._async_main([
        "--output", str(tmp_path / "output"), "--cache-dir", str(cache_dir), "--cache-ttl", ttl,
    ]))

    assert result == 0
    assert caches[0].path == cache_dir / "metadata.sqlite3"
    assert caches[0].ttl == float(ttl)
    assert options[0]["cache"] is caches[0]
    assert caches[0].closed


def test_no_cache_bypasses_creation_even_with_cache_options(monkeypatch, tmp_path):
    _, caches, options = install_pipeline(monkeypatch)

    result = asyncio.run(cli._async_main([
        "--output", str(tmp_path), "--no-cache", "--cache-dir", str(tmp_path / "cache"),
        "--cache-ttl", "0",
    ]))

    assert result == 0
    assert caches == []
    assert options[0]["cache"] is None


@pytest.mark.parametrize("arguments", [
    ["--discovery", "unknown"],
    ["--transport", "requests"],
    ["--cache-ttl=-1"],
    ["--cache-ttl=nan"],
    ["--cache-ttl=inf"],
    ["--cache-ttl=-inf"],
    *[[f"{option}={value}"]
      for option in ("--delay", "--downloads-per-minute", "--retry-backoff")
      for value in ("nan", "inf", "-inf")],
])
def test_invalid_options_are_rejected_before_resource_or_network_setup(monkeypatch, arguments):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid arguments reached resource/network setup")

    monkeypatch.setattr(cli, "MetadataCache", forbidden)
    monkeypatch.setattr(cli, "SmallSatDownloader", forbidden)
    monkeypatch.setattr(cli, "PresentationConversionQueue", forbidden)

    with pytest.raises(SystemExit) as failure:
        asyncio.run(cli._async_main(arguments))
    assert failure.value.code == 2


def test_cache_and_diagnostics_close_even_when_downloader_close_fails(monkeypatch, tmp_path):
    events, caches, _ = install_pipeline(monkeypatch, close_error=RuntimeError("close failed"))
    diagnostics = tmp_path / "diagnostics.jsonl"

    with pytest.raises(RuntimeError, match="close failed"):
        asyncio.run(cli._async_main([
            "--output", str(tmp_path), "--diagnostics", str(diagnostics),
        ]))

    assert "queue_finished" in events
    assert "queue_closed" in events
    assert caches[0].closed
    records = [json.loads(line) for line in diagnostics.read_text().splitlines()]
    assert records[-1]["event"] == "run_end"


def test_missing_default_transport_is_concise_before_network_and_closes_resources(
    monkeypatch, tmp_path, capsys,
):
    events, caches, options = install_pipeline(
        monkeypatch, constructor_error=RuntimeError("curl_cffi is not installed; run uv sync"),
    )
    diagnostics = tmp_path / "diagnostics.jsonl"

    result = asyncio.run(cli._async_main([
        "--output", str(tmp_path), "--diagnostics", str(diagnostics),
    ]))

    assert result == 1
    assert options[0]["transport"] == "curl-cffi"
    assert "discovery" not in events
    assert "download" not in events
    assert "queue_finished" not in events
    assert "queue_closed" in events
    assert caches[0].closed
    records = [json.loads(line) for line in diagnostics.read_text().splitlines()]
    assert records[-1]["event"] == "run_end"
    error = capsys.readouterr().err
    assert "curl_cffi is not installed; run uv sync" in error
    assert "Traceback" not in error
