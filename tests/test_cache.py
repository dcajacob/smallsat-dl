import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from smallsat_dl.cache import DEFAULT_CACHE_TTL, MetadataCache


URL = "https://example.test/archive/2025"
TEXT = "<html>validated archive metadata</html>"


def test_cache_is_persistent_and_replaces_accepted_metadata(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    cache = MetadataCache(path)
    assert cache.ttl == DEFAULT_CACHE_TTL == 86400
    cache.put(URL, TEXT)
    cache.close()

    reopened = MetadataCache(path)
    assert reopened.get(URL) == TEXT
    reopened.put(URL, "updated metadata")
    assert MetadataCache(path).get(URL) == "updated metadata"


def test_freshness_expiry_and_clock_rollback(monkeypatch, tmp_path: Path):
    clock = [100.0]
    monkeypatch.setattr("smallsat_dl.cache.time.time", lambda: clock[0])
    cache = MetadataCache(tmp_path / "metadata.sqlite3", ttl=60)
    cache.put(URL, TEXT)
    assert cache.get(URL) == TEXT
    clock[0] = 159.999
    assert cache.get(URL) == TEXT
    clock[0] = 160.0
    assert cache.get(URL) is None
    clock[0] = 99.0
    assert cache.get(URL) is None
    clock[0] = 200.0
    cache.put(URL, "refreshed metadata")
    assert cache.get(URL) == "refreshed metadata"


def test_zero_ttl_always_refreshes(tmp_path: Path):
    cache = MetadataCache(tmp_path / "metadata.sqlite3", ttl=0)
    cache.put(URL, TEXT)
    assert cache.get(URL) is None


def test_misses_and_invalidation_do_not_create_cache_directories(tmp_path: Path):
    parent = tmp_path / "new" / "cache"
    cache = MetadataCache(parent / "metadata.sqlite3")
    assert cache.get(URL) is None
    cache.invalidate(URL)
    cache.clear()
    cache.close()
    assert not parent.exists()


def test_existing_cache_misses_do_not_write_to_store(monkeypatch, tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    clock = [100.0]
    monkeypatch.setattr("smallsat_dl.cache.time.time", lambda: clock[0])
    cache = MetadataCache(path, ttl=1)
    cache.put(URL, TEXT)
    before = path.stat()
    contents = path.read_bytes()
    clock[0] = 102.0

    assert cache.get(URL) is None
    assert cache.get("https://example.test/unknown") is None
    after = path.stat()
    assert before.st_mtime_ns == after.st_mtime_ns
    assert path.read_bytes() == contents
    assert list(tmp_path.iterdir()) == [path]


def test_invalidate_and_clear_persist_without_removing_other_records(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    cache = MetadataCache(path)
    cache.put(URL, TEXT)
    other = "https://example.test/other"
    cache.put(other, "other metadata")

    cache.invalidate(URL)
    reopened = MetadataCache(path)
    assert reopened.get(URL) is None
    assert reopened.get(other) == "other metadata"
    reopened.clear()
    assert MetadataCache(path).get(other) is None


def test_corrupt_database_is_a_reported_miss_and_does_not_break_writes(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    path.write_bytes(b"not a sqlite database")
    reports = []
    cache = MetadataCache(path, log=reports.append)

    assert cache.get(URL) is None
    assert cache.get(URL) is None
    cache.put(URL, TEXT)
    cache.invalidate(URL)
    cache.clear()

    assert reports
    assert all("Metadata cache unavailable" in report for report in reports)
    assert len(reports) == 1
    assert path.read_bytes() == b"not a sqlite database"


def test_invalid_record_is_a_reported_miss(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    cache = MetadataCache(path)
    cache.put(URL, TEXT)
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE metadata SET stored_at = ?", ("bad timestamp",))
    reports = []
    reopened = MetadataCache(path, log=reports.append)

    assert reopened.get(URL) is None
    assert reports and "invalid metadata cache entry" in reports[0]


def test_parallel_cache_instances_commit_complete_records(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    # Initialize once, then exercise separate connections as independent runs do.
    MetadataCache(path).put(URL, TEXT)

    def save(index):
        cache = MetadataCache(path)
        text = f"record {index} " * 100
        cache.put(f"{URL}/{index}", text)
        cache.close()
        return text

    with ThreadPoolExecutor(max_workers=4) as workers:
        expected = list(workers.map(save, range(12)))

    reopened = MetadataCache(path)
    assert [reopened.get(f"{URL}/{index}") for index in range(12)] == expected
    assert reopened.get(URL) == TEXT


def test_bytes_are_not_accepted_as_metadata(tmp_path: Path):
    path = tmp_path / "metadata.sqlite3"
    with pytest.raises(TypeError, match="text only"):
        MetadataCache(path).put(URL, b"%PDF-1.7\n%%EOF")
    assert not path.exists()


@pytest.mark.parametrize("ttl", [-1, float("nan"), float("inf")])
def test_invalid_ttl_is_rejected(tmp_path: Path, ttl):
    with pytest.raises(ValueError, match="TTL"):
        MetadataCache(tmp_path / "metadata.sqlite3", ttl=ttl)
