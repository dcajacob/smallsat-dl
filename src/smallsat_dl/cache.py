"""Persistent cache for metadata text already accepted by its caller's validator."""

from __future__ import annotations

import math
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path


DEFAULT_CACHE_TTL = 24 * 60 * 60


class MetadataCache:
    """Cache accepted metadata using atomic SQLite transactions.

    The caller must validate HTML/XML before calling ``put``. Downloads, HTTP
    failures, and unvalidated responses do not belong in this store. Connections
    are opened only for individual operations; reads never create the database.
    Cache errors are reported once per distinct error and behave as cache misses.
    """

    def __init__(
        self,
        path: Path,
        ttl: float = DEFAULT_CACHE_TTL,
        log: Callable[[str], None] = print,
    ) -> None:
        if not math.isfinite(ttl) or ttl < 0:
            raise ValueError("metadata cache TTL must be a finite number zero or greater")
        self.path = Path(path).expanduser().resolve()
        self.ttl = ttl
        self.log = log
        self._reported_errors: set[str] = set()
        self._closed = False

    def get(self, url: str) -> str | None:
        if self._closed:
            return None
        try:
            if not self.path.is_file():
                return None
            with closing(self._connect("ro")) as connection:
                row = connection.execute(
                    "SELECT text, stored_at FROM metadata WHERE url = ?", (url,)
                ).fetchone()
            if row is None:
                return None
            text, stored_at = row
            if not isinstance(text, str) or not isinstance(stored_at, (int, float)):
                self._report("invalid metadata cache entry")
                return None
            age = time.time() - stored_at
            if not 0 <= age < self.ttl:
                return None
            return text
        except (OSError, sqlite3.Error) as exc:
            self._report(str(exc))
            return None

    def put(self, url: str, text: str) -> None:
        if not isinstance(text, str):
            raise TypeError("metadata cache accepts text only")
        if self._closed:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with closing(self._connect("rwc")) as connection, connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS metadata ("
                    "url TEXT PRIMARY KEY, text TEXT NOT NULL, stored_at REAL NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO metadata (url, text, stored_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(url) DO UPDATE SET text = excluded.text, "
                    "stored_at = excluded.stored_at",
                    (url, text, time.time()),
                )
        except (OSError, sqlite3.Error) as exc:
            self._report(str(exc))

    def invalidate(self, url: str) -> None:
        self._delete("DELETE FROM metadata WHERE url = ?", (url,))

    def clear(self) -> None:
        self._delete("DELETE FROM metadata", ())

    def close(self) -> None:
        """Stop using the cache; operations hold no persistent connections."""
        self._closed = True

    def _connect(self, mode: str) -> sqlite3.Connection:
        # A short busy timeout lets another downloader finish its tiny metadata
        # transaction without holding up this run for a long database lock.
        return sqlite3.connect(f"{self.path.as_uri()}?mode={mode}", uri=True, timeout=0.1)

    def _delete(self, statement: str, parameters: tuple[str, ...]) -> None:
        if self._closed:
            return
        try:
            if not self.path.is_file():
                return
            with closing(self._connect("rw")) as connection, connection:
                connection.execute(statement, parameters)
        except (OSError, sqlite3.Error) as exc:
            self._report(str(exc))

    def _report(self, message: str) -> None:
        if message not in self._reported_errors:
            self._reported_errors.add(message)
            self.log(f"  Metadata cache unavailable at {self.path}: {message}")
