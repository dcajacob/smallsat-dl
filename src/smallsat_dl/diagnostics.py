"""Structured JSON Lines diagnostics for live downloader investigations."""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any


class JsonlDiagnostics:
    """Write timestamped diagnostic events and flush each line immediately."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._started = time.monotonic()
        self._file = path.open("w", encoding="utf-8")

    def record(self, event: str, **fields: Any) -> None:
        payload = {
            "timestamp": dt.datetime.now(dt.UTC).isoformat(),
            "elapsed_seconds": round(time.monotonic() - self._started, 6),
            "event": event,
            **fields,
        }
        self._file.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
        self._file.flush()

    def close(self) -> None:
        self._file.close()
