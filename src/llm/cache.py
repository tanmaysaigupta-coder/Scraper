"""On-disk extraction cache.

Every successful LLM extraction is stored keyed by a hash of (target schema +
instructions + source text). A re-run — or a resumed run after a crash / rate-limit
storm — reuses cached results instead of paying for the call again. This is what
makes a 1,000+ record run over free-tier provider quotas actually finish: partial
progress is never lost.

Keyed on content, not provider, so switching the model chain still hits the cache
(the extracted facts don't depend on which model produced them).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path

from src.config import get_pipeline_config


def _key(target_name: str, instructions: str, raw_text: str) -> str:
    h = hashlib.sha256()
    h.update(target_name.encode())
    h.update(b"\x00")
    h.update(instructions.encode())
    h.update(b"\x00")
    h.update(raw_text.encode())
    return h.hexdigest()


class ExtractionCache:
    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            cache_dir = Path(get_pipeline_config()["seen_store"]["sqlite_path"]).parent
            path = cache_dir / "extraction_cache.sqlite"
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self._path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS extraction ("
            "  k TEXT PRIMARY KEY, payload TEXT, provider TEXT, ts REAL)"
        )
        self._db.commit()

    def get(self, target_name: str, instructions: str, raw_text: str) -> dict | None:
        cur = self._db.execute(
            "SELECT payload FROM extraction WHERE k = ?",
            (_key(target_name, instructions, raw_text),),
        )
        row = cur.fetchone()
        return json.loads(row[0]) if row else None

    def put(self, target_name: str, instructions: str, raw_text: str,
            payload: dict, provider: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO extraction (k, payload, provider, ts) VALUES (?, ?, ?, ?)",
            (_key(target_name, instructions, raw_text), json.dumps(payload), provider, time.time()),
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()
