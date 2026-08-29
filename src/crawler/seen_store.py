"""Deduplication store — "have we processed this URL/item before?"

Interface first, backend second. Local runs use SQLite. The architecture doc
(Phase VI) specifies Redis (SET of URL hashes, or a Bloom filter for very large
keyspaces) shared by all distributed crawler nodes so the same article/job is
never processed twice. Swapping backends is a config change
(`seen_store.backend`), not a code change.

Key = sha1 of the normalized URL (scheme+host+path+sorted query, fragment and
common tracking params stripped).
"""

from __future__ import annotations

import abc
import hashlib
import sqlite3
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.config import get_pipeline_config

_TRACKING = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
             "ref", "ref_src", "fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_url(url: str) -> str:
    s = urlsplit(url.strip())
    query = [(k, v) for k, v in parse_qsl(s.query, keep_blank_values=False)
             if k.lower() not in _TRACKING]
    query.sort()
    path = s.path.rstrip("/") or "/"
    return urlunsplit((s.scheme.lower(), s.netloc.lower(), path, urlencode(query), ""))


def url_key(url: str) -> str:
    return hashlib.sha1(normalize_url(url).encode()).hexdigest()


class SeenStore(abc.ABC):
    @abc.abstractmethod
    def seen(self, url: str) -> bool: ...

    @abc.abstractmethod
    def add(self, url: str, *, source: str = "") -> None: ...

    def add_if_new(self, url: str, *, source: str = "") -> bool:
        """Return True if this call was the first to see `url`."""
        if self.seen(url):
            return False
        self.add(url, source=source)
        return True

    def close(self) -> None:  # noqa: B027 - optional lifecycle hook, not all backends need it
        pass


class SqliteSeenStore(SeenStore):
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self._path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS seen ("
            "  k TEXT PRIMARY KEY, url TEXT, source TEXT, ts REAL)"
        )
        self._db.commit()

    def seen(self, url: str) -> bool:
        cur = self._db.execute("SELECT 1 FROM seen WHERE k = ?", (url_key(url),))
        return cur.fetchone() is not None

    def add(self, url: str, *, source: str = "") -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO seen (k, url, source, ts) VALUES (?, ?, ?, ?)",
            (url_key(url), normalize_url(url), source, time.time()),
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()


class RedisSeenStore(SeenStore):  # pragma: no cover - infra path, documented not run
    """Scale backend. Requires `redis` and a running server (see Phase VI doc)."""

    def __init__(self, url: str, namespace: str = "seen") -> None:
        import redis  # lazy

        self._r = redis.Redis.from_url(url)
        self._ns = namespace

    def seen(self, url: str) -> bool:
        return bool(self._r.sismember(self._ns, url_key(url)))

    def add(self, url: str, *, source: str = "") -> None:
        self._r.sadd(self._ns, url_key(url))


def build_seen_store() -> SeenStore:
    cfg = get_pipeline_config()["seen_store"]
    backend = cfg.get("backend", "sqlite")
    if backend == "sqlite":
        return SqliteSeenStore(cfg["sqlite_path"])
    if backend == "redis":
        return RedisSeenStore(cfg["redis_url"])
    raise ValueError(f"unknown seen_store backend: {backend}")
