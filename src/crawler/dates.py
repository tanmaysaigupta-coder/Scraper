"""Date normalization + freshness (Phase II).

Two problems the brief calls out:

1. Publication dates come in every shape: ISO strings, "2 hours ago",
   "Yesterday", "Mar 3", localized month names, or nothing at all. `parse_date`
   walks a ladder of strategies and always returns tz-aware UTC or None.

2. "Is this new since the last run?" When a source has no usable date we fall
   back to a heuristic: compare against the high-water mark recorded for that
   source in `FreshnessState` (persisted to disk), plus the seen-store for hard
   dedupe.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dateparser
from dateutil import parser as du_parser

_REL_RE = re.compile(
    r"\b(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago\b", re.I)
_UNIT_SECONDS = {
    "second": 1, "minute": 60, "hour": 3600, "day": 86400,
    "week": 604800, "month": 2629800, "year": 31557600,
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


def parse_date(value: object | None, *, now: datetime | None = None) -> datetime | None:
    if value is None or value == "":
        return None
    now = now or _utcnow()

    # numeric epoch (seconds or milliseconds) — some job APIs return ints
    if isinstance(value, int | float) or (isinstance(value, str) and value.strip().isdigit()):
        ts = float(value)
        if ts > 1e11:  # milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()

    m = _REL_RE.search(text)
    if m:
        secs = int(m.group(1)) * _UNIT_SECONDS[m.group(2).lower()]
        return now - timedelta(seconds=secs)

    low = text.lower()
    if low in ("just now", "moments ago", "now"):
        return now
    if low.startswith("yesterday"):
        return now - timedelta(days=1)
    if low.startswith("today"):
        return now

    try:
        dt = du_parser.parse(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except (ValueError, OverflowError):
        pass

    dt = dateparser.parse(
        text,
        settings={"RETURN_AS_TIMEZONE_AWARE": True, "RELATIVE_BASE": now.replace(tzinfo=None),
                  "TO_TIMEZONE": "UTC", "TIMEZONE": "UTC"},
    )
    if dt:
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return None


def within_window(dt: datetime | None, *, hours: int, now: datetime | None = None) -> bool:
    if dt is None:
        return False
    now = now or _utcnow()
    return (now - dt) <= timedelta(hours=hours) and dt <= now + timedelta(minutes=5)


class FreshnessState:
    """Per-source high-water mark of the newest item seen on a prior run."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, str] = {}
        if self._path.exists():
            self._data = json.loads(self._path.read_text() or "{}")

    def high_water(self, source: str) -> datetime | None:
        raw = self._data.get(source)
        return du_parser.isoparse(raw) if raw else None

    def is_new(self, source: str, dt: datetime | None) -> bool:
        """Heuristic used when a hard 24h date check is not possible."""
        hw = self.high_water(source)
        if dt is None:
            # no date anywhere: treat as new only if we've never run this source
            return hw is None
        return hw is None or dt > hw

    def observe(self, source: str, dt: datetime | None) -> None:
        if dt is None:
            return
        hw = self.high_water(source)
        if hw is None or dt > hw:
            self._data[source] = dt.astimezone(UTC).isoformat()

    def save(self) -> None:
        self._path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
