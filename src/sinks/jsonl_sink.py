"""Local JSONL sink — the durable record of every run.

One file per record type, append-only, newline-delimited JSON. This is the
source of truth the Sheets sink is projected from, and what you'd diff between
runs. Written even when Sheets is disabled or unreachable.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from src.config import get_pipeline_config
from src.logging_setup import get_logger

log = get_logger("sink")


class JsonlSink:
    def __init__(self, out_dir: str | None = None) -> None:
        self._dir = Path(out_dir or get_pipeline_config()["output"]["jsonl_dir"])
        self._dir.mkdir(parents=True, exist_ok=True)
        self._counts: dict[str, int] = {}

    def write(self, record_type: str, rows: list[BaseModel]) -> Path:
        path = self._dir / f"{record_type.lower()}.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(row.model_dump_json())
                fh.write("\n")
        self._counts[record_type] = self._counts.get(record_type, 0) + len(rows)
        log.info("jsonl_written", record_type=record_type, rows=len(rows), path=str(path))
        return path

    def write_dicts(self, name: str, rows: list[dict]) -> Path:
        path = self._dir / f"{name}.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, default=str))
                fh.write("\n")
        log.info("jsonl_written", name=name, rows=len(rows), path=str(path))
        return path

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)
