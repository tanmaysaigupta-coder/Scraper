"""Assemble the 6-tab .xlsx deliverable from whatever JSONL exists in
data/output/ — decoupled from a full pipeline run so a crash in one vertical
never costs the others.

    python scripts/build_workbook.py

Reads:  data/output/{startup,product,research_paper,job,news}.jsonl
        data/output/entity_mapping_log.jsonl
Writes: data/output/graphone_intelligence.xlsx
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline import (  # noqa: E402
    JOB_COLUMNS,
    MAPPING_COLUMNS,
    NEWS_COLUMNS,
    PAPER_COLUMNS,
    PRODUCT_COLUMNS,
    STARTUP_COLUMNS,
)
from src.sinks.sheets_sink import _flatten  # noqa: E402
from src.sinks.xlsx_sink import XlsxSink, _cell  # noqa: E402

OUT = ROOT / "data" / "output"

SPEC = [
    ("startups", "startup.jsonl", STARTUP_COLUMNS),
    ("products", "product.jsonl", PRODUCT_COLUMNS),
    ("research_papers", "research_paper.jsonl", PAPER_COLUMNS),
    ("jobs", "job.jsonl", JOB_COLUMNS),
    ("news", "news.jsonl", NEWS_COLUMNS),
]


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def main() -> None:
    xlsx = XlsxSink()
    total = 0
    for tab_key, fname, columns in SPEC:
        records = _rows(OUT / fname)
        flat = [[_flatten(r).get(c) for c in columns] for r in records]
        xlsx._sheet(tab_key, columns, [[_cell(v) for v in row] for row in flat])
        print(f"  {tab_key:16} {len(records):>5} rows")
        total += len(records)

    mapping = _rows(OUT / "entity_mapping_log.jsonl")
    if mapping:
        xlsx._sheet("mapping_log", MAPPING_COLUMNS,
                    [[_cell(r.get(c)) for c in MAPPING_COLUMNS] for r in mapping])
        print(f"  {'mapping_log':16} {len(mapping):>5} rows")

    path = xlsx.save()
    print(f"\n{total} data rows -> {path}")


if __name__ == "__main__":
    main()
