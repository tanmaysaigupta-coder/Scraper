"""Output sinks: local JSONL (always) + Google Sheets (deliverable)."""

from src.sinks.jsonl_sink import JsonlSink
from src.sinks.sheets_sink import SheetsSink

__all__ = ["JsonlSink", "SheetsSink"]
