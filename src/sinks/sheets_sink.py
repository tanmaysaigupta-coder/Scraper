"""Google Sheets sink — the 6-tab deliverable.

Needs a service-account JSON (GOOGLE_SERVICE_ACCOUNT_JSON) and a spreadsheet id
(GSHEET_ID) whose sheet is shared with the service-account email as Editor.

Each tab is fully rewritten per run: flattened rows, a fixed header, batch
`update` (one API call per tab, not one per row — stays well under quota).
Nested schema fields are flattened with dotted keys to match the brief's field
names (`content.entityName`, `source.url`, ...).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from src.config import get_pipeline_config, get_settings
from src.logging_setup import get_logger

log = get_logger("sink")


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(_flatten(v, f"{prefix}{k}."))
    elif isinstance(obj, list):
        out[prefix.rstrip(".")] = ", ".join(str(x) for x in obj)
    else:
        out[prefix.rstrip(".")] = obj
    return out


class SheetsSink:
    def __init__(self) -> None:
        cfg = get_pipeline_config()["output"]["sheets"]
        self.enabled = bool(cfg.get("enabled"))
        self._tab_names = cfg["tabs"]
        self._settings = get_settings()
        self._gc = None
        self._ss = None

    def _connect(self) -> None:
        if self._ss is not None:
            return
        import gspread
        from google.oauth2.service_account import Credentials

        if not self._settings.gsheet_id:
            raise RuntimeError("GSHEET_ID not set")
        creds = Credentials.from_service_account_file(
            self._settings.google_service_account_json,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self._gc = gspread.authorize(creds)
        self._ss = self._gc.open_by_key(self._settings.gsheet_id)
        log.info("sheets_connected", spreadsheet=self._ss.title)

    def _tab(self, key: str):
        title = self._tab_names[key]
        try:
            return self._ss.worksheet(title)
        except Exception:  # noqa: BLE001 - gspread WorksheetNotFound
            return self._ss.add_worksheet(title=title, rows=2000, cols=26)

    def write_records(self, tab_key: str, records: Iterable[BaseModel], columns: list[str]) -> None:
        if not self.enabled:
            log.info("sheets_disabled", tab=tab_key)
            return
        self._connect()
        rows = [columns]
        for rec in records:
            flat = _flatten(rec.model_dump(mode="json"))
            rows.append([_cell(flat.get(c)) for c in columns])
        ws = self._tab(tab_key)
        ws.clear()
        ws.update(rows, value_input_option="RAW")
        log.info("sheets_tab_written", tab=self._tab_names[tab_key], rows=len(rows) - 1)

    def write_rows(self, tab_key: str, rows: list[dict], columns: list[str]) -> None:
        if not self.enabled:
            log.info("sheets_disabled", tab=tab_key)
            return
        self._connect()
        matrix = [columns]
        for r in rows:
            matrix.append([_cell(r.get(c)) for c in columns])
        ws = self._tab(tab_key)
        ws.clear()
        ws.update(matrix, value_input_option="RAW")
        log.info("sheets_tab_written", tab=self._tab_names[tab_key], rows=len(matrix) - 1)


def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return str(v)
