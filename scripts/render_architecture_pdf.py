"""Render docs/architecture.md -> architecture.pdf (repo root).

Kept as a script so the PDF can be regenerated after edits:
    python scripts/render_architecture_pdf.py
"""

from __future__ import annotations

from pathlib import Path

import markdown
from xhtml2pdf import pisa

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs" / "architecture.md"
OUT = ROOT / "architecture.pdf"

CSS = """
@page { size: A4; margin: 1.3cm 1.6cm; }
body { font-family: "Helvetica", "Arial", sans-serif; font-size: 9.5pt;
       line-height: 1.32; color: #1a1a1a; }
h1 { font-size: 16pt; margin: 0 0 3pt; color: #10233f; }
h2 { font-size: 12pt; margin: 11pt 0 3pt; color: #10233f;
     border-bottom: 1px solid #d0d7e2; padding-bottom: 2pt; }
h3 { font-size: 10.5pt; margin: 8pt 0 2pt; color: #24466f; }
p, li { margin: 2pt 0; }
code { font-family: "Courier New", monospace; font-size: 8pt;
       background: #f1f3f7; padding: 1pt 2pt; }
pre { background: #f1f3f7; padding: 6pt; font-size: 7.5pt; line-height: 1.25;
      white-space: pre-wrap; }
table { border-collapse: collapse; width: 100%; margin: 5pt 0; font-size: 8.5pt; }
th, td { border: 1px solid #c4ccd8; padding: 3pt 5pt; text-align: left;
         vertical-align: top; }
th { background: #eef1f6; }
em { color: #5a6b82; }
"""


def main() -> None:
    md = SRC.read_text(encoding="utf-8")
    body = markdown.markdown(md, extensions=["tables", "fenced_code", "sane_lists"])
    html = f"<html><head><style>{CSS}</style></head><body>{body}</body></html>"
    with OUT.open("wb") as fh:
        result = pisa.CreatePDF(html, dest=fh)
    if result.err:
        raise SystemExit(f"PDF render failed with {result.err} error(s)")
    print(f"wrote {OUT}  ({OUT.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
