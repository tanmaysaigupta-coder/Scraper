"""Best-effort full-text extraction from an article HTML page (Phase II).

Not a full readability port — a pragmatic heuristic:
  1. strip script/style/nav/aside/footer/form
  2. prefer <article>, then the <div>/<main> with the most <p> text
  3. join paragraph text, collapse whitespace

Good enough to feed the LLM layer; the LLM cleans up the rest.
"""

from __future__ import annotations

from selectolax.parser import HTMLParser

_STRIP = ["script", "style", "noscript", "nav", "aside", "footer", "form", "header", "svg"]


def extract_main_text(html: str) -> str:
    tree = HTMLParser(html)
    for sel in _STRIP:
        for node in tree.css(sel):
            node.decompose()

    article = tree.css_first("article")
    if article:
        text = _paragraphs(article)
        if len(text) > 400:
            return text

    best = ""
    for container in tree.css("main, div, section"):
        text = _paragraphs(container)
        if len(text) > len(best):
            best = text
    if len(best) > 200:
        return best

    body = tree.css_first("body")
    return _paragraphs(body) if body else ""


def _paragraphs(node) -> str:
    parts = [p.text(strip=True) for p in node.css("p")]
    parts = [p for p in parts if len(p) > 30]
    return "\n\n".join(parts).strip()
