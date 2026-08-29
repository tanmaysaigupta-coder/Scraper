"""Intelligent chunking / truncation so payloads never trip 413.

Strategy
--------
1. Cheap length guess up front: `len(text) / chars_per_token`. No network, no
   tokenizer dependency — deliberately conservative.
2. Budget per tier = `max_input_tokens * safety_ratio` minus a fixed reserve for
   the prompt scaffolding and the model's response.
3. If the doc fits: one chunk, untouched.
4. If not: split on structural boundaries (headings, blank lines, then
   sentences) — never mid-word — into overlapping windows so an entity spanning
   a boundary still appears whole in one chunk.
5. On a live PayloadTooLarge from the provider, the orchestrator calls
   `shrink()` to halve the budget and re-chunk — a runtime feedback loop, not a
   guess frozen at config time.

For extraction we generally want the *most information-dense* single chunk
rather than a map-reduce over all of them, so `pick_primary_chunk` scores
chunks by density of schema-relevant signals.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_HEADING_RE = re.compile(r"\n(?=#{1,6}\s|\s*[A-Z][A-Za-z ]{0,60}\n[-=]{3,}\n)")
_PARA_RE = re.compile(r"\n\s*\n")
_SENT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


@dataclass(frozen=True)
class ChunkPlan:
    chars_per_token: int
    safety_ratio: float
    overlap_chars: int
    prompt_reserve_tokens: int = 1500
    response_reserve_tokens: int = 2000

    def budget_chars(self, max_input_tokens: int) -> int:
        usable = int(max_input_tokens * self.safety_ratio)
        usable -= self.prompt_reserve_tokens + self.response_reserve_tokens
        return max(2000, usable * self.chars_per_token)

    def shrink(self) -> ChunkPlan:
        """Called after a live 413: be twice as conservative."""
        return ChunkPlan(
            chars_per_token=self.chars_per_token,
            safety_ratio=self.safety_ratio * 0.5,
            overlap_chars=self.overlap_chars,
            prompt_reserve_tokens=self.prompt_reserve_tokens,
            response_reserve_tokens=self.response_reserve_tokens,
        )


def estimate_tokens(text: str, chars_per_token: int = 4) -> int:
    return len(text) // max(1, chars_per_token)


def _split_recursive(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    for splitter in (_HEADING_RE, _PARA_RE, _SENT_RE):
        parts = splitter.split(text)
        if len(parts) > 1:
            out: list[str] = []
            buf = ""
            for part in parts:
                candidate = f"{buf}\n\n{part}" if buf else part
                if len(candidate) <= limit:
                    buf = candidate
                else:
                    if buf:
                        out.append(buf)
                    buf = part if len(part) <= limit else ""
                    if not buf:
                        out.extend(_hard_wrap(part, limit))
            if buf:
                out.append(buf)
            return out
    return _hard_wrap(text, limit)


def _hard_wrap(text: str, limit: int) -> list[str]:
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def chunk_document(text: str, max_input_tokens: int, plan: ChunkPlan) -> list[str]:
    text = text.strip()
    limit = plan.budget_chars(max_input_tokens)
    if len(text) <= limit:
        return [text]

    base = _split_recursive(text, limit)
    if plan.overlap_chars <= 0 or len(base) < 2:
        return base

    overlapped: list[str] = []
    for i, chunk in enumerate(base):
        prefix = base[i - 1][-plan.overlap_chars :] if i > 0 else ""
        overlapped.append((prefix + "\n" + chunk).strip() if prefix else chunk)
    return overlapped


_SIGNAL_RE = re.compile(
    r"\b(pricing|free|freemium|paid|enterprise|employees?|founded|headquarters|"
    r"github|stars?|arxiv|authors?|published|remote|salary|apply|hiring)\b",
    re.I,
)


def pick_primary_chunk(chunks: list[str]) -> str:
    if len(chunks) == 1:
        return chunks[0]
    return max(chunks, key=lambda c: len(_SIGNAL_RE.findall(c)) / max(1, len(c) / 1000))
