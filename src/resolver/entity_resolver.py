"""Canonicalize messy org / product strings to a single canonical form.

"OpenAI", "OpenAI, Inc.", "Open AI", "openai"  ->  "OpenAI"

Deterministic ladder (no LLM, fully reproducible):
  1. normalize: lowercase, strip legal suffixes (Inc/LLC/Ltd/Corp/GmbH...),
     drop punctuation, collapse whitespace, de-space known squished names.
  2. exact hit on the normalized seed index -> canonical.
  3. known alias table hit -> canonical.
  4. fuzzy match (rapidfuzz token_set_ratio) against seed index; accept if
     score >= threshold, else the entity is "new": we mint a canonical form
     (Title Case of the cleaned string) and add it to the in-memory index so
     later variants in the same run converge on it.

Every decision is written to the mapping log (raw -> canonical, method, score)
which becomes the "Entity Mapping Log" deliverable tab.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from rapidfuzz import fuzz, process

from src.logging_setup import get_logger

log = get_logger("resolve")

_LEGAL = re.compile(
    r"\b(inc|incorporated|llc|l\.l\.c|ltd|limited|corp|corporation|co|company|"
    r"gmbh|s\.a|sa|plc|ag|pty|pvt|private|technologies|technology|labs|lab|ai)\b\.?",
    re.I,
)
_PUNCT = re.compile(r"[^\w\s&+-]")
_WS = re.compile(r"\s+")


def normalize(name: str) -> str:
    s = name.strip().lower()
    s = _PUNCT.sub(" ", s)
    s = _LEGAL.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def _despace(s: str) -> str:
    return s.replace(" ", "")


@dataclass
class Resolution:
    raw: str
    canonical: str
    method: str  # exact | alias | fuzzy | new
    score: float
    is_new_entity: bool


@dataclass
class EntityResolver:
    threshold: float = 88.0
    _canon_by_norm: dict[str, str] = field(default_factory=dict)
    _canon_by_despace: dict[str, str] = field(default_factory=dict)
    _aliases: dict[str, str] = field(default_factory=dict)
    _canonicals: set[str] = field(default_factory=set)
    mapping_log: list[Resolution] = field(default_factory=list)

    @classmethod
    def from_seed_file(cls, path: str | Path, *, threshold: float = 88.0) -> EntityResolver:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        r = cls(threshold=threshold)
        for entry in data.get("entities", []):
            canonical = entry["canonical"]
            r._register(canonical)
            for alias in entry.get("aliases", []):
                r._aliases[normalize(alias)] = canonical
        log.info("resolver_loaded", canonicals=len(r._canonicals), aliases=len(r._aliases))
        return r

    def _register(self, canonical: str) -> None:
        self._canonicals.add(canonical)
        n = normalize(canonical)
        self._canon_by_norm[n] = canonical
        self._canon_by_despace[_despace(n)] = canonical

    def resolve(self, raw: str) -> Resolution:
        if not raw or not raw.strip():
            res = Resolution(raw, "", "new", 0.0, True)
            self.mapping_log.append(res)
            return res

        n = normalize(raw)

        if n in self._canon_by_norm:
            res = Resolution(raw, self._canon_by_norm[n], "exact", 100.0, False)
        elif n in self._aliases:
            res = Resolution(raw, self._aliases[n], "alias", 100.0, False)
        elif _despace(n) in self._canon_by_despace:
            res = Resolution(raw, self._canon_by_despace[_despace(n)], "exact", 99.0, False)
        else:
            match = process.extractOne(
                n, self._canon_by_norm.keys(), scorer=fuzz.token_set_ratio
            )
            if match and match[1] >= self.threshold:
                res = Resolution(raw, self._canon_by_norm[match[0]], "fuzzy", float(match[1]), False)
            else:
                canonical = _title_case(n)
                self._register(canonical)
                res = Resolution(raw, canonical, "new", float(match[1]) if match else 0.0, True)

        self.mapping_log.append(res)
        return res

    def resolve_str(self, raw: str) -> str:
        return self.resolve(raw).canonical


def _title_case(norm_name: str) -> str:
    return " ".join(w if w.isupper() else w.capitalize() for w in norm_name.split())
