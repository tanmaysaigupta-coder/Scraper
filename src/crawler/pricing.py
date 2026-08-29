"""Deterministic pricing-model classifier.

Scans product page / description text for pricing signals and returns one of
FREE / FREEMIUM / PAID / ENTERPRISE, or None when the text doesn't say. No LLM —
so it works regardless of provider quota, and it's reproducible.

Precedence when multiple signals appear:
  FREEMIUM  (a free tier AND paid tiers)  >  ENTERPRISE (custom / contact-sales)
  >  PAID (explicit price, no free tier)  >  FREE (free / open-source, no paid)
"""

from __future__ import annotations

import re

from src.schemas import PricingModel

_FREE = re.compile(
    r"\b(free forever|100% free|completely free|totally free|always free|"
    r"free to use|open[- ]source|no cost|free plan|free tier|free version)\b", re.I)
_PAID_PRICE = re.compile(
    r"(\$\s?\d[\d,]*(\.\d+)?\s?(/|per\s)?\s?(mo|month|yr|year|user|seat|mo\.)?"
    r"|\b\d+\s?(usd|eur|gbp)\b|\bstarting at\s?\$?\d)", re.I)
_PAID_WORDS = re.compile(
    r"\b(paid plan|pro plan|premium plan|subscription|billed (annually|monthly)|"
    r"upgrade to pro|buy now|purchase|pricing starts)\b", re.I)
_ENTERPRISE = re.compile(
    r"\b(contact (us for|sales)|custom pricing|talk to sales|request a (quote|demo)"
    r"|enterprise plan|enterprise pricing|volume (pricing|discount)|book a demo)\b", re.I)
_TRIAL = re.compile(r"\b(free trial|\d+[- ]day trial|try (it )?free)\b", re.I)


def classify_pricing(*texts: str) -> PricingModel | None:
    blob = "\n".join(t for t in texts if t).lower()
    if not blob:
        return None

    has_free = bool(_FREE.search(blob))
    has_price = bool(_PAID_PRICE.search(blob)) or bool(_PAID_WORDS.search(blob))
    has_enterprise = bool(_ENTERPRISE.search(blob))
    has_trial = bool(_TRIAL.search(blob))

    if has_free and (has_price or has_enterprise):
        return PricingModel.FREEMIUM
    if has_enterprise and not has_price:
        return PricingModel.ENTERPRISE
    if has_price or (has_trial and not has_free):
        return PricingModel.PAID
    if has_free:
        return PricingModel.FREE
    return None
