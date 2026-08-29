"""Canonical output schemas.

Every record the pipeline emits is one of these models. They are the contract
between the LLM extraction layer (Phase III) and the output sinks (Google Sheets
/ JSONL). The LLM is asked to fill `content`; the pipeline fills `source`,
`collectedAt`, `schemaVersion`, and `recordType`.

WARNING per the brief: hallucinated data => disqualification. Every record MUST
carry a real `source.url` it can be traced back to, so `source.url` is required
and validated as an http(s) URL.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator

SCHEMA_VERSION = "1.0"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Source(BaseModel):
    name: str = Field(..., description="Name of the source site, e.g. 'arXiv'")
    url: HttpUrl = Field(..., description="Original source URL the record traces back to")


class RecordBase(BaseModel):
    schemaVersion: str = Field(default=SCHEMA_VERSION)
    source: Source
    collectedAt: datetime = Field(default_factory=_utcnow, description="ISO-8601")

    @field_validator("collectedAt")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        return v if v.tzinfo else v.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Startup
# --------------------------------------------------------------------------- #
class StartupData(BaseModel):
    employeeCount: int | None = Field(default=None, description="If available")


class StartupContent(BaseModel):
    entityName: str = Field(..., description="Canonical startup name (post entity-resolution)")
    data: StartupData = Field(default_factory=StartupData)


class StartupRecord(RecordBase):
    recordType: Literal["STARTUP"] = "STARTUP"
    content: StartupContent


# --------------------------------------------------------------------------- #
# Product
# --------------------------------------------------------------------------- #
class PricingModel(str, Enum):
    FREE = "FREE"
    FREEMIUM = "FREEMIUM"
    PAID = "PAID"
    ENTERPRISE = "ENTERPRISE"


class ProductContent(BaseModel):
    startupName: str = Field(..., description="Canonical name of the owning startup")
    pricingModel: PricingModel | None = None


class ProductRecord(RecordBase):
    recordType: Literal["PRODUCT"] = "PRODUCT"
    content: ProductContent


# --------------------------------------------------------------------------- #
# Research paper
# --------------------------------------------------------------------------- #
class ResearchPaperContent(BaseModel):
    title: str
    authors: list[str] = Field(default_factory=list)
    paper_url: HttpUrl = Field(..., description="Link to the arXiv / PDF page")
    github_url: HttpUrl | None = Field(default=None, description="Associated code repo, if any")
    github_stars: int | None = Field(default=None, description="Current star count")
    published_date: datetime | None = Field(default=None, description="ISO-8601")


class ResearchPaperRecord(RecordBase):
    recordType: Literal["RESEARCH_PAPER"] = "RESEARCH_PAPER"
    content: ResearchPaperContent


# --------------------------------------------------------------------------- #
# Job
# --------------------------------------------------------------------------- #
class JobContent(BaseModel):
    company: str = Field(..., description="Canonical company name")
    date: datetime | None = Field(default=None, description="ISO-8601 publication date")
    is_remote: bool | None = None
    role_family: str | None = Field(default=None, description="e.g. 'Engineering'")
    title: str | None = None
    url: HttpUrl | None = None


class JobRecord(RecordBase):
    recordType: Literal["JOB"] = "JOB"
    content: JobContent


# --------------------------------------------------------------------------- #
# News (Phase II) — schema not specified in the brief; kept minimal + traceable
# --------------------------------------------------------------------------- #
class NewsContent(BaseModel):
    title: str
    published_date: datetime | None = None
    author: str | None = None
    full_text: str = Field(..., description="Extracted full-text article body")
    url: HttpUrl


class NewsRecord(RecordBase):
    recordType: Literal["NEWS"] = "NEWS"
    content: NewsContent


AnyRecord = (
    StartupRecord
    | ProductRecord
    | ResearchPaperRecord
    | JobRecord
    | NewsRecord
)

RECORD_BY_TYPE: dict[str, type[RecordBase]] = {
    "STARTUP": StartupRecord,
    "PRODUCT": ProductRecord,
    "RESEARCH_PAPER": ResearchPaperRecord,
    "JOB": JobRecord,
    "NEWS": NewsRecord,
}
