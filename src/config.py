"""Configuration loading.

Two layers:
  * `Settings`  -> secrets + runtime knobs, from environment / .env
  * YAML files  -> declarative source lists and tuning (config/*.yaml)

Nothing about scaling the pipeline should require a code change; it should only
require editing config or adding infrastructure. Concurrency, the LLM model
chain, retry budgets, and every source URL live in config, not in code.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # LLM providers
    gemini_api_key: str | None = None
    groq_api_key: str | None = None
    openrouter_api_key: str | None = None
    deepseek_api_key: str | None = None

    # GitHub
    github_token: str | None = None

    # Product Hunt (products vertical)
    producthunt_token: str | None = None

    # Google Sheets sink
    google_service_account_json: str = "./config/gcp-service-account.json"
    gsheet_id: str | None = None

    # Anti-bot
    proxy_url: str | None = None
    scraper_api_key: str | None = None

    # Runtime
    log_level: str = "INFO"


@functools.lru_cache
def get_settings() -> Settings:
    return Settings()


@functools.lru_cache
def load_yaml(name: str) -> dict[str, Any]:
    path = CONFIG_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"missing config file: {path}")
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def get_pipeline_config() -> dict[str, Any]:
    return load_yaml("settings.yaml")


def get_sources_config() -> dict[str, Any]:
    return load_yaml("sources.yaml")
