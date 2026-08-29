"""Structured logging. JSON in, human-readable console out.

Every log line carries a `stage` key so a run can be filtered per phase
(crawl / extract / resolve / sink).
"""

from __future__ import annotations

import logging
import sys

import structlog

from src.config import get_settings


def configure_logging() -> None:
    level = getattr(logging, get_settings().log_level.upper(), logging.INFO)

    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty()),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


def get_logger(stage: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger().bind(stage=stage)
