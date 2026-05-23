"""Central structlog configuration.

Console (pretty) for local dev; JSON for production (set AIA_LOG_JSON=1, e.g.
under Dagster). Call configure_logging() once at process start; modules then use
structlog.get_logger(__name__). Agent runs bind a run_id (UUID4) for correlation.
"""

from __future__ import annotations

import logging
import os
import uuid

import structlog


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def configure_logging(json: bool | None = None, level: str = "INFO") -> None:
    """Idempotent structlog + stdlib logging setup."""
    use_json = json if json is not None else os.environ.get("AIA_LOG_JSON") == "1"

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )

    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
