"""Structured JSON logging configuration.

Emitting logs as JSON makes them machine-parseable by log aggregators
(Elasticsearch, Datadog, CloudWatch, etc.) while remaining human-readable in
development. Every log record carries a timestamp, level, logger name and an
optional bag of contextual fields supplied via the ``extra`` argument.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Merge any structured context passed via logger.info(..., extra={...}).
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    """Configure the root logger. Idempotent across repeated calls."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    if json_output:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
    root.addHandler(handler)


def get_logger(name: str) -> logging.LoggerAdapter:
    """Return a logger bound to ``name`` with structured-context support."""
    return logging.LoggerAdapter(logging.getLogger(name), extra={})
