"""Retry helper with exponential backoff and jitter."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

from src.utils.logging_config import get_logger

T = TypeVar("T")
_log = get_logger("orchestration.retry")


def retry_with_backoff(
    func: Callable[[], T],
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
    max_backoff: float = 60.0,
    jitter: bool = True,
    description: str = "operation",
) -> T:
    """Execute ``func`` retrying on exception with exponential backoff.

    The delay after attempt ``n`` is ``backoff_seconds * 2**(n-1)`` capped at
    ``max_backoff``, optionally with random jitter to avoid thundering herds.
    The final exception is re-raised once retries are exhausted.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - retry any failure
            if attempt > max_retries:
                _log.error(
                    "Operation failed after retries",
                    extra={"description": description, "attempts": attempt,
                           "error": str(exc)},
                )
                raise
            delay = min(backoff_seconds * (2 ** (attempt - 1)), max_backoff)
            if jitter:
                delay += random.uniform(0, delay * 0.25)
            _log.warning(
                "Operation failed; retrying",
                extra={"description": description, "attempt": attempt,
                       "next_delay_seconds": round(delay, 2), "error": str(exc)},
            )
            time.sleep(delay)
