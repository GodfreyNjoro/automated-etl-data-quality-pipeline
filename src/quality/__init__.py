"""Data quality validation framework.

A lightweight, dependency-free expectation engine inspired by Great
Expectations. It validates a pandas DataFrame against a declarative set of
expectations (schema, completeness, uniqueness, range, allowed values,
freshness and custom business rules) and produces a structured, storable
report with per-expectation pass/fail status and metrics.
"""

from __future__ import annotations

from src.quality.results import ExpectationResult, ValidationReport
from src.quality.engine import QualityEngine, register_expectation

__all__ = [
    "ExpectationResult",
    "ValidationReport",
    "QualityEngine",
    "register_expectation",
]
