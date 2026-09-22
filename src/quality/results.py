"""Result objects produced by the quality engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ExpectationResult:
    """Outcome of evaluating a single expectation."""

    name: str
    type: str
    success: bool
    severity: str  # error | warning
    columns: list[str] = field(default_factory=list)
    observed: dict[str, Any] = field(default_factory=dict)
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "success": self.success,
            "severity": self.severity,
            "columns": self.columns,
            "observed": self.observed,
            "message": self.message,
        }


@dataclass
class ValidationReport:
    """Aggregate report over all expectations for a dataset."""

    dataset: str
    rule_set: str
    row_count: int
    results: list[ExpectationResult] = field(default_factory=list)
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return self.total - self.passed

    @property
    def error_failures(self) -> list[ExpectationResult]:
        """Failed expectations with ``error`` severity (these block loads)."""
        return [r for r in self.results if not r.success and r.severity == "error"]

    @property
    def warning_failures(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.success and r.severity == "warning"]

    @property
    def success(self) -> bool:
        """The dataset passes if there are no error-severity failures."""
        return len(self.error_failures) == 0

    @property
    def score(self) -> float:
        """Fraction of expectations that passed (0–1)."""
        return round(self.passed / self.total, 4) if self.total else 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "rule_set": self.rule_set,
            "row_count": self.row_count,
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "score": self.score,
            "success": self.success,
            "evaluated_at": self.evaluated_at.isoformat(),
            "results": [r.to_dict() for r in self.results],
        }
