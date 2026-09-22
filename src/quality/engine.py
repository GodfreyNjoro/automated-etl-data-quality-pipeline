"""The quality engine: runs a rule set against a DataFrame."""

from __future__ import annotations

import contextlib
from collections.abc import Callable

import pandas as pd

from src.quality.expectations import BUILTIN_EXPECTATIONS
from src.quality.results import ExpectationResult, ValidationReport
from src.utils.config_loader import ExpectationConfig, QualityRuleSet
from src.utils.logging_config import get_logger

ExpectationFn = Callable[[pd.DataFrame, ExpectationConfig], ExpectationResult]

_REGISTRY: dict[str, ExpectationFn] = dict(BUILTIN_EXPECTATIONS)


def register_expectation(type_name: str, fn: ExpectationFn) -> None:
    """Register a custom expectation implementation under ``type_name``."""
    _REGISTRY[type_name] = fn


class QualityEngine:
    """Evaluate a :class:`QualityRuleSet` against a dataset."""

    def __init__(self, rule_set: QualityRuleSet) -> None:
        self.rule_set = rule_set
        self.log = get_logger("quality.engine")

    def validate(self, df: pd.DataFrame, dataset: str | None = None) -> ValidationReport:
        report = ValidationReport(
            dataset=dataset or self.rule_set.dataset or self.rule_set.name,
            rule_set=self.rule_set.name,
            row_count=len(df),
        )
        for expectation in self.rule_set.expectations:
            fn = _REGISTRY.get(expectation.type)
            if fn is None:
                report.results.append(
                    ExpectationResult(
                        name=expectation.name,
                        type=expectation.type,
                        success=False,
                        severity=expectation.severity,
                        columns=expectation.columns,
                        message=f"Unknown expectation type '{expectation.type}'",
                    )
                )
                continue
            try:
                result = fn(df, expectation)
            except Exception as exc:  # noqa: BLE001 - never let one rule crash the run
                result = ExpectationResult(
                    name=expectation.name,
                    type=expectation.type,
                    success=False,
                    severity=expectation.severity,
                    columns=expectation.columns,
                    message=f"Expectation raised an error: {exc}",
                )
            report.results.append(result)

        self.log.info(
            "Quality validation complete",
            extra={
                "dataset": report.dataset,
                "rule_set": report.rule_set,
                "passed": report.passed,
                "failed": report.failed,
                "score": report.score,
                "success": report.success,
            },
        )
        return report

    def split_valid_invalid(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Partition rows into valid and quarantined based on row-level rules.

        Row-level rules (not_null, unique, range, allowed_values, custom) are
        applied to build a per-row validity mask. Dataset-level rules (schema,
        freshness) do not remove individual rows.
        """
        if df.empty:
            return df, df

        invalid_mask = pd.Series(False, index=df.index)
        for exp in self.rule_set.expectations:
            if exp.severity != "error":
                continue
            invalid_mask |= self._row_violations(df, exp)

        valid = df[~invalid_mask].copy()
        quarantined = df[invalid_mask].copy()
        return valid, quarantined

    @staticmethod
    def _row_violations(df: pd.DataFrame, exp: ExpectationConfig) -> pd.Series:
        """Return a boolean mask of rows violating a single expectation."""
        mask = pd.Series(False, index=df.index)
        if exp.type in {"not_null", "completeness"}:
            for col in exp.columns:
                if col in df.columns:
                    mask |= df[col].isna()
        elif exp.type in {"unique", "primary_key"}:
            cols = [c for c in exp.columns if c in df.columns]
            if cols:
                mask |= df.duplicated(subset=cols, keep="first")
                if exp.params.get("primary_key"):
                    mask |= df[cols].isna().any(axis=1)
        elif exp.type == "range":
            low, high = exp.params.get("min"), exp.params.get("max")
            for col in exp.columns:
                if col in df.columns:
                    series = pd.to_numeric(df[col], errors="coerce")
                    if low is not None:
                        mask |= series < low
                    if high is not None:
                        mask |= series > high
        elif exp.type in {"allowed_values", "categorical"}:
            allowed = set(exp.params.get("values", []))
            for col in exp.columns:
                if col in df.columns:
                    mask |= ~df[col].isna() & ~df[col].isin(allowed)
        elif exp.type in {"custom", "business_rule"}:
            expression = exp.params.get("expression")
            if expression:
                with contextlib.suppress(Exception):
                    mask |= ~df.eval(expression)
        return mask.fillna(False)
