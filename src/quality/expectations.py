"""Built-in expectation implementations.

Each expectation is a callable ``(df, config) -> ExpectationResult``. They are
registered in the engine by ``type`` name. New checks can be added here or
registered at runtime via :func:`src.quality.engine.register_expectation`.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.quality.results import ExpectationResult
from src.utils.config_loader import ExpectationConfig

_PANDAS_TYPE_ALIASES = {
    "int": "int64",
    "integer": "int64",
    "float": "float64",
    "double": "float64",
    "str": "object",
    "string": "object",
    "bool": "bool",
    "boolean": "bool",
    "datetime": "datetime64[ns]",
    "date": "datetime64[ns]",
}


def _result(cfg: ExpectationConfig, success: bool, observed: dict[str, Any], message: str) -> ExpectationResult:
    return ExpectationResult(
        name=cfg.name,
        type=cfg.type,
        success=success,
        severity=cfg.severity,
        columns=cfg.columns,
        observed=observed,
        message=message,
    )


def expect_schema(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Validate column presence and (optionally) data types.

    params:
        columns: {col_name: expected_type}  OR use cfg.columns for presence-only.
        allow_extra: bool (default True) - fail if unexpected columns present.
    """
    expected_types: dict[str, str] = cfg.params.get("columns", {})
    required_cols = list(expected_types) or cfg.columns
    missing = [c for c in required_cols if c not in df.columns]

    type_mismatches: dict[str, str] = {}
    for col, expected in expected_types.items():
        if col in df.columns:
            actual = str(df[col].dtype)
            expected_dtype = _PANDAS_TYPE_ALIASES.get(expected.lower(), expected)
            if not actual.startswith(expected_dtype.replace("64", "")) and actual != expected_dtype:
                type_mismatches[col] = f"expected {expected_dtype}, got {actual}"

    extra: list[str] = []
    if not cfg.params.get("allow_extra", True):
        extra = [c for c in df.columns if c not in required_cols]

    success = not missing and not type_mismatches and not extra
    return _result(
        cfg,
        success,
        {"missing": missing, "type_mismatches": type_mismatches, "unexpected": extra},
        "Schema valid" if success else "Schema validation failed",
    )


def expect_not_null(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Completeness check: null rate per column must be within threshold.

    params:
        max_null_rate: float in [0,1] (default 0.0)
    """
    threshold = float(cfg.params.get("max_null_rate", 0.0))
    rates: dict[str, float] = {}
    violations: dict[str, float] = {}
    for col in cfg.columns:
        if col not in df.columns:
            violations[col] = 1.0
            continue
        rate = float(df[col].isna().mean()) if len(df) else 0.0
        rates[col] = round(rate, 4)
        if rate > threshold:
            violations[col] = round(rate, 4)
    success = not violations
    return _result(
        cfg,
        success,
        {"null_rates": rates, "threshold": threshold, "violations": violations},
        "Completeness within threshold" if success else "Null rate threshold exceeded",
    )


def expect_unique(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Uniqueness / primary-key check across one or more columns.

    params:
        primary_key: bool - also require non-null (default False)
    """
    cols = cfg.columns
    present = [c for c in cols if c in df.columns]
    if len(present) != len(cols):
        return _result(cfg, False, {"missing": [c for c in cols if c not in present]},
                       "Uniqueness columns missing")
    dup_mask = df.duplicated(subset=cols, keep=False)
    dup_count = int(dup_mask.sum())
    observed: dict[str, Any] = {"duplicate_rows": dup_count}
    success = dup_count == 0
    if cfg.params.get("primary_key"):
        null_count = int(df[cols].isna().any(axis=1).sum())
        observed["null_key_rows"] = null_count
        success = success and null_count == 0
    return _result(
        cfg, success, observed,
        "Values unique" if success else f"Found {dup_count} duplicate rows",
    )


def expect_range(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Range / bounds validation for numeric columns.

    params:
        min: numeric lower bound (inclusive)
        max: numeric upper bound (inclusive)
        max_violation_rate: allowed fraction out of bounds (default 0.0)
    """
    low = cfg.params.get("min")
    high = cfg.params.get("max")
    max_violation_rate = float(cfg.params.get("max_violation_rate", 0.0))
    observed: dict[str, Any] = {}
    ok = True
    for col in cfg.columns:
        if col not in df.columns:
            observed[col] = "missing"
            ok = False
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        mask = pd.Series(False, index=series.index)
        if low is not None:
            mask |= series < low
        if high is not None:
            mask |= series > high
        rate = float(mask.mean()) if len(series) else 0.0
        observed[col] = {"violation_rate": round(rate, 4), "violations": int(mask.sum())}
        if rate > max_violation_rate:
            ok = False
    return _result(
        cfg, ok, observed,
        "Values within range" if ok else "Range violations exceeded threshold",
    )


def expect_allowed_values(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Categorical validation: values must be in an allowed set.

    params:
        values: [allowed, ...]
        max_violation_rate: float (default 0.0)
    """
    allowed = set(cfg.params.get("values", []))
    max_violation_rate = float(cfg.params.get("max_violation_rate", 0.0))
    observed: dict[str, Any] = {}
    ok = True
    for col in cfg.columns:
        if col not in df.columns:
            observed[col] = "missing"
            ok = False
            continue
        non_null = df[col].dropna()
        mask = ~non_null.isin(allowed)
        rate = float(mask.mean()) if len(non_null) else 0.0
        unexpected = sorted(map(str, set(non_null[mask].unique())))[:20]
        observed[col] = {"violation_rate": round(rate, 4), "unexpected": unexpected}
        if rate > max_violation_rate:
            ok = False
    return _result(
        cfg, ok, observed,
        "Categorical values valid" if ok else "Unexpected categorical values found",
    )


def expect_outliers(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Outlier detection using the IQR or z-score method.

    params:
        method: iqr|zscore (default iqr)
        factor: IQR multiplier (default 1.5) or z-score threshold (default 3)
        max_outlier_rate: allowed fraction of outliers (default 0.05)
    """
    method = cfg.params.get("method", "iqr")
    max_rate = float(cfg.params.get("max_outlier_rate", 0.05))
    observed: dict[str, Any] = {}
    ok = True
    for col in cfg.columns:
        if col not in df.columns:
            observed[col] = "missing"
            ok = False
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            observed[col] = {"outlier_rate": 0.0}
            continue
        if method == "zscore":
            factor = float(cfg.params.get("factor", 3.0))
            std = series.std(ddof=0)
            z = (series - series.mean()).abs() / std if std else pd.Series(0, index=series.index)
            mask = z > factor
        else:
            factor = float(cfg.params.get("factor", 1.5))
            q1, q3 = series.quantile(0.25), series.quantile(0.75)
            iqr = q3 - q1
            mask = (series < q1 - factor * iqr) | (series > q3 + factor * iqr)
        rate = float(mask.mean())
        observed[col] = {"outlier_rate": round(rate, 4), "outliers": int(mask.sum())}
        if rate > max_rate:
            ok = False
    return _result(
        cfg, ok, observed,
        "Outliers within threshold" if ok else "Outlier rate exceeded threshold",
    )


def expect_freshness(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Freshness check: the most recent timestamp must be recent enough.

    params:
        max_age_hours: maximum allowed age of the newest record (default 24)
        reference: 'now' (default) or an ISO timestamp for deterministic tests
    """
    max_age_hours = float(cfg.params.get("max_age_hours", 24))
    col = cfg.columns[0] if cfg.columns else None
    if not col or col not in df.columns or df.empty:
        return _result(cfg, False, {"column": col}, "Freshness column missing or empty")
    ts = pd.to_datetime(df[col], errors="coerce").dropna()
    if ts.empty:
        return _result(cfg, False, {"column": col}, "No valid timestamps")
    reference = cfg.params.get("reference", "now")
    ref = pd.Timestamp.utcnow().tz_localize(None) if reference == "now" else pd.Timestamp(reference)
    latest = ts.max()
    if getattr(latest, "tzinfo", None) is not None:
        latest = latest.tz_localize(None)
    age_hours = (ref - latest).total_seconds() / 3600.0
    success = age_hours <= max_age_hours
    return _result(
        cfg, success,
        {"latest": str(latest), "age_hours": round(age_hours, 2), "max_age_hours": max_age_hours},
        "Data is fresh" if success else "Data is stale",
    )


def expect_custom(df: pd.DataFrame, cfg: ExpectationConfig) -> ExpectationResult:
    """Custom business rule expressed as a pandas ``query`` predicate.

    params:
        expression: a boolean expression evaluated per row via df.eval();
                    rows evaluating to False are violations.
        max_violation_rate: float (default 0.0)
    """
    expression = cfg.params.get("expression")
    max_rate = float(cfg.params.get("max_violation_rate", 0.0))
    if not expression:
        return _result(cfg, False, {}, "No expression provided for custom rule")
    try:
        passing = df.eval(expression)
    except Exception as exc:  # noqa: BLE001 - surface bad expressions as failures
        return _result(cfg, False, {"error": str(exc)}, "Custom expression error")
    violation_rate = float((~passing).mean()) if len(df) else 0.0
    success = violation_rate <= max_rate
    return _result(
        cfg, success,
        {"expression": expression, "violation_rate": round(violation_rate, 4),
         "violations": int((~passing).sum())},
        "Business rule satisfied" if success else "Business rule violated",
    )


BUILTIN_EXPECTATIONS = {
    "schema": expect_schema,
    "not_null": expect_not_null,
    "completeness": expect_not_null,
    "unique": expect_unique,
    "primary_key": expect_unique,
    "range": expect_range,
    "allowed_values": expect_allowed_values,
    "categorical": expect_allowed_values,
    "outliers": expect_outliers,
    "freshness": expect_freshness,
    "custom": expect_custom,
    "business_rule": expect_custom,
}
