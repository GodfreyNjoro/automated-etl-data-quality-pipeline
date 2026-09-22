"""Tests for the data quality framework."""

from __future__ import annotations

import pandas as pd

from src.quality import QualityEngine, register_expectation
from src.quality.expectations import (
    expect_allowed_values,
    expect_freshness,
    expect_not_null,
    expect_outliers,
    expect_range,
    expect_schema,
    expect_unique,
)
from src.quality.results import ExpectationResult
from src.utils.config_loader import ExpectationConfig, QualityRuleSet


def _cfg(**kwargs) -> ExpectationConfig:
    kwargs.setdefault("name", kwargs.get("type", "x"))
    return ExpectationConfig(**kwargs)


def test_schema_detects_missing_columns(sample_df):
    cfg = _cfg(type="schema", params={"columns": {"id": "int", "missing": "str"}})
    result = expect_schema(sample_df, cfg)
    assert not result.success
    assert "missing" in result.observed["missing"]


def test_schema_passes(sample_df):
    cfg = _cfg(type="schema", params={"columns": {"id": "int"}})
    assert expect_schema(sample_df, cfg).success


def test_not_null_detects_nulls(sample_df):
    cfg = _cfg(type="not_null", columns=["category"])
    result = expect_not_null(sample_df, cfg)
    assert not result.success
    assert result.observed["violations"]["category"] > 0


def test_not_null_threshold_tolerates(sample_df):
    cfg = _cfg(type="not_null", columns=["category"], params={"max_null_rate": 0.5})
    assert expect_not_null(sample_df, cfg).success


def test_unique_detects_duplicates(dup_df):
    cfg = _cfg(type="unique", columns=["id"])
    result = expect_unique(dup_df, cfg)
    assert not result.success
    assert result.observed["duplicate_rows"] >= 2


def test_primary_key_flags_nulls(dup_df):
    cfg = _cfg(type="primary_key", columns=["id"], params={"primary_key": True})
    result = expect_unique(dup_df, cfg)
    assert not result.success
    assert result.observed["null_key_rows"] == 1


def test_range_detects_violation(sample_df):
    cfg = _cfg(type="range", columns=["amount"], params={"min": 0})
    result = expect_range(sample_df, cfg)
    assert not result.success


def test_range_within_bounds(sample_df):
    cfg = _cfg(type="range", columns=["amount"], params={"min": -100, "max": 5000})
    assert expect_range(sample_df, cfg).success


def test_allowed_values(sample_df):
    cfg = _cfg(type="allowed_values", columns=["category"], params={"values": ["a", "b"]})
    result = expect_allowed_values(sample_df, cfg)
    assert not result.success
    assert "c" in result.observed["category"]["unexpected"]


def test_outliers_iqr():
    df = pd.DataFrame({"v": [1, 2, 3, 4, 5, 6, 7, 8, 9, 1000]})
    cfg = _cfg(type="outliers", columns=["v"], params={"method": "iqr", "max_outlier_rate": 0.0})
    assert not expect_outliers(df, cfg).success


def test_outliers_zscore():
    df = pd.DataFrame({"v": [10] * 20 + [10000]})
    cfg = _cfg(type="outliers", columns=["v"], params={"method": "zscore", "factor": 3, "max_outlier_rate": 0.0})
    assert not expect_outliers(df, cfg).success


def test_freshness_stale():
    df = pd.DataFrame({"ts": pd.to_datetime(["2000-01-01", "2000-01-02"])})
    cfg = _cfg(type="freshness", columns=["ts"], params={"max_age_hours": 24})
    assert not expect_freshness(df, cfg).success


def test_freshness_fresh():
    now = pd.Timestamp.utcnow().tz_localize(None)
    df = pd.DataFrame({"ts": [now]})
    cfg = _cfg(type="freshness", columns=["ts"], params={"max_age_hours": 24})
    assert expect_freshness(df, cfg).success


def test_engine_validate_and_split(sample_df):
    rule_set = QualityRuleSet(
        name="rs",
        dataset="sample",
        expectations=[
            _cfg(name="cat_not_null", type="not_null", columns=["category"], severity="error"),
            _cfg(name="amount_range", type="range", columns=["amount"], params={"min": 0}, severity="error"),
        ],
    )
    engine = QualityEngine(rule_set)
    report = engine.validate(sample_df)
    assert report.total == 2
    assert not report.success  # both fail
    valid, quarantined = engine.split_valid_invalid(sample_df)
    assert len(valid) + len(quarantined) == len(sample_df)
    assert len(quarantined) >= 2


def test_engine_unknown_type():
    rule_set = QualityRuleSet(
        name="rs", expectations=[_cfg(name="x", type="does_not_exist")]
    )
    report = QualityEngine(rule_set).validate(pd.DataFrame({"a": [1]}))
    assert not report.success


def test_register_custom_expectation():
    def always_fail(df, cfg):
        return ExpectationResult(name=cfg.name, type=cfg.type, success=False, severity="error")

    register_expectation("always_fail", always_fail)
    rule_set = QualityRuleSet(name="rs", expectations=[_cfg(name="x", type="always_fail")])
    assert not QualityEngine(rule_set).validate(pd.DataFrame({"a": [1]})).success


def test_report_score_and_dict(sample_df):
    rule_set = QualityRuleSet(
        name="rs",
        expectations=[
            _cfg(name="ok", type="range", columns=["amount"], params={"min": -100, "max": 5000}),
        ],
    )
    report = QualityEngine(rule_set).validate(sample_df)
    assert report.score == 1.0
    d = report.to_dict()
    assert d["rule_set"] == "rs" and "results" in d
