"""Tests for config loading, env interpolation and validation."""

from __future__ import annotations

import pytest

from src.utils.config_loader import (
    PipelineConfig,
    load_pipeline,
    load_quality_rule_set,
    load_raw,
    load_source,
    load_target,
)


def test_env_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret123")
    cfg = tmp_path / "s.yaml"
    cfg.write_text("name: s\ntype: csv\noptions:\n  token: ${MY_TOKEN}\n  fallback: ${MISSING:default_val}\n")
    data = load_raw(cfg)
    assert data["options"]["token"] == "secret123"
    assert data["options"]["fallback"] == "default_val"


def test_unsupported_format(tmp_path):
    bad = tmp_path / "s.txt"
    bad.write_text("x")
    with pytest.raises(ValueError):
        load_raw(bad)


def test_missing_file():
    with pytest.raises(FileNotFoundError):
        load_raw("/nonexistent/path.yaml")


def test_load_project_configs():
    """The bundled retail example configs must parse and validate."""
    source = load_source("product_catalog")
    assert source.type == "csv"
    target = load_target("retail_warehouse")
    assert target.type == "sqlite"
    rules = load_quality_rule_set("pos_transactions")
    assert any(e.type == "primary_key" for e in rules.expectations)
    pipeline = load_pipeline("retail_aggregation")
    assert isinstance(pipeline, PipelineConfig)
    assert len(pipeline.stages) == 4
    assert pipeline.schedule == "0 2 * * *"


def test_pipeline_dependencies_defined():
    pipeline = load_pipeline("retail_aggregation")
    stage_names = {s.name for s in pipeline.stages}
    for stage in pipeline.stages:
        for dep in stage.depends_on:
            assert dep in stage_names
