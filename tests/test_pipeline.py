"""End-to-end and unit tests for the orchestration layer."""

from __future__ import annotations

import pandas as pd
import pytest

from src.orchestration.pipeline_runner import PipelineRunner
from src.orchestration.retry import retry_with_backoff
from src.utils.config_loader import PipelineStage


def test_retry_succeeds_after_failures():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert retry_with_backoff(flaky, max_retries=3, backoff_seconds=0.01) == "ok"
    assert calls["n"] == 3


def test_retry_exhausts_and_raises():
    def always_fail():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        retry_with_backoff(always_fail, max_retries=2, backoff_seconds=0.01)


def test_stage_ordering_respects_dependencies():
    stages = [
        PipelineStage(name="c", source="s", target="t", depends_on=["b"]),
        PipelineStage(name="b", source="s", target="t", depends_on=["a"]),
        PipelineStage(name="a", source="s", target="t"),
    ]
    ordered = [s.name for s in PipelineRunner._order_stages(stages)]
    assert ordered.index("a") < ordered.index("b") < ordered.index("c")


def test_stage_ordering_handles_cycle():
    stages = [
        PipelineStage(name="a", source="s", target="t", depends_on=["b"]),
        PipelineStage(name="b", source="s", target="t", depends_on=["a"]),
    ]
    ordered = PipelineRunner._order_stages(stages)
    assert len(ordered) == 2  # cycle broken, no crash


def test_end_to_end_retail_pipeline():
    """Run the full bundled pipeline offline (SQLite, no metadata DB)."""
    from examples.retail_aggregation.generate_data import generate_all

    generate_all(seed=7)
    runner = PipelineRunner.from_config("retail_aggregation", enable_metadata=False)
    result = runner.run(triggered_by="test")

    assert result.status == "success"
    assert result.rows_loaded > 0
    assert result.rows_quarantined > 0  # injected quality issues are caught
    assert {s.name for s in result.stages} == {
        "products", "customers", "inventory", "transactions"
    }
    # Every stage should have produced a quality score.
    assert all(s.quality_score is not None for s in result.stages)
    d = result.to_dict()
    assert d["status"] == "success"


def test_warehouse_tables_populated(tmp_path):
    """The pipeline should create and populate warehouse tables."""
    from sqlalchemy import create_engine, inspect

    from examples.retail_aggregation.generate_data import generate_all
    from src.utils.settings import PROJECT_ROOT

    generate_all(seed=11)
    runner = PipelineRunner.from_config("retail_aggregation", enable_metadata=False)
    runner.run(triggered_by="test")

    db = PROJECT_ROOT / "examples" / "retail_aggregation" / "data" / "retail_warehouse.db"
    engine = create_engine(f"sqlite:///{db}")
    tables = set(inspect(engine).get_table_names())
    assert {"dim_product", "dim_customer", "fact_inventory", "fact_sales"} <= tables
    count = pd.read_sql("SELECT COUNT(*) AS n FROM fact_sales", engine)["n"].iloc[0]
    assert count > 0
