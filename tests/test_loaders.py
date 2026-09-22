"""Tests for warehouse loaders and quarantine (SQLite backend)."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from src.loaders import get_loader
from src.loaders.quarantine import quarantine_records
from src.loaders.warehouse_loader import WarehouseLoader
from src.utils.config_loader import TargetConfig


def _target(path) -> TargetConfig:
    return TargetConfig(name="wh", type="sqlite", connection={"path": str(path)})


def test_full_load_replaces(tmp_path):
    db = tmp_path / "wh.db"
    loader = WarehouseLoader(_target(db))
    df = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    result = loader.load(df, "t", strategy="full")
    assert result.rows_loaded == 2

    # Reload replaces rather than appends.
    loader.load(pd.DataFrame({"id": [3], "v": ["c"]}), "t", strategy="full")
    engine = create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM t")).scalar()
    assert count == 1


def test_incremental_upsert(tmp_path):
    db = tmp_path / "wh.db"
    loader = WarehouseLoader(_target(db))
    loader.load(pd.DataFrame({"id": [1, 2], "v": ["a", "b"]}), "t", strategy="full")
    # Upsert id=2 (update) and id=3 (insert).
    loader.load(
        pd.DataFrame({"id": [2, 3], "v": ["B", "c"]}),
        "t", strategy="incremental", incremental_key="id",
    )
    engine = create_engine(f"sqlite:///{db}")
    df = pd.read_sql("SELECT * FROM t ORDER BY id", engine)
    assert df["id"].tolist() == [1, 2, 3]
    assert df.set_index("id").loc[2, "v"] == "B"


def test_loader_factory(tmp_path):
    assert isinstance(get_loader(_target(tmp_path / "x.db")), WarehouseLoader)


def test_test_connection(tmp_path):
    assert WarehouseLoader(_target(tmp_path / "x.db")).test_connection() is True


def test_quarantine_writes_file(tmp_path, monkeypatch):
    import src.loaders.quarantine as q

    monkeypatch.setattr(q, "QUARANTINE_DIR", tmp_path)
    df = pd.DataFrame({"id": [1], "bad": [True]})
    path = quarantine_records(df, "pipe", "stage", "run1", reason="test")
    assert path is not None
    written = pd.read_csv(path)
    assert "_quarantine_reason" in written.columns


def test_quarantine_empty_returns_none():
    assert quarantine_records(pd.DataFrame(), "p", "s", "r") is None
