"""Tests for metadata repository helpers and graceful degradation."""

from __future__ import annotations

import pandas as pd

from src.metadata.repository import (
    MetadataRepository,
    _classify_schema_change,
    _split_sql,
)


def test_split_sql_strips_comments():
    ddl = "-- comment\nCREATE TABLE a (id INT);\n-- another\nCREATE TABLE b (id INT);"
    statements = _split_sql(ddl)
    assert len(statements) == 2
    assert all("comment" not in s for s in statements)


def test_classify_schema_change():
    assert _classify_schema_change(None, {"a": "int"}) == "initial"
    assert _classify_schema_change({"a": "int"}, {"a": "int", "b": "str"}) == "added"
    assert _classify_schema_change({"a": "int", "b": "str"}, {"a": "int"}) == "removed"
    assert _classify_schema_change({"a": "int"}, {"a": "float"}) == "changed"
    assert _classify_schema_change({"a": "int"}, {"a": "int"}) == "unchanged"


def test_classify_from_json_string():
    import json

    prev = json.dumps({"a": "int"})
    assert _classify_schema_change(prev, {"a": "int"}) == "unchanged"


def test_repository_degrades_gracefully():
    """An unreachable DB yields a disabled repository whose writes are no-ops."""
    repo = MetadataRepository(
        db_url="postgresql+psycopg2://x:x@127.0.0.1:1/none", required=False
    )
    assert repo.enabled is False
    # None of these should raise despite the DB being unavailable.
    repo.start_run("r1", "p")
    repo.finish_run("r1", "success")
    repo.record_stage("r1", "s", "success")
    repo.record_schema("r1", "ds", pd.DataFrame({"a": [1]}))
    repo.log_error("r1", "boom")
    assert repo.get_recent_runs().empty
    assert repo.recent_alert_exists("k", 30) is False
