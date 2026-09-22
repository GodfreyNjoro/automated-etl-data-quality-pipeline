"""Tests for extractors (file-based and factory)."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.extractors import get_extractor
from src.extractors.file_extractor import FileExtractor
from src.utils.config_loader import SourceConfig


def test_csv_extractor(tmp_path):
    csv = tmp_path / "data.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(csv, index=False)
    cfg = SourceConfig(name="s", type="csv", options={"path": str(csv)})
    result = FileExtractor(cfg).extract()
    assert result.row_count == 2
    assert list(result.data.columns) == ["a", "b"]


def test_json_records_path(tmp_path):
    payload = {"data": [{"id": 1}, {"id": 2}, {"id": 3}]}
    path = tmp_path / "data.json"
    path.write_text(json.dumps(payload))
    cfg = SourceConfig(
        name="s", type="json", options={"path": str(path), "records_path": "data"}
    )
    result = FileExtractor(cfg).extract()
    assert result.row_count == 3


def test_glob_multiple_files(tmp_path):
    for i in range(2):
        pd.DataFrame({"a": [i]}).to_csv(tmp_path / f"part{i}.csv", index=False)
    cfg = SourceConfig(name="s", type="csv", options={"path": str(tmp_path / "part*.csv")})
    result = FileExtractor(cfg).extract()
    assert result.row_count == 2


def test_missing_file_raises(tmp_path):
    cfg = SourceConfig(name="s", type="csv", options={"path": str(tmp_path / "nope.csv")})
    extractor = FileExtractor(cfg)
    assert extractor.test_connection() is False
    with pytest.raises(FileNotFoundError):
        extractor.extract()


def test_incremental_filter(tmp_path):
    csv = tmp_path / "data.csv"
    pd.DataFrame(
        {"ts": ["2024-01-01", "2024-06-01"], "v": [1, 2]}
    ).to_csv(csv, index=False)
    cfg = SourceConfig(
        name="s", type="csv",
        options={"path": str(csv), "incremental_key": "ts"},
    )
    result = FileExtractor(cfg).extract(since="2024-03-01")
    assert result.row_count == 1


def test_factory_returns_correct_type(tmp_path):
    csv = tmp_path / "data.csv"
    pd.DataFrame({"a": [1]}).to_csv(csv, index=False)
    cfg = SourceConfig(name="s", type="csv", options={"path": str(csv)})
    assert isinstance(get_extractor(cfg), FileExtractor)


def test_rest_records_dig():
    from src.extractors.rest_api_extractor import RateLimiter, RestApiExtractor

    body = {"result": {"items": [{"id": 1}, {"id": 2}]}}
    assert RestApiExtractor._dig(body, "result.items") == [{"id": 1}, {"id": 2}]
    assert RestApiExtractor._dig(body, None) == body
    # A rate limiter with no rate configured never blocks.
    RateLimiter(None).wait()
