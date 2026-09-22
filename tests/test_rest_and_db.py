"""Tests for the REST API and database extractors using mocks/SQLite."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine

from src.extractors.database_extractor import DatabaseExtractor
from src.extractors.rest_api_extractor import RestApiExtractor
from src.utils.config_loader import SourceConfig


class _FakeResp:
    def __init__(self, payload, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_rest_pagination(monkeypatch):
    pages = {
        1: {"data": [{"id": 1}, {"id": 2}]},
        2: {"data": [{"id": 3}, {"id": 4}]},
        3: {"data": []},
    }

    def fake_request(self, method, url, params=None, timeout=None):
        page = params.get("page", 1)
        return _FakeResp(pages.get(page, {"data": []}))

    monkeypatch.setattr("requests.Session.request", fake_request)
    cfg = SourceConfig(
        name="api", type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "endpoint": "/records",
            "records_path": "data",
            "auth": {"type": "bearer", "token": "t"},
            "pagination": {"type": "page", "page_param": "page",
                           "size_param": "size", "page_size": 2, "start_page": 1},
        },
    )
    result = RestApiExtractor(cfg).extract()
    assert result.row_count == 4
    assert sorted(result.data["id"].tolist()) == [1, 2, 3, 4]


def test_rest_retry_on_error(monkeypatch):
    calls = {"n": 0}

    def flaky_request(self, method, url, params=None, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise __import__("requests").RequestException("transient")
        return _FakeResp({"items": [{"id": 1}]})

    monkeypatch.setattr("requests.Session.request", flaky_request)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    cfg = SourceConfig(
        name="api", type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "records_path": "items",
            "max_retries": 2,
            "retry_backoff_seconds": 0.01,
        },
    )
    result = RestApiExtractor(cfg).extract()
    assert result.row_count == 1
    assert calls["n"] == 2


def test_rest_handles_429(monkeypatch):
    calls = {"n": 0}

    def rate_limited(self, method, url, params=None, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeResp({}, status=429, headers={"Retry-After": "0"})
        return _FakeResp({"data": [{"id": 1}]})

    monkeypatch.setattr("requests.Session.request", rate_limited)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    cfg = SourceConfig(
        name="api", type="rest_api",
        options={"base_url": "https://api.example.com", "records_path": "data"},
    )
    result = RestApiExtractor(cfg).extract()
    assert result.row_count == 1


def test_rest_auth_variants():
    for auth in (
        {"type": "api_key", "header": "X-Key", "value": "k"},
        {"type": "basic", "username": "u", "password": "p"},
        {"type": "none"},
    ):
        cfg = SourceConfig(
            name="api", type="rest_api",
            options={"base_url": "https://api.example.com", "auth": auth},
        )
        session = RestApiExtractor(cfg)._build_session()
        assert session is not None


@pytest.fixture
def sqlite_source(tmp_path):
    db = tmp_path / "src.db"
    engine = create_engine(f"sqlite:///{db}")
    pd.DataFrame({"id": [1, 2, 3], "ts": ["2024-01-01", "2024-02-01", "2024-03-01"]}).to_sql(
        "records", engine, index=False
    )
    return f"sqlite:///{db}"


def test_db_extract_table(sqlite_source):
    cfg = SourceConfig(
        name="db", type="postgresql",
        options={"connection": sqlite_source, "table": "records"},
    )
    result = DatabaseExtractor(cfg).extract()
    assert result.row_count == 3


def test_db_extract_query(sqlite_source):
    cfg = SourceConfig(
        name="db", type="postgresql",
        options={"connection": sqlite_source, "query": "SELECT * FROM records WHERE id > 1"},
    )
    result = DatabaseExtractor(cfg).extract()
    assert result.row_count == 2


def test_db_incremental(sqlite_source):
    cfg = SourceConfig(
        name="db", type="postgresql",
        options={"connection": sqlite_source, "table": "records", "incremental_key": "ts"},
    )
    result = DatabaseExtractor(cfg).extract(since="2024-01-15")
    assert result.row_count == 2


def test_db_test_connection(sqlite_source):
    cfg = SourceConfig(name="db", type="postgresql", options={"connection": sqlite_source})
    assert DatabaseExtractor(cfg).test_connection() is True


def test_db_connection_url_dict():
    cfg = SourceConfig(
        name="db", type="postgresql",
        options={"connection": {"host": "h", "port": 5432, "database": "d",
                                "user": "u", "password": "p"}},
    )
    url = DatabaseExtractor(cfg)._connection_url()
    assert url.startswith("postgresql+psycopg2://u:p@h:5432/d")


def test_db_missing_connection_raises():
    cfg = SourceConfig(name="db", type="postgresql", options={})
    with pytest.raises(ValueError):
        DatabaseExtractor(cfg)._connection_url()
