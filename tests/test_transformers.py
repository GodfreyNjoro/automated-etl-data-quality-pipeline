"""Tests for the transformation engine."""

from __future__ import annotations

import pandas as pd
import pytest

from src.transformers import Transformer
from src.utils.config_loader import TransformConfig


def _steps(*specs) -> list[TransformConfig]:
    return [TransformConfig(type=t, params=p) for t, p in specs]


def test_rename_lower():
    df = pd.DataFrame({"Product Name": [1], "UnitPrice": [2]})
    out = Transformer(_steps(("rename_lower", {}))).apply(df)
    assert list(out.columns) == ["product_name", "unit_price"]


def test_cast_numeric_and_datetime():
    df = pd.DataFrame({"a": ["1", "2"], "d": ["2024-01-01", "2024-02-01"]})
    out = Transformer(
        _steps(("cast", {"columns": {"a": "int", "d": "datetime"}}))
    ).apply(df)
    assert str(out["a"].dtype) == "Int64"
    assert "datetime64" in str(out["d"].dtype)


def test_derive_and_filter():
    df = pd.DataFrame({"q": [2, 3], "p": [10.0, 20.0]})
    out = Transformer(
        _steps(
            ("derive", {"column": "total", "expression": "q * p"}),
            ("filter", {"expression": "total > 30"}),
        )
    ).apply(df)
    assert out["total"].tolist() == [60.0]


def test_drop_duplicates_and_columns():
    df = pd.DataFrame({"id": [1, 1, 2], "x": [1, 1, 2]})
    out = Transformer(
        _steps(
            ("drop_duplicates", {"subset": ["id"]}),
            ("drop_columns", {"columns": ["x"]}),
        )
    ).apply(df)
    assert len(out) == 2 and "x" not in out.columns


def test_fillna_and_constant():
    df = pd.DataFrame({"a": [1, None]})
    out = Transformer(
        _steps(
            ("fillna", {"values": {"a": 0}}),
            ("add_constant", {"column": "src", "value": "retail"}),
        )
    ).apply(df)
    assert out["a"].tolist() == [1, 0]
    assert out["src"].unique().tolist() == ["retail"]


def test_aggregate():
    df = pd.DataFrame({"g": ["x", "x", "y"], "v": [1, 2, 3]})
    out = Transformer(
        _steps(("aggregate", {"group_by": ["g"], "aggregations": {"v": "sum"}}))
    ).apply(df)
    assert out.set_index("g")["v"].to_dict() == {"x": 3, "y": 3}


def test_unknown_transform_raises():
    with pytest.raises(ValueError):
        Transformer(_steps(("nope", {}))).apply(pd.DataFrame({"a": [1]}))
