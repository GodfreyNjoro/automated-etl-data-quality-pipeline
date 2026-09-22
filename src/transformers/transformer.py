"""Declarative transformation engine.

Transformations are defined as an ordered list of steps in the pipeline config.
Each step has a ``type`` and a ``params`` dict. Supported steps:

    rename            params: {columns: {old: new}}
    cast              params: {columns: {col: int|float|str|datetime|bool}}
    derive            params: {column: name, expression: <pandas eval expr>}
    drop_columns      params: {columns: [..]}
    drop_duplicates   params: {subset: [..], keep: first|last}
    fillna            params: {values: {col: value}} or {value: v}
    filter            params: {expression: <pandas query expr>}
    rename_lower      params: {}  (lower-case + snake-case all columns)
    add_constant      params: {column: name, value: v}
    aggregate         params: {group_by: [..], aggregations: {col: func}}

The engine is deterministic and returns a new DataFrame, leaving the input
untouched.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from src.utils.config_loader import TransformConfig
from src.utils.logging_config import get_logger

_CAST_MAP = {
    "int": "Int64",
    "integer": "Int64",
    "float": "float64",
    "double": "float64",
    "str": "string",
    "string": "string",
    "bool": "boolean",
    "boolean": "boolean",
    "datetime": "datetime64[ns]",
    "date": "datetime64[ns]",
}


def _snake_case(name: str) -> str:
    name = re.sub(r"[\s\-]+", "_", str(name).strip())
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    return name.lower()


class Transformer:
    """Apply an ordered list of transformation steps to a DataFrame."""

    def __init__(self, steps: list[TransformConfig]) -> None:
        self.steps = steps
        self.log = get_logger("transformer")

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for step in self.steps:
            handler = getattr(self, f"_op_{step.type}", None)
            if handler is None:
                raise ValueError(f"Unknown transform type: {step.type}")
            result = handler(result, step.params)
            self.log.debug("Applied transform", extra={"type": step.type, "rows": len(result)})
        return result

    # ------------------------------------------------------------------ ops
    def _op_rename(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        return df.rename(columns=params.get("columns", {}))

    def _op_rename_lower(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        return df.rename(columns={c: _snake_case(c) for c in df.columns})

    def _op_cast(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        for col, dtype in params.get("columns", {}).items():
            if col not in df.columns:
                continue
            target = _CAST_MAP.get(str(dtype).lower(), dtype)
            if target == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif target in {"Int64", "float64"}:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                if target == "Int64":
                    df[col] = df[col].round().astype("Int64")
            else:
                df[col] = df[col].astype(target)
        return df

    def _op_derive(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        column = params["column"]
        # Use the python engine so nullable extension dtypes are supported.
        df[column] = df.eval(params["expression"], engine="python")
        return df

    def _op_drop_columns(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        return df.drop(columns=[c for c in params.get("columns", []) if c in df.columns])

    def _op_drop_duplicates(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        subset = params.get("subset")
        return df.drop_duplicates(subset=subset, keep=params.get("keep", "first"))

    def _op_fillna(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        if "values" in params:
            return df.fillna(params["values"])
        return df.fillna(params.get("value"))

    def _op_filter(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        return df.query(params["expression"])

    def _op_add_constant(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        df[params["column"]] = params["value"]
        return df

    def _op_aggregate(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        group_by = params["group_by"]
        aggregations = params["aggregations"]
        return df.groupby(group_by, as_index=False).agg(aggregations)
