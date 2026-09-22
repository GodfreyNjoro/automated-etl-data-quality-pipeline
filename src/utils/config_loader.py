"""Configuration loading and validation.

Source, target, quality-rule and pipeline definitions live as YAML/JSON files
under ``config/``. They are parsed into validated pydantic models so that a
malformed config fails fast with a clear error rather than deep inside a run.

Environment-variable interpolation is supported via ``${VAR}`` or
``${VAR:default}`` syntax, keeping secrets out of the config files.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

from src.utils.settings import CONFIG_DIR

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _interpolate(value: Any) -> Any:
    """Recursively replace ``${VAR}`` / ``${VAR:default}`` with env values."""
    if isinstance(value, str):
        def repl(match: re.Match) -> str:
            var, default = match.group(1), match.group(2)
            return os.getenv(var, default if default is not None else "")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def load_raw(path: str | Path) -> dict[str, Any]:
    """Load a YAML or JSON config file with env interpolation applied."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = path.read_text()
    if path.suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(text) or {}
    elif path.suffix == ".json":
        data = json.loads(text)
    else:
        raise ValueError(f"Unsupported config format: {path.suffix}")
    return _interpolate(data)


# ---------------------------------------------------------------------------
# Pydantic config models
# ---------------------------------------------------------------------------
class SourceConfig(BaseModel):
    """Definition of a single data source."""

    name: str
    type: Literal["rest_api", "postgresql", "mysql", "csv", "json", "excel"]
    description: str = ""
    options: dict[str, Any] = Field(default_factory=dict)


class TargetConfig(BaseModel):
    """Definition of a target warehouse."""

    name: str
    type: Literal["postgresql", "mysql", "sqlite"]
    description: str = ""
    connection: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)


class ExpectationConfig(BaseModel):
    """A single data quality expectation/rule."""

    name: str
    type: str  # e.g. schema, not_null, unique, range, allowed_values, freshness, custom
    columns: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    severity: Literal["error", "warning"] = "error"


class QualityRuleSet(BaseModel):
    """A named collection of expectations applied to a dataset."""

    name: str
    dataset: str = ""
    expectations: list[ExpectationConfig] = Field(default_factory=list)


class TransformConfig(BaseModel):
    """A single transformation step."""

    type: str  # cast, rename, derive, drop_duplicates, filter, aggregate, fillna
    params: dict[str, Any] = Field(default_factory=dict)


class PipelineStage(BaseModel):
    """One extract→quality→transform→load stage in a pipeline."""

    name: str
    source: str
    target: str
    load_strategy: Literal["full", "incremental"] = "full"
    incremental_key: str | None = None
    quality_rule_set: str | None = None
    transforms: list[TransformConfig] = Field(default_factory=list)
    target_table: str = ""
    depends_on: list[str] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    """Top-level pipeline definition."""

    name: str
    description: str = ""
    schedule: str | None = None  # cron expression
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    stages: list[PipelineStage] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Typed loaders
# ---------------------------------------------------------------------------
def load_source(name: str, config_dir: Path | None = None) -> SourceConfig:
    base = config_dir or CONFIG_DIR
    return SourceConfig(**load_raw(base / "sources" / f"{name}.yaml"))


def load_target(name: str, config_dir: Path | None = None) -> TargetConfig:
    base = config_dir or CONFIG_DIR
    return TargetConfig(**load_raw(base / "targets" / f"{name}.yaml"))


def load_quality_rule_set(name: str, config_dir: Path | None = None) -> QualityRuleSet:
    base = config_dir or CONFIG_DIR
    return QualityRuleSet(**load_raw(base / "quality_rules" / f"{name}.yaml"))


def load_pipeline(name: str, config_dir: Path | None = None) -> PipelineConfig:
    base = config_dir or CONFIG_DIR
    return PipelineConfig(**load_raw(base / "pipelines" / f"{name}.yaml"))
