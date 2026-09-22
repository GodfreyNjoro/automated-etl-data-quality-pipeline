"""Environment-based application settings.

All secrets and environment-specific values are sourced from environment
variables (never hard-coded), following twelve-factor app conventions. A local
``.env`` file is loaded automatically when present to ease development.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

try:  # optional convenience for local development
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is optional
    pass

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
QUARANTINE_DIR = DATA_DIR / "quarantine"


class Settings(BaseSettings):
    """Central runtime configuration resolved from the environment."""

    model_config = SettingsConfigDict(env_prefix="ETL_", extra="ignore")

    # Runtime environment name (development | staging | production).
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=True)

    # Metadata database (pipeline history, quality metrics, lineage).
    metadata_db_host: str = Field(default="localhost")
    metadata_db_port: int = Field(default=5432)
    metadata_db_name: str = Field(default="etl_metadata")
    metadata_db_user: str = Field(default="etl")
    metadata_db_password: str = Field(default="etl")

    # Default target warehouse (can be overridden per-target config).
    warehouse_db_host: str = Field(default="localhost")
    warehouse_db_port: int = Field(default=5432)
    warehouse_db_name: str = Field(default="retail_warehouse")
    warehouse_db_user: str = Field(default="etl")
    warehouse_db_password: str = Field(default="etl")

    # Batch / performance tuning.
    batch_size: int = Field(default=5000)
    max_retries: int = Field(default=3)
    retry_backoff_seconds: float = Field(default=2.0)

    # Alerting.
    alert_email_enabled: bool = Field(default=False)
    smtp_host: str = Field(default="localhost")
    smtp_port: int = Field(default=587)
    smtp_user: str = Field(default="")
    smtp_password: str = Field(default="")
    alert_email_from: str = Field(default="etl-alerts@example.com")
    alert_email_to: str = Field(default="")

    slack_webhook_url: str = Field(default="")
    alert_suppression_minutes: int = Field(default=30)

    def metadata_db_url(self) -> str:
        """SQLAlchemy URL for the metadata database."""
        return (
            f"postgresql+psycopg2://{self.metadata_db_user}:{self.metadata_db_password}"
            f"@{self.metadata_db_host}:{self.metadata_db_port}/{self.metadata_db_name}"
        )

    def warehouse_db_url(self) -> str:
        """SQLAlchemy URL for the default target warehouse."""
        return (
            f"postgresql+psycopg2://{self.warehouse_db_user}:{self.warehouse_db_password}"
            f"@{self.warehouse_db_host}:{self.warehouse_db_port}/{self.warehouse_db_name}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached, process-wide settings instance."""
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    return Settings()
