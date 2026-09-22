"""Loader interface and result structure."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.utils.config_loader import TargetConfig
from src.utils.logging_config import get_logger


@dataclass
class LoadResult:
    """Outcome of a load operation."""

    target_name: str
    table: str
    rows_loaded: int
    strategy: str
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseLoader(ABC):
    """Common contract for warehouse loaders."""

    def __init__(self, config: TargetConfig) -> None:
        self.config = config
        self.log = get_logger(f"loader.{config.type}")

    @abstractmethod
    def load(
        self,
        df: pd.DataFrame,
        table: str,
        strategy: str = "full",
        incremental_key: str | None = None,
    ) -> LoadResult:
        """Load a DataFrame into the target table."""

    def test_connection(self) -> bool:
        return True
