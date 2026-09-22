"""Base extractor interface and shared data structures."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.utils.config_loader import SourceConfig
from src.utils.logging_config import get_logger


@dataclass
class ExtractionResult:
    """Outcome of an extraction run."""

    source_name: str
    source_type: str
    data: pd.DataFrame
    row_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_frame(
        cls, source: SourceConfig, frame: pd.DataFrame, **metadata: Any
    ) -> "ExtractionResult":
        return cls(
            source_name=source.name,
            source_type=source.type,
            data=frame,
            row_count=len(frame),
            metadata=metadata,
        )


class BaseExtractor(ABC):
    """Common contract for all extractors."""

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self.options = config.options
        self.log = get_logger(f"extractor.{config.type}")

    @abstractmethod
    def extract(self, since: Any | None = None) -> ExtractionResult:
        """Extract data from the source.

        Parameters
        ----------
        since:
            Optional incremental watermark. Implementations that support
            incremental extraction use it to pull only newer records.
        """

    def test_connection(self) -> bool:
        """Best-effort connectivity check. Overridden where meaningful."""
        return True
