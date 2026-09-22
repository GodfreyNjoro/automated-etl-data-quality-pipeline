"""Data source extractors.

Each extractor implements :class:`~src.extractors.base.BaseExtractor` and is
constructed from a :class:`~src.utils.config_loader.SourceConfig`. The
:func:`get_extractor` factory maps a source ``type`` to its implementation.
"""

from __future__ import annotations

from src.extractors.base import BaseExtractor, ExtractionResult
from src.extractors.database_extractor import DatabaseExtractor
from src.extractors.file_extractor import FileExtractor
from src.extractors.rest_api_extractor import RestApiExtractor
from src.utils.config_loader import SourceConfig

_REGISTRY: dict[str, type[BaseExtractor]] = {
    "rest_api": RestApiExtractor,
    "postgresql": DatabaseExtractor,
    "mysql": DatabaseExtractor,
    "csv": FileExtractor,
    "json": FileExtractor,
    "excel": FileExtractor,
}


def get_extractor(config: SourceConfig) -> BaseExtractor:
    """Instantiate the extractor matching ``config.type``."""
    if config.type not in _REGISTRY:
        raise ValueError(f"No extractor registered for source type '{config.type}'")
    return _REGISTRY[config.type](config)


__all__ = [
    "BaseExtractor",
    "ExtractionResult",
    "DatabaseExtractor",
    "FileExtractor",
    "RestApiExtractor",
    "get_extractor",
]
