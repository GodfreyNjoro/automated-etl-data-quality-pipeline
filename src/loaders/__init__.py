"""Data warehouse loaders."""

from src.loaders.base import BaseLoader, LoadResult
from src.loaders.warehouse_loader import WarehouseLoader
from src.utils.config_loader import TargetConfig


def get_loader(config: TargetConfig) -> BaseLoader:
    """Instantiate a loader for the given target configuration."""
    if config.type in {"postgresql", "mysql", "sqlite"}:
        return WarehouseLoader(config)
    raise ValueError(f"No loader registered for target type '{config.type}'")


__all__ = ["BaseLoader", "LoadResult", "WarehouseLoader", "get_loader"]
