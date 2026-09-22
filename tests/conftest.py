"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "category": ["a", "b", "a", "c", None],
            "amount": [10.0, 20.0, 30.0, -5.0, 1000.0],
        }
    )


@pytest.fixture
def dup_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 1, 2, 3, None],
            "value": [10, 10, 20, 30, 40],
        }
    )
