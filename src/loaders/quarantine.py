"""Quarantine store for records that fail data quality checks.

Failed records are written to a local Parquet/CSV store partitioned by pipeline,
stage and run id, and a summary is recorded in the metadata database so they can
be inspected, reprocessed or reported on later.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.logging_config import get_logger
from src.utils.settings import QUARANTINE_DIR

_log = get_logger("quarantine")


def quarantine_records(
    df: pd.DataFrame,
    pipeline: str,
    stage: str,
    run_id: str,
    reason: str = "quality_failure",
) -> str | None:
    """Persist quarantined rows and return the file path (or None if empty)."""
    if df is None or df.empty:
        return None

    run_dir = Path(QUARANTINE_DIR) / pipeline / stage
    run_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = run_dir / f"{run_id}_{timestamp}.csv"

    enriched = df.copy()
    enriched["_quarantine_reason"] = reason
    enriched["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
    enriched.to_csv(path, index=False)

    _log.warning(
        "Records quarantined",
        extra={"pipeline": pipeline, "stage": stage, "run_id": run_id,
               "rows": len(df), "path": str(path)},
    )
    return str(path)
