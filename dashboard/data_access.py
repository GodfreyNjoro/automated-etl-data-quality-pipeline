"""Data access layer for the monitoring dashboard.

The dashboard prefers the metadata database when it is reachable, and otherwise
falls back to the local ``last_run.json`` summary written by the example runner
and the quarantine CSV files on disk. This keeps the dashboard useful in a demo
environment without a running PostgreSQL instance.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.metadata import MetadataRepository
from src.utils.settings import PROJECT_ROOT, QUARANTINE_DIR

_EXAMPLE_RUN = (
    PROJECT_ROOT / "examples" / "retail_aggregation" / "data" / "last_run.json"
)


class DashboardData:
    """Unified read API backed by the metadata DB with a local fallback."""

    def __init__(self) -> None:
        self.repo = MetadataRepository()
        self.db_available = self.repo.enabled

    # ------------------------------------------------------------------
    def _fallback_summary(self) -> dict | None:
        if _EXAMPLE_RUN.exists():
            try:
                return json.loads(_EXAMPLE_RUN.read_text())
            except Exception:  # noqa: BLE001
                return None
        return None

    def recent_runs(self, limit: int = 100) -> pd.DataFrame:
        if self.db_available:
            df = self.repo.get_recent_runs(limit)
            if not df.empty:
                return df
        summary = self._fallback_summary()
        if summary:
            return pd.DataFrame(
                [
                    {
                        "run_id": summary["run_id"],
                        "pipeline_name": summary["pipeline_name"],
                        "status": summary["status"],
                        "started_at": summary["started_at"],
                        "finished_at": summary.get("finished_at"),
                        "duration_seconds": summary.get("duration_seconds"),
                        "rows_extracted": summary.get("rows_extracted", 0),
                        "rows_loaded": summary.get("rows_loaded", 0),
                        "rows_quarantined": summary.get("rows_quarantined", 0),
                        "triggered_by": "example",
                    }
                ]
            )
        return pd.DataFrame()

    def stage_runs(self, run_id: str | None = None) -> pd.DataFrame:
        if self.db_available:
            df = self.repo.get_stage_runs(run_id)
            if not df.empty:
                return df
        summary = self._fallback_summary()
        if summary:
            rows = []
            for s in summary.get("stages", []):
                rows.append(
                    {
                        "run_id": summary["run_id"],
                        "stage_name": s["name"],
                        "status": s["status"],
                        "rows_extracted": s.get("rows_extracted", 0),
                        "rows_loaded": s.get("rows_loaded", 0),
                        "rows_quarantined": s.get("rows_quarantined", 0),
                        "quality_score": s.get("quality_score"),
                    }
                )
            return pd.DataFrame(rows)
        return pd.DataFrame()

    def quality_scores(self, dataset: str | None = None) -> pd.DataFrame:
        if self.db_available:
            df = self.repo.get_quality_scores(dataset)
            if not df.empty:
                return df
        summary = self._fallback_summary()
        if summary:
            rows = []
            for s in summary.get("stages", []):
                if s.get("quality_score") is not None:
                    rows.append(
                        {
                            "dataset": s["name"],
                            "score": s.get("quality_score"),
                            "row_count": s.get("rows_extracted", 0),
                            "recorded_at": summary.get("finished_at"),
                        }
                    )
            return pd.DataFrame(rows)
        return pd.DataFrame()

    def quality_metrics(self, run_id: str | None = None) -> pd.DataFrame:
        if self.db_available:
            return self.repo.get_quality_metrics(run_id)
        return pd.DataFrame()

    def lineage(self) -> pd.DataFrame:
        if self.db_available:
            df = self.repo.get_lineage()
            if not df.empty:
                return df
        summary = self._fallback_summary()
        if summary:
            rows = [
                {
                    "pipeline_name": summary["pipeline_name"],
                    "stage_name": s["name"],
                    "source_name": s["name"],
                    "target_name": "retail_warehouse",
                    "target_table": s["name"],
                }
                for s in summary.get("stages", [])
            ]
            return pd.DataFrame(rows)
        return pd.DataFrame()

    def alerts(self) -> pd.DataFrame:
        if self.db_available:
            return self.repo.get_alerts()
        return pd.DataFrame()

    def quarantine_log(self) -> pd.DataFrame:
        if self.db_available:
            df = self.repo.get_quarantine_log()
            if not df.empty:
                return df
        # Fallback: scan the quarantine directory on disk.
        rows = []
        base = Path(QUARANTINE_DIR)
        if base.exists():
            for path in sorted(base.rglob("*.csv")):
                try:
                    with open(path) as handle:
                        count = sum(1 for _ in handle) - 1
                except Exception:  # noqa: BLE001
                    count = 0
                rows.append(
                    {
                        "pipeline_name": path.parents[1].name,
                        "stage_name": path.parent.name,
                        "row_count": max(count, 0),
                        "reason": "quality_failure",
                        "file_path": str(path),
                    }
                )
        return pd.DataFrame(rows)

    def quarantine_sample(self, file_path: str, limit: int = 200) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path).head(limit)
        except Exception:  # noqa: BLE001
            return pd.DataFrame()

    def schema_history(self, dataset: str | None = None) -> pd.DataFrame:
        if self.db_available:
            return self.repo.get_schema_history(dataset)
        return pd.DataFrame()
