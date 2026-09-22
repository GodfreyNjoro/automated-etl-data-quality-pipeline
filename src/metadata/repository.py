"""Persistence layer for pipeline metadata, quality metrics and lineage.

The repository writes execution history, data quality metrics, lineage and
schema-evolution snapshots to a PostgreSQL metadata database. It is designed to
**degrade gracefully**: if the metadata database is unavailable the pipeline
still runs, and every write becomes a no-op that is logged rather than raised.
This keeps the ETL resilient while the dashboard remains the source of truth
when the database is reachable.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.quality.results import ValidationReport
from src.utils.logging_config import get_logger
from src.utils.settings import PROJECT_ROOT, get_settings

_log = get_logger("metadata.repository")

_SCHEMA_FILE = PROJECT_ROOT / "db" / "metadata_schema.sql"


class MetadataRepository:
    """Read/write access to the metadata database.

    Parameters
    ----------
    db_url:
        SQLAlchemy URL. Defaults to the configured metadata database.
    required:
        When ``False`` (default) connection failures are swallowed and the
        repository operates in a disabled state. When ``True`` a failure to
        connect raises, which is useful for the dashboard.
    """

    def __init__(self, db_url: str | None = None, required: bool = False) -> None:
        self.db_url = db_url or get_settings().metadata_db_url()
        self._engine: Engine | None = None
        self.enabled = False
        try:
            self._engine = create_engine(
                self.db_url, pool_pre_ping=True, pool_size=5, max_overflow=5
            )
            # Validate connectivity eagerly so callers know the real state.
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.enabled = True
        except Exception as exc:  # noqa: BLE001 - graceful degradation
            if required:
                raise
            _log.warning(
                "Metadata database unavailable; running without persistence",
                extra={"error": str(exc)},
            )

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------
    def init_schema(self) -> bool:
        """Create metadata tables if they do not already exist."""
        if not self.enabled or self._engine is None:
            return False
        if not _SCHEMA_FILE.exists():
            _log.error("Schema file missing", extra={"path": str(_SCHEMA_FILE)})
            return False
        ddl = _SCHEMA_FILE.read_text()
        try:
            with self._engine.begin() as conn:
                for statement in _split_sql(ddl):
                    conn.execute(text(statement))
            _log.info("Metadata schema initialised")
            return True
        except Exception as exc:  # noqa: BLE001
            _log.error("Failed to initialise schema", extra={"error": str(exc)})
            return False

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        assert self._engine is not None
        with self._engine.begin() as conn:
            yield conn

    # ------------------------------------------------------------------
    # Pipeline run lifecycle
    # ------------------------------------------------------------------
    def start_run(
        self, run_id: str, pipeline_name: str, triggered_by: str = "manual"
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at, triggered_by)
                        VALUES (:run_id, :name, 'running', :started_at, :triggered_by)
                        ON CONFLICT (run_id) DO NOTHING
                        """
                    ),
                    {
                        "run_id": run_id,
                        "name": pipeline_name,
                        "started_at": datetime.now(timezone.utc),
                        "triggered_by": triggered_by,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("start_run failed", extra={"error": str(exc)})

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        rows_quarantined: int = 0,
        duration_seconds: float | None = None,
        error_message: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE pipeline_runs
                        SET status = :status,
                            finished_at = :finished_at,
                            duration_seconds = :duration,
                            rows_extracted = :rx,
                            rows_loaded = :rl,
                            rows_quarantined = :rq,
                            error_message = :err
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "status": status,
                        "finished_at": datetime.now(timezone.utc),
                        "duration": duration_seconds,
                        "rx": rows_extracted,
                        "rl": rows_loaded,
                        "rq": rows_quarantined,
                        "err": error_message,
                        "run_id": run_id,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("finish_run failed", extra={"error": str(exc)})

    def record_stage(
        self,
        run_id: str,
        stage_name: str,
        status: str,
        source_name: str | None = None,
        target_name: str | None = None,
        target_table: str | None = None,
        load_strategy: str | None = None,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        rows_quarantined: int = 0,
        started_at: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO stage_runs
                            (run_id, stage_name, source_name, target_name, target_table,
                             load_strategy, status, rows_extracted, rows_loaded,
                             rows_quarantined, started_at, finished_at, error_message)
                        VALUES
                            (:run_id, :stage, :source, :target, :table, :strategy, :status,
                             :rx, :rl, :rq, :started_at, :finished_at, :err)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "stage": stage_name,
                        "source": source_name,
                        "target": target_name,
                        "table": target_table,
                        "strategy": load_strategy,
                        "status": status,
                        "rx": rows_extracted,
                        "rl": rows_loaded,
                        "rq": rows_quarantined,
                        "started_at": started_at or datetime.now(timezone.utc),
                        "finished_at": datetime.now(timezone.utc),
                        "err": error_message,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_stage failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Quality metrics
    # ------------------------------------------------------------------
    def record_quality(
        self, run_id: str, stage_name: str, report: ValidationReport
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                for r in report.results:
                    conn.execute(
                        text(
                            """
                            INSERT INTO quality_metrics
                                (run_id, stage_name, dataset, rule_set, expectation_name,
                                 expectation_type, severity, success, observed, message)
                            VALUES
                                (:run_id, :stage, :dataset, :rule_set, :name, :type,
                                 :severity, :success, :observed, :message)
                            """
                        ),
                        {
                            "run_id": run_id,
                            "stage": stage_name,
                            "dataset": report.dataset,
                            "rule_set": report.rule_set,
                            "name": r.name,
                            "type": r.type,
                            "severity": r.severity,
                            "success": r.success,
                            "observed": json.dumps(r.observed, default=str),
                            "message": r.message,
                        },
                    )
                conn.execute(
                    text(
                        """
                        INSERT INTO quality_scores
                            (run_id, stage_name, dataset, rule_set, total, passed,
                             failed, score, row_count)
                        VALUES
                            (:run_id, :stage, :dataset, :rule_set, :total, :passed,
                             :failed, :score, :row_count)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "stage": stage_name,
                        "dataset": report.dataset,
                        "rule_set": report.rule_set,
                        "total": report.total,
                        "passed": report.passed,
                        "failed": report.failed,
                        "score": report.score,
                        "row_count": report.row_count,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_quality failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Lineage & schema history
    # ------------------------------------------------------------------
    def record_lineage(
        self,
        run_id: str,
        pipeline_name: str,
        stage_name: str,
        source_name: str,
        source_type: str,
        target_name: str,
        target_table: str,
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO lineage
                            (run_id, pipeline_name, stage_name, source_name, source_type,
                             target_name, target_table)
                        VALUES
                            (:run_id, :pipeline, :stage, :source, :source_type,
                             :target, :table)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "pipeline": pipeline_name,
                        "stage": stage_name,
                        "source": source_name,
                        "source_type": source_type,
                        "target": target_name,
                        "table": target_table,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_lineage failed", extra={"error": str(exc)})

    def record_schema(self, run_id: str, dataset: str, df: pd.DataFrame) -> None:
        """Snapshot a dataset schema and classify change vs the last snapshot."""
        if not self.enabled:
            return
        snapshot = {col: str(dtype) for col, dtype in df.dtypes.items()}
        try:
            with self._connect() as conn:
                prev = conn.execute(
                    text(
                        """
                        SELECT schema_snapshot FROM schema_history
                        WHERE dataset = :dataset
                        ORDER BY recorded_at DESC LIMIT 1
                        """
                    ),
                    {"dataset": dataset},
                ).fetchone()
                change_type = _classify_schema_change(
                    prev[0] if prev else None, snapshot
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO schema_history
                            (run_id, dataset, schema_snapshot, change_type)
                        VALUES (:run_id, :dataset, :snapshot, :change_type)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "dataset": dataset,
                        "snapshot": json.dumps(snapshot),
                        "change_type": change_type,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_schema failed", extra={"error": str(exc)})

    def record_quarantine(
        self,
        run_id: str,
        pipeline_name: str,
        stage_name: str,
        row_count: int,
        reason: str,
        file_path: str | None,
    ) -> None:
        if not self.enabled or row_count <= 0:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO quarantine_log
                            (run_id, pipeline_name, stage_name, row_count, reason, file_path)
                        VALUES (:run_id, :pipeline, :stage, :rows, :reason, :path)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "pipeline": pipeline_name,
                        "stage": stage_name,
                        "rows": row_count,
                        "reason": reason,
                        "path": file_path,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_quarantine failed", extra={"error": str(exc)})

    def log_error(
        self,
        run_id: str,
        message: str,
        level: str = "ERROR",
        stage_name: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO error_logs (run_id, stage_name, level, message, context)
                        VALUES (:run_id, :stage, :level, :message, :context)
                        """
                    ),
                    {
                        "run_id": run_id,
                        "stage": stage_name,
                        "level": level,
                        "message": message,
                        "context": json.dumps(context or {}, default=str),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("log_error failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Alert suppression
    # ------------------------------------------------------------------
    def recent_alert_exists(self, alert_key: str, within_minutes: int) -> bool:
        """Return True if a matching alert was sent within the window."""
        if not self.enabled:
            return False
        try:
            with self._connect() as conn:
                row = conn.execute(
                    text(
                        """
                        SELECT 1 FROM alert_log
                        WHERE alert_key = :key
                          AND sent = TRUE
                          AND created_at >= NOW() - (:mins || ' minutes')::interval
                        LIMIT 1
                        """
                    ),
                    {"key": alert_key, "mins": str(within_minutes)},
                ).fetchone()
                return row is not None
        except Exception as exc:  # noqa: BLE001
            _log.error("recent_alert_exists failed", extra={"error": str(exc)})
            return False

    def record_alert(
        self,
        alert_key: str,
        channel: str,
        severity: str,
        subject: str,
        body: str,
        sent: bool,
        suppressed: bool,
    ) -> None:
        if not self.enabled:
            return
        try:
            with self._connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO alert_log
                            (alert_key, channel, severity, subject, body, sent, suppressed)
                        VALUES (:key, :channel, :severity, :subject, :body, :sent, :suppressed)
                        """
                    ),
                    {
                        "key": alert_key,
                        "channel": channel,
                        "severity": severity,
                        "subject": subject,
                        "body": body,
                        "sent": sent,
                        "suppressed": suppressed,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            _log.error("record_alert failed", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Read helpers for the monitoring dashboard
    # ------------------------------------------------------------------
    def _read_df(self, query: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        if not self.enabled or self._engine is None:
            return pd.DataFrame()
        try:
            return pd.read_sql(text(query), self._engine, params=params or {})
        except Exception as exc:  # noqa: BLE001
            _log.error("read query failed", extra={"error": str(exc)})
            return pd.DataFrame()

    def get_recent_runs(self, limit: int = 100) -> pd.DataFrame:
        return self._read_df(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def get_stage_runs(self, run_id: str | None = None) -> pd.DataFrame:
        if run_id:
            return self._read_df(
                "SELECT * FROM stage_runs WHERE run_id = :run_id ORDER BY started_at",
                {"run_id": run_id},
            )
        return self._read_df("SELECT * FROM stage_runs ORDER BY started_at DESC LIMIT 500")

    def get_quality_scores(self, dataset: str | None = None, limit: int = 500) -> pd.DataFrame:
        if dataset:
            return self._read_df(
                """
                SELECT * FROM quality_scores WHERE dataset = :dataset
                ORDER BY recorded_at DESC LIMIT :limit
                """,
                {"dataset": dataset, "limit": limit},
            )
        return self._read_df(
            "SELECT * FROM quality_scores ORDER BY recorded_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def get_quality_metrics(self, run_id: str | None = None, limit: int = 1000) -> pd.DataFrame:
        if run_id:
            return self._read_df(
                "SELECT * FROM quality_metrics WHERE run_id = :run_id ORDER BY recorded_at",
                {"run_id": run_id},
            )
        return self._read_df(
            "SELECT * FROM quality_metrics ORDER BY recorded_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def get_lineage(self) -> pd.DataFrame:
        return self._read_df(
            """
            SELECT DISTINCT pipeline_name, stage_name, source_name, source_type,
                   target_name, target_table
            FROM lineage ORDER BY pipeline_name, stage_name
            """
        )

    def get_quarantine_log(self, limit: int = 200) -> pd.DataFrame:
        return self._read_df(
            "SELECT * FROM quarantine_log ORDER BY recorded_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def get_alerts(self, limit: int = 200) -> pd.DataFrame:
        return self._read_df(
            "SELECT * FROM alert_log ORDER BY created_at DESC LIMIT :limit",
            {"limit": limit},
        )

    def get_schema_history(self, dataset: str | None = None, limit: int = 200) -> pd.DataFrame:
        if dataset:
            return self._read_df(
                """
                SELECT * FROM schema_history WHERE dataset = :dataset
                ORDER BY recorded_at DESC LIMIT :limit
                """,
                {"dataset": dataset, "limit": limit},
            )
        return self._read_df(
            "SELECT * FROM schema_history ORDER BY recorded_at DESC LIMIT :limit",
            {"limit": limit},
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _split_sql(ddl: str) -> list[str]:
    """Split a DDL script into individual executable statements."""
    statements = []
    for chunk in ddl.split(";"):
        cleaned = "\n".join(
            line for line in chunk.splitlines() if not line.strip().startswith("--")
        ).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


def _classify_schema_change(
    previous: Any, current: dict[str, str]
) -> str:
    """Classify a schema change relative to the previous snapshot."""
    if previous is None:
        return "initial"
    if isinstance(previous, str):
        try:
            previous = json.loads(previous)
        except Exception:  # noqa: BLE001
            return "unknown"
    prev_cols = set(previous)
    cur_cols = set(current)
    if cur_cols - prev_cols:
        return "added"
    if prev_cols - cur_cols:
        return "removed"
    if any(previous[c] != current[c] for c in cur_cols & prev_cols):
        return "changed"
    return "unchanged"
