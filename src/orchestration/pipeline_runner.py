"""Core ETL orchestrator.

The :class:`PipelineRunner` executes a pipeline definition end-to-end:

    extract -> validate (quality) -> quarantine invalid -> transform -> load

for each stage, while honouring inter-stage dependencies, applying retries with
exponential backoff, recording metadata/lineage, and firing alerts on quality
or execution failures. It is deliberately decoupled from any specific
orchestrator (Prefect, Airflow, cron) so it can be embedded anywhere; the
Prefect flow in ``flows/`` is a thin wrapper around it.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.alerting import Alert, AlertManager
from src.extractors import get_extractor
from src.loaders import get_loader
from src.loaders.quarantine import quarantine_records
from src.metadata import MetadataRepository
from src.orchestration.retry import retry_with_backoff
from src.quality import QualityEngine
from src.transformers import Transformer
from src.utils.config_loader import (
    CONFIG_DIR,
    PipelineConfig,
    PipelineStage,
    load_pipeline,
    load_quality_rule_set,
    load_source,
    load_target,
)
from src.utils.logging_config import get_logger
from src.utils.settings import get_settings


@dataclass
class StageResult:
    """Outcome of a single pipeline stage."""

    name: str
    status: str  # success | failed | skipped
    rows_extracted: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    quality_score: float | None = None
    quality_passed: bool | None = None
    error: str | None = None
    quarantine_path: str | None = None


@dataclass
class PipelineResult:
    """Aggregate outcome of a pipeline run."""

    run_id: str
    pipeline_name: str
    status: str  # success | failed | partial
    started_at: datetime
    finished_at: datetime | None = None
    stages: list[StageResult] = field(default_factory=list)
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        if self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def rows_extracted(self) -> int:
        return sum(s.rows_extracted for s in self.stages)

    @property
    def rows_loaded(self) -> int:
        return sum(s.rows_loaded for s in self.stages)

    @property
    def rows_quarantined(self) -> int:
        return sum(s.rows_quarantined for s in self.stages)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "rows_extracted": self.rows_extracted,
            "rows_loaded": self.rows_loaded,
            "rows_quarantined": self.rows_quarantined,
            "stages": [s.__dict__ for s in self.stages],
            "error": self.error,
        }


class PipelineRunner:
    """Execute a configured ETL pipeline."""

    def __init__(
        self,
        pipeline: PipelineConfig,
        config_dir: Path | None = None,
        repository: MetadataRepository | None = None,
        alert_manager: AlertManager | None = None,
        enable_metadata: bool = True,
    ) -> None:
        self.pipeline = pipeline
        self.config_dir = config_dir or CONFIG_DIR
        self.settings = get_settings()
        self.log = get_logger("orchestration.runner")
        self.repository = repository or (
            MetadataRepository() if enable_metadata else None
        )
        self.alerts = alert_manager or AlertManager(
            settings=self.settings, repository=self.repository
        )

    # ------------------------------------------------------------------
    @classmethod
    def from_config(
        cls, pipeline_name: str, config_dir: Path | None = None, **kwargs: Any
    ) -> "PipelineRunner":
        pipeline = load_pipeline(pipeline_name, config_dir)
        return cls(pipeline, config_dir=config_dir, **kwargs)

    # ------------------------------------------------------------------
    def run(self, triggered_by: str = "manual", full_refresh: bool = False) -> PipelineResult:
        run_id = uuid.uuid4().hex
        started = datetime.now(timezone.utc)
        result = PipelineResult(
            run_id=run_id,
            pipeline_name=self.pipeline.name,
            status="running",
            started_at=started,
        )
        self.log.info(
            "Pipeline run started",
            extra={"run_id": run_id, "pipeline": self.pipeline.name,
                   "triggered_by": triggered_by},
        )
        if self.repository is not None:
            self.repository.init_schema()
            self.repository.start_run(run_id, self.pipeline.name, triggered_by)

        ordered = self._order_stages(self.pipeline.stages)
        completed: set[str] = set()

        for stage in ordered:
            unmet = [d for d in stage.depends_on if d not in completed]
            if unmet:
                self.log.warning(
                    "Skipping stage; unmet dependencies",
                    extra={"stage": stage.name, "unmet": unmet},
                )
                result.stages.append(
                    StageResult(name=stage.name, status="skipped",
                                error=f"Unmet dependencies: {unmet}")
                )
                continue

            stage_result = self._run_stage_with_retry(run_id, stage, full_refresh)
            result.stages.append(stage_result)
            if stage_result.status == "success":
                completed.add(stage.name)

        # Determine overall status.
        statuses = {s.status for s in result.stages}
        if statuses == {"success"}:
            result.status = "success"
        elif "success" in statuses:
            result.status = "partial"
        else:
            result.status = "failed"

        result.finished_at = datetime.now(timezone.utc)
        if self.repository is not None:
            self.repository.finish_run(
                run_id,
                status=result.status,
                rows_extracted=result.rows_extracted,
                rows_loaded=result.rows_loaded,
                rows_quarantined=result.rows_quarantined,
                duration_seconds=result.duration_seconds,
                error_message=result.error,
            )

        self.log.info(
            "Pipeline run finished",
            extra={"run_id": run_id, "status": result.status,
                   "rows_loaded": result.rows_loaded,
                   "rows_quarantined": result.rows_quarantined,
                   "duration_seconds": round(result.duration_seconds, 2)},
        )

        if result.status != "success":
            self._alert_run_failure(result)

        return result

    # ------------------------------------------------------------------
    def _run_stage_with_retry(
        self, run_id: str, stage: PipelineStage, full_refresh: bool
    ) -> StageResult:
        try:
            return retry_with_backoff(
                lambda: self._run_stage(run_id, stage, full_refresh),
                max_retries=self.pipeline.max_retries,
                backoff_seconds=self.pipeline.retry_backoff_seconds,
                description=f"stage:{stage.name}",
            )
        except Exception as exc:  # noqa: BLE001
            self.log.error(
                "Stage failed after retries",
                extra={"stage": stage.name, "error": str(exc)},
            )
            if self.repository is not None:
                self.repository.record_stage(
                    run_id, stage.name, status="failed",
                    source_name=stage.source, target_name=stage.target,
                    error_message=str(exc),
                )
                self.repository.log_error(
                    run_id, str(exc), level="ERROR", stage_name=stage.name
                )
            return StageResult(name=stage.name, status="failed", error=str(exc))

    def _run_stage(
        self, run_id: str, stage: PipelineStage, full_refresh: bool
    ) -> StageResult:
        stage_started = datetime.now(timezone.utc)
        self.log.info("Stage started", extra={"stage": stage.name, "run_id": run_id})

        # 1. Extract -----------------------------------------------------
        source = load_source(stage.source, self.config_dir)
        extractor = get_extractor(source)
        extraction = extractor.extract()
        df = extraction.data
        rows_extracted = len(df)

        if self.repository is not None:
            self.repository.record_schema(run_id, stage.name, df)

        # 2. Quality validation -----------------------------------------
        quality_score: float | None = None
        quality_passed: bool | None = None
        quarantined = pd.DataFrame()
        if stage.quality_rule_set:
            rule_set = load_quality_rule_set(stage.quality_rule_set, self.config_dir)
            engine = QualityEngine(rule_set)
            report = engine.validate(df, dataset=stage.name)
            quality_score = report.score
            quality_passed = report.success
            if self.repository is not None:
                self.repository.record_quality(run_id, stage.name, report)

            valid, quarantined = engine.split_valid_invalid(df)
            df = valid

            if not report.success:
                self._alert_quality_failure(run_id, stage, report)

        # 3. Quarantine invalid rows ------------------------------------
        quarantine_path = None
        rows_quarantined = len(quarantined)
        if rows_quarantined:
            quarantine_path = quarantine_records(
                quarantined, self.pipeline.name, stage.name, run_id,
                reason="quality_failure",
            )
            if self.repository is not None:
                self.repository.record_quarantine(
                    run_id, self.pipeline.name, stage.name, rows_quarantined,
                    "quality_failure", quarantine_path,
                )

        # 4. Transform ---------------------------------------------------
        if stage.transforms:
            df = Transformer(stage.transforms).apply(df)

        # 5. Load --------------------------------------------------------
        target = load_target(stage.target, self.config_dir)
        loader = get_loader(target)
        table = stage.target_table or stage.name
        strategy = "full" if full_refresh else stage.load_strategy
        load_result = loader.load(
            df, table, strategy=strategy, incremental_key=stage.incremental_key
        )

        # 6. Metadata & lineage -----------------------------------------
        if self.repository is not None:
            self.repository.record_stage(
                run_id, stage.name, status="success",
                source_name=stage.source, target_name=stage.target,
                target_table=table, load_strategy=strategy,
                rows_extracted=rows_extracted, rows_loaded=load_result.rows_loaded,
                rows_quarantined=rows_quarantined, started_at=stage_started,
            )
            self.repository.record_lineage(
                run_id, self.pipeline.name, stage.name, source.name,
                source.type, target.name, table,
            )

        self.log.info(
            "Stage completed",
            extra={"stage": stage.name, "rows_extracted": rows_extracted,
                   "rows_loaded": load_result.rows_loaded,
                   "rows_quarantined": rows_quarantined},
        )
        return StageResult(
            name=stage.name,
            status="success",
            rows_extracted=rows_extracted,
            rows_loaded=load_result.rows_loaded,
            rows_quarantined=rows_quarantined,
            quality_score=quality_score,
            quality_passed=quality_passed,
            quarantine_path=quarantine_path,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _order_stages(stages: list[PipelineStage]) -> list[PipelineStage]:
        """Topologically sort stages by ``depends_on`` (stable on ties)."""
        by_name = {s.name: s for s in stages}
        ordered: list[PipelineStage] = []
        visited: set[str] = set()
        temp: set[str] = set()

        def visit(stage: PipelineStage) -> None:
            if stage.name in visited:
                return
            if stage.name in temp:
                # Cyclic dependency: break the cycle, preserve original order.
                return
            temp.add(stage.name)
            for dep in stage.depends_on:
                if dep in by_name:
                    visit(by_name[dep])
            temp.discard(stage.name)
            visited.add(stage.name)
            ordered.append(stage)

        for stage in stages:
            visit(stage)
        return ordered

    # ------------------------------------------------------------------
    def _alert_quality_failure(self, run_id, stage, report) -> None:
        failures = report.error_failures
        lines = [f"- {r.name} ({r.type}): {r.message}" for r in failures[:10]]
        body = (
            f"Pipeline: {self.pipeline.name}\n"
            f"Stage: {stage.name}\n"
            f"Run: {run_id}\n"
            f"Quality score: {report.score:.2%}\n"
            f"Failed expectations ({len(failures)}):\n" + "\n".join(lines)
        )
        self.alerts.send(
            Alert(
                key=f"quality:{self.pipeline.name}:{stage.name}",
                subject=f"Data quality failure in {self.pipeline.name}/{stage.name}",
                body=body,
                severity="error",
                context={"run_id": run_id},
            )
        )

    def _alert_run_failure(self, result: PipelineResult) -> None:
        failed = [s for s in result.stages if s.status == "failed"]
        lines = [f"- {s.name}: {s.error}" for s in failed]
        body = (
            f"Pipeline: {result.pipeline_name}\n"
            f"Run: {result.run_id}\n"
            f"Status: {result.status}\n"
            f"Failed stages ({len(failed)}):\n" + "\n".join(lines)
        )
        self.alerts.send(
            Alert(
                key=f"run:{result.pipeline_name}",
                subject=f"Pipeline {result.pipeline_name} finished with status {result.status}",
                body=body,
                severity="error",
                context={"run_id": result.run_id},
            )
        )


def run_pipeline(
    pipeline_name: str,
    config_dir: Path | None = None,
    triggered_by: str = "manual",
    full_refresh: bool = False,
    enable_metadata: bool = True,
) -> PipelineResult:
    """Convenience entry point: load a pipeline by name and run it."""
    runner = PipelineRunner.from_config(
        pipeline_name, config_dir=config_dir, enable_metadata=enable_metadata
    )
    return runner.run(triggered_by=triggered_by, full_refresh=full_refresh)
