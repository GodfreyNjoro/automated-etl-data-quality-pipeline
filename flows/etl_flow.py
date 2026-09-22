"""Prefect flow wrapping the ETL pipeline runner.

This module exposes the pipeline to Prefect for scheduling, retries and
observability. The heavy lifting stays in :class:`PipelineRunner`; Prefect adds
orchestration concerns (scheduling via cron, task-level retries, concurrency and
run history in the Prefect UI).

Run ad-hoc::

    python -m flows.etl_flow --pipeline retail_aggregation

Deploy on a cron schedule (schedule read from the pipeline YAML)::

    python -m flows.etl_flow --pipeline retail_aggregation --deploy
"""

from __future__ import annotations

import argparse
from typing import Any

from src.orchestration.pipeline_runner import PipelineRunner
from src.utils.config_loader import PipelineStage, load_pipeline
from src.utils.logging_config import configure_logging, get_logger

try:
    from prefect import flow, task

    _PREFECT_AVAILABLE = True
except Exception:  # pragma: no cover - Prefect optional at import time
    _PREFECT_AVAILABLE = False

    def flow(*args: Any, **kwargs: Any):  # type: ignore
        def decorator(fn):
            return fn

        return decorator if not args else args[0]

    def task(*args: Any, **kwargs: Any):  # type: ignore
        def decorator(fn):
            return fn

        return decorator if not args else args[0]


_log = get_logger("flows.etl")


@task(name="run-stage", retries=3, retry_delay_seconds=[5, 15, 45])
def _run_stage_task(runner: PipelineRunner, run_id: str, stage: PipelineStage,
                    full_refresh: bool):
    """Prefect task that executes a single pipeline stage."""
    return runner._run_stage(run_id, stage, full_refresh)


@flow(name="etl-data-quality-pipeline", log_prints=True)
def etl_pipeline_flow(
    pipeline_name: str = "retail_aggregation",
    triggered_by: str = "prefect",
    full_refresh: bool = False,
) -> dict[str, Any]:
    """Prefect entry point for a full pipeline run.

    The runner already implements dependency ordering, quality gating,
    quarantine, metadata and alerting, so the flow delegates to it and returns a
    serialisable summary that Prefect stores with the run.
    """
    configure_logging()
    runner = PipelineRunner.from_config(pipeline_name)
    result = runner.run(triggered_by=triggered_by, full_refresh=full_refresh)
    _log.info(
        "Flow finished",
        extra={"pipeline": pipeline_name, "status": result.status,
               "run_id": result.run_id},
    )
    return result.to_dict()


def _build_deployment(pipeline_name: str):  # pragma: no cover - requires Prefect server
    """Create a cron-scheduled deployment from the pipeline's YAML schedule."""
    if not _PREFECT_AVAILABLE:
        raise RuntimeError("Prefect is not installed; cannot deploy.")
    pipeline = load_pipeline(pipeline_name)
    schedule = pipeline.schedule
    _log.info(
        "Deploying flow",
        extra={"pipeline": pipeline_name, "schedule": schedule},
    )
    return etl_pipeline_flow.serve(
        name=f"{pipeline_name}-deployment",
        cron=schedule,
        parameters={"pipeline_name": pipeline_name},
        tags=["etl", "data-quality"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run or deploy the ETL flow.")
    parser.add_argument("--pipeline", default="retail_aggregation")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument(
        "--deploy", action="store_true",
        help="Serve the flow on the cron schedule defined in the pipeline config.",
    )
    args = parser.parse_args()

    if args.deploy:
        _build_deployment(args.pipeline)
    else:
        summary = etl_pipeline_flow(
            pipeline_name=args.pipeline, full_refresh=args.full_refresh
        )
        print(summary)


if __name__ == "__main__":
    main()
