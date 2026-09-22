"""End-to-end runnable example: retail multi-source aggregation.

This script demonstrates the full system with **no external services**:

    1. Generates synthetic retail data (with realistic quality issues).
    2. Runs the ``retail_aggregation`` pipeline into a local SQLite warehouse.
    3. Prints a run summary and writes it to ``data/last_run.json`` so the
       monitoring dashboard can display results even without a metadata database.

Usage::

    python examples/retail_aggregation/run_example.py
    python examples/retail_aggregation/run_example.py --no-metadata
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a plain script (python examples/.../run_example.py) by
# ensuring the project root is importable.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from examples.retail_aggregation.generate_data import generate_all
from src.orchestration.pipeline_runner import PipelineRunner
from src.utils.logging_config import configure_logging, get_logger

_log = get_logger("examples.retail")
DATA_DIR = Path(__file__).resolve().parent / "data"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail aggregation example.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--no-metadata", action="store_true",
        help="Run without the metadata database (pure offline demo).",
    )
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    configure_logging()

    print("Generating synthetic retail data...")
    paths = generate_all(seed=args.seed)
    for name, path in paths.items():
        print(f"  - {name}: {path.name}")

    print("\nRunning the retail_aggregation pipeline...")
    runner = PipelineRunner.from_config(
        "retail_aggregation", enable_metadata=not args.no_metadata
    )
    result = runner.run(triggered_by="example", full_refresh=args.full_refresh)

    summary = result.to_dict()
    out_path = DATA_DIR / "last_run.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str))

    print("\n" + "=" * 60)
    print(f"Run ID:        {result.run_id}")
    print(f"Status:        {result.status}")
    print(f"Duration:      {result.duration_seconds:.2f}s")
    print(f"Rows extracted:   {result.rows_extracted}")
    print(f"Rows loaded:      {result.rows_loaded}")
    print(f"Rows quarantined: {result.rows_quarantined}")
    print("-" * 60)
    for stage in result.stages:
        score = f"{stage.quality_score:.0%}" if stage.quality_score is not None else "n/a"
        print(
            f"  {stage.name:<14} {stage.status:<8} "
            f"extracted={stage.rows_extracted:<6} loaded={stage.rows_loaded:<6} "
            f"quarantined={stage.rows_quarantined:<4} quality={score}"
        )
    print("=" * 60)
    print(f"\nRun summary written to: {out_path}")
    print("Warehouse (SQLite):     examples/retail_aggregation/data/retail_warehouse.db")


if __name__ == "__main__":
    main()
