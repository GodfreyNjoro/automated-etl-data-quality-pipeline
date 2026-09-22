# Architecture

This document explains how the pipeline is structured, how a run flows through the
system, and the design decisions behind it.

## Design goals

1. **Configuration over code.** Adding a source, a quality rule, or a whole
   pipeline should not require touching Python. All behaviour is declared in
   validated YAML.
2. **Data quality is not optional.** Every dataset is validated before it lands,
   and failing records are isolated rather than dropped or allowed through.
3. **Observability by default.** Every run records its history, quality metrics,
   volumes, and lineage so problems can be caught and traced.
4. **Orchestrator-agnostic core.** The execution engine has no dependency on any
   scheduler, so it runs identically in a test, a script, a cron job, or Prefect.
5. **Fail loud, degrade gracefully.** Misconfiguration fails fast with a clear
   error; optional infrastructure (metadata DB, Slack) degrades without taking the
   run down.

## Component overview

| Layer | Module | Responsibility |
| --- | --- | --- |
| Configuration | `src/utils/config_loader.py` | Parse and validate YAML into pydantic models; interpolate `${ENV}` secrets |
| Settings | `src/utils/settings.py` | Twelve-factor environment configuration |
| Extraction | `src/extractors/` | Pull data from REST APIs, databases, and files |
| Quality | `src/quality/` | Run expectations, score results, split valid/invalid |
| Transformation | `src/transformers/` | Apply an ordered list of declarative transforms |
| Loading | `src/loaders/` | Load to warehouse (full/incremental) and quarantine bad rows |
| Metadata | `src/metadata/` | Persist runs, stages, quality metrics, quarantine, lineage |
| Alerting | `src/alerting/` | Route quality/execution failures to email and Slack |
| Orchestration | `src/orchestration/` | Order stages, apply retries, drive the whole run |
| Flow | `flows/etl_flow.py` | Prefect wrapper for scheduling |
| Dashboard | `dashboard/` | Streamlit monitoring UI |

## The execution flow

The heart of the system is `PipelineRunner` (`src/orchestration/pipeline_runner.py`).
A run proceeds as follows:

1. **Start.** A `run_id` is generated, the metadata schema is ensured, and a run
   record is opened.
2. **Order stages.** Stages are topologically sorted by their `depends_on`
   declarations. Cyclic dependencies are broken defensively while preserving the
   original order.
3. **For each stage, in order:**
   - Skip if any dependency did not succeed.
   - **Extract** via the extractor resolved from the source's `type`.
   - **Validate** with the `QualityEngine` if a rule set is attached. The engine
     produces a report (per-expectation results plus an overall score) and splits
     the DataFrame into valid and invalid rows.
   - **Quarantine** invalid rows to a timestamped file, recording the event in
     metadata.
   - **Transform** the valid rows through the declarative `Transformer`.
   - **Load** into the target table using the configured strategy (full or
     incremental), inside a transaction.
   - **Record** stage metrics and lineage.
   - Retries with exponential backoff wrap the whole stage.
4. **Finish.** The overall status is derived (`success`, `partial`, or `failed`),
   the run record is closed, and a failure alert is sent if warranted.

```
extract ─▶ validate ─▶ quarantine invalid ─▶ transform ─▶ load
   │           │              │                              │
   └───────────┴──────────────┴──── metadata + lineage ─────┘
```

## Quality engine

The engine (`src/quality/engine.py`) holds a registry of expectation
implementations keyed by `type`. Each built-in expectation
(`src/quality/expectations.py`) is a pure function `(df, config) -> ExpectationResult`.

- The **score** for a dataset is the fraction of `error`-severity expectations that
  passed. `warning` expectations are recorded and trended but never block a run.
- `split_valid_invalid` identifies the rows responsible for failures so they can be
  quarantined, while clean rows continue downstream.
- New checks can be added to the module or registered at runtime with
  `register_expectation(type_name, fn)` — no engine changes required.

## Extraction

All extractors implement a common `BaseExtractor` interface and return an
`ExtractionResult` (data plus metadata). They are resolved by a factory keyed on
the source `type`:

- **REST API** — bearer/API-key/basic auth, token-bucket rate limiting, page /
  offset / cursor pagination, retry/backoff on transient errors and HTTP 429, and
  incremental extraction via a watermark query parameter.
- **Database** — PostgreSQL and MySQL over SQLAlchemy, with optional incremental
  `WHERE key > watermark` predicates.
- **File** — CSV, JSON (including nested records via a dotted `records_path`), and
  Excel.

## Loading & quarantine

The warehouse loader supports PostgreSQL, MySQL, and SQLite. Loads run inside a
transaction and roll back on failure. Two strategies are supported:

- **full** — replace the target table's contents.
- **incremental** — upsert/append using an `incremental_key` so re-runs are
  idempotent.

Quarantined records are written with their failure context so they can be
inspected, corrected, and replayed rather than lost.

## Metadata & lineage

The `MetadataRepository` persists everything needed for monitoring and audit:
pipeline runs, per-stage results, quality metrics (for historical trending),
observed schemas, quarantine events, column-level lineage, and error logs. When no
metadata database is configured the repository degrades gracefully — the pipeline
still runs, and the example writes a `last_run.json` the dashboard can read.

The schema DDL lives in [`db/metadata_schema.sql`](../db/metadata_schema.sql).

## Alerting

The `AlertManager` fans alerts out to configured channels (email over SMTP, Slack
over an incoming webhook). Alerts are keyed and suppressed within a configurable
window to prevent floods, and suppression state is tracked so the same failure does
not page repeatedly.

## Orchestration & scheduling

The runner is standalone. For scheduled production runs, `flows/etl_flow.py` wraps
it in a Prefect flow that can be deployed with a cron schedule. Prefect is an
optional dependency — the flow imports it lazily so the core remains usable without
it.
