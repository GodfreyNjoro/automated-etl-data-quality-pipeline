# Deployment guide

This guide covers running the pipeline in a real environment: containers, secrets,\
scheduling, the metadata database, the dashboard, and CI/CD.

## Prerequisites

* Docker and Docker Compose (for the containerised stack), or

* Python 3.10+ and PostgreSQL 13+ (for a manual install).

## Configuration & secrets

All secrets are read from the environment — never from committed files. Copy the\
template and fill it in:

```bash
cp .env.example .env
```

Key groups:

* `ETL_METADATA_DB_*` — the metadata database (run history, quality metrics, lineage).

* `ETL_WAREHOUSE_DB_*` — the default target warehouse.

* `ETL_SMTP_*`, `ETL_ALERT_EMAIL_*`, `ETL_SLACK_WEBHOOK_URL` — alerting.

* Source secrets referenced by config files, e.g. `POS_API_TOKEN`, `CRM_API_KEY`,\
  `INVENTORY_DB_PASSWORD`.

In production, inject these through your platform's secret manager (Docker secrets,\
Kubernetes secrets, AWS SSM/Secrets Manager, Vault) rather than a `.env` file.

## Containerised stack (Docker Compose)

```bash
docker compose up --build
```

The compose file provisions:

* **postgres** — hosts both the metadata database and the warehouse.

* **pipeline** — the ETL service (runs flows / scheduled pipelines).

* **dashboard** — the Streamlit monitoring UI.

Bring the metadata schema up on first run:

```bash
docker compose exec postgres psql -U etl -d etl_metadata -f /app/db/metadata_schema.sql
```

## Manual install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# create the metadata schema
psql "$DATABASE_URL" -f db/metadata_schema.sql

# run a pipeline
python -c "from src.orchestration.pipeline_runner import run_pipeline; run_pipeline('retail_aggregation')"
```

## Scheduling with Prefect

The runner is orchestrator-agnostic; for scheduled runs use the Prefect flow:

```bash
# run once
python -m flows.etl_flow

# build a cron deployment (daily at 02:00)
prefect deployment build flows/etl_flow.py:etl_pipeline_flow \
    -n retail-daily --cron "0 2 * * *"
prefect deployment apply etl_pipeline_flow-deployment.yaml
prefect agent start -q default
```

The `schedule` field in the pipeline YAML documents the intended cadence and can be\
used to drive the deployment's cron expression.

## The monitoring dashboard

```bash
streamlit run dashboard/app.py
```

Point it at the metadata database via the same `ETL_METADATA_DB_*` variables. In a\
container deployment it is exposed by the `dashboard` service. Put it behind your\
reverse proxy / SSO for production access.

## Operational concerns

* **Backups.** The metadata database holds run history and quality trends — back it\
  up alongside the warehouse.

* **Quarantine review.** Periodically review quarantined records; correct upstream\
  data issues and replay where appropriate.

* **Alert tuning.** Adjust `ETL_ALERT_SUPPRESSION_MINUTES` and per-rule severities\
  to balance signal against noise.

* **Scaling.** The runner processes stages within a run sequentially by dependency\
  order; scale horizontally by running independent pipelines as separate Prefect\
  deployments/workers.

* **Logging.** Logs are emitted as structured JSON (`ETL_LOG_JSON=true`) for easy\
  ingestion into a log aggregator.

## CI/CD

`.github/workflows/ci.yml` installs dependencies, runs the linters/tests, and\
reports coverage on every push and pull request. Extend it with a deployment job\
(build and push the image, apply the Prefect deployment) once you have a target\
environment and registry credentials configured as repository secrets.