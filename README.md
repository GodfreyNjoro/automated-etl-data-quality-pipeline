# Automated ETL & Data Quality Monitoring Pipeline

A production-grade, configuration-driven ETL framework that extracts data from
multiple heterogeneous sources, validates it against a rich data-quality rule
set, quarantines bad records, loads clean data into a warehouse, and continuously
monitors pipeline health through a live dashboard and alerting.

The project ships with a fully runnable retail example — no external services
required — so you can see the entire system work end to end in under a minute.

---

## Why this project exists

Analytics and reporting are only as trustworthy as the data feeding them. In most
retail and e-commerce organisations, data arrives from a patchwork of systems:
a point-of-sale API, an inventory database, a product-catalog export, a CRM. Each
source has its own schema, cadence, and quality problems — duplicate keys, missing
prices, negative quantities, stale records. When those problems reach the warehouse
unchecked, they silently corrupt dashboards, forecasts, and decisions.

This pipeline treats data quality as a first-class concern rather than an
afterthought. Every record is validated *before* it lands, failures are isolated
instead of dropped or ignored, and the health of every run is measured, stored,
and surfaced so issues are caught early and traced quickly.

### What it does

- **Extracts** from REST APIs (auth, pagination, rate limiting, retries),
  relational databases (PostgreSQL / MySQL), and files (CSV / JSON / Excel) —
  all defined declaratively in YAML.
- **Validates** each dataset against schema, completeness, uniqueness, range,
  categorical, outlier, freshness, and custom business-rule expectations.
- **Quarantines** records that fail validation, with full context, instead of
  silently discarding them or letting them pollute the warehouse.
- **Transforms and loads** clean data into a warehouse using full or incremental
  strategies, inside transactions with rollback on failure.
- **Orchestrates** multi-stage pipelines with inter-stage dependencies, retries
  with exponential backoff, and cron scheduling via Prefect.
- **Monitors** run history, quality trends, data volumes, freshness, and lineage
  through a Streamlit dashboard.
- **Alerts** on quality and execution failures over email and Slack, with
  threshold-based routing and suppression to prevent alert fatigue.

---

## Architecture

```
                         ┌─────────────────────────────────────────────┐
                         │              Configuration (YAML)            │
                         │  sources · targets · quality_rules · pipelines│
                         └───────────────────────┬─────────────────────┘
                                                 │  (validated by pydantic)
                                                 ▼
  ┌────────────┐   ┌────────────┐   ┌──────────────────────────────────────┐
  │  REST API  │   │  Database  │   │            PipelineRunner             │
  │  (POS/CRM) │──▶│(PG / MySQL)│──▶│   for each stage (dependency-ordered) │
  └────────────┘   └────────────┘   │                                       │
  ┌────────────┐                     │   extract ─▶ quality ─▶ quarantine    │
  │   Files    │────────────────────▶│               │                       │
  │ CSV/JSON/  │                     │               ▼                       │
  │   Excel    │                     │        transform ─▶ load (txn)        │
  └────────────┘                     └───────┬───────────────────┬──────────┘
                                             │                   │
                          ┌──────────────────▼───┐      ┌────────▼───────────┐
                          │   Metadata & Lineage  │      │   Data Warehouse   │
                          │  (PostgreSQL): runs,  │      │  dim_product,      │
                          │  quality metrics,     │      │  dim_customer,     │
                          │  quarantine, lineage  │      │  fact_inventory,   │
                          └───────┬───────────────┘      │  fact_sales        │
                                  │                       └────────────────────┘
             ┌────────────────────┼────────────────────┐
             ▼                                          ▼
   ┌───────────────────┐                    ┌───────────────────────┐
   │ Streamlit         │                    │ Alerting              │
   │ monitoring        │                    │ email · Slack         │
   │ dashboard         │                    │ thresholds · dedup    │
   └───────────────────┘                    └───────────────────────┘
```

The `PipelineRunner` is deliberately decoupled from any specific orchestrator so
it can be embedded in a script, a test, a cron job, or a Prefect flow. The Prefect
flow under `flows/` is a thin wrapper around it.

For a deeper walk-through see [`docs/architecture.md`](docs/architecture.md).

---

## Quick start

### 1. Install

```bash
git clone https://github.com/GodfreyNjoro/automated-etl-data-quality-pipeline.git
cd automated-etl-data-quality-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the example (no external services needed)

```bash
python examples/retail_aggregation/run_example.py --no-metadata
```

This generates synthetic retail data (with realistic quality issues baked in),
runs the four-stage `retail_aggregation` pipeline into a local SQLite warehouse,
and prints a run summary:

```
============================================================
Run ID:        3f9c1a...
Status:        success
Duration:      0.44s
Rows extracted:   3014
Rows loaded:      2950
Rows quarantined: 64
------------------------------------------------------------
  products       success  extracted=121    loaded=118   quarantined=3    quality=43%
  customers      success  extracted=401    loaded=384   quarantined=17   quality=20%
  inventory      success  extracted=484    loaded=469   quarantined=15   quality=20%
  transactions   success  extracted=2008   loaded=1979  quarantined=29   quality=29%
============================================================
```

The intentionally low quality scores are the point: the generator injects the
kinds of defects real retail feeds contain (duplicate SKUs, missing prices,
negative quantities, invalid emails, stale timestamps), and the quality engine
catches them and quarantines the offending rows.

### 3. Launch the monitoring dashboard

```bash
streamlit run dashboard/app.py
```

The dashboard reads from the metadata database when configured, and falls back to
the example's `last_run.json` so it works immediately after the example run.

---

## Configuration

Everything is driven by YAML under `config/`. No code changes are needed to add a
source, change a quality rule, or wire up a new pipeline.

```
config/
├── sources/         # where data comes from
├── targets/         # where clean data goes
├── quality_rules/   # what "good" looks like per dataset
└── pipelines/       # how it all fits together
```

Secrets are never stored in config files. Any `${VAR}` or `${VAR:default}` token
is interpolated from the environment at load time.

### A source (REST API example)

```yaml
name: pos_api
type: rest_api
options:
  base_url: https://api.retail.example.com
  endpoint: /v2/transactions
  auth:
    type: bearer
    token: ${POS_API_TOKEN}
  records_path: data
  rate_limit_per_sec: 5
  pagination:
    type: page
    page_param: page
    size_param: page_size
    page_size: 500
  max_retries: 4
```

### A quality rule set

```yaml
name: pos_transactions
expectations:
  - name: transaction_id_unique
    type: primary_key
    columns: [transaction_id]
    severity: error
  - name: quantity_positive
    type: range
    columns: [quantity]
    params: { min: 1, max: 10000 }
    severity: error
  - name: total_amount_consistent
    type: business_rule
    params:
      expression: "abs(total_amount - (quantity * unit_price)) < 0.01"
    severity: warning
```

### A pipeline

```yaml
name: retail_aggregation
schedule: "0 2 * * *"        # daily at 02:00 when deployed via Prefect
max_retries: 2
stages:
  - name: products
    source: product_catalog
    target: retail_warehouse
    target_table: dim_product
    quality_rule_set: product_catalog
    transforms:
      - type: rename_lower
  - name: transactions
    source: pos_transactions
    target: retail_warehouse
    target_table: fact_sales
    load_strategy: incremental
    incremental_key: transaction_id
    quality_rule_set: pos_transactions
    depends_on: [products]
```

See [`docs/configuration.md`](docs/configuration.md) for the complete reference of
every source type, target type, expectation, and transform.

---

## Data quality expectations

| Type | Purpose |
| --- | --- |
| `schema` | Required columns present and correctly typed |
| `not_null` / `completeness` | Null rate within threshold |
| `unique` / `primary_key` | No duplicate (and, for keys, non-null) values |
| `range` | Numeric values within bounds, with an allowed violation rate |
| `allowed_values` / `categorical` | Values restricted to a permitted set |
| `outliers` | IQR or z-score outlier rate within threshold |
| `freshness` | Newest record no older than a maximum age |
| `custom` / `business_rule` | Arbitrary per-row boolean expression |

Each expectation has a severity of `error` (fails the check, quarantines rows) or
`warning` (recorded and trended but non-blocking). Custom checks can be registered
at runtime via `register_expectation`. See
[`docs/guides/adding-quality-rules.md`](docs/guides/adding-quality-rules.md).

---

## Programmatic use

```python
from src.orchestration.pipeline_runner import run_pipeline

result = run_pipeline("retail_aggregation", triggered_by="manual")

print(result.status)             # success | partial | failed
print(result.rows_loaded)
print(result.rows_quarantined)
for stage in result.stages:
    print(stage.name, stage.quality_score, stage.status)
```

Run without a metadata database (pure in-process):

```python
result = run_pipeline("retail_aggregation", enable_metadata=False)
```

---

## Orchestration & scheduling

Deploy the pipeline on a cron schedule with Prefect:

```bash
python -m flows.etl_flow          # run the flow once
prefect deployment build flows/etl_flow.py:etl_pipeline_flow \
    -n retail-daily --cron "0 2 * * *"
```

The runner applies per-stage retries with exponential backoff and orders stages
topologically by their `depends_on` declarations, skipping any stage whose
dependencies did not succeed.

---

## Deployment

The stack runs anywhere Docker does:

```bash
cp .env.example .env      # then edit secrets
docker compose up --build
```

This brings up PostgreSQL (metadata + warehouse), the pipeline service, and the
Streamlit dashboard. See [`docs/deployment.md`](docs/deployment.md) for
production guidance (secrets management, scaling, backups, and CI/CD).

---

## Testing

```bash
pytest                                   # run the suite
pytest --cov=src --cov-report=term-missing   # with coverage
```

The suite covers extraction, quality, transformation, loading, alerting,
metadata, and the orchestrator end to end (84%+ line coverage). Continuous
integration runs the same checks on every push (see `.github/workflows/ci.yml`).

---

## Project layout

```
automated-etl-data-quality-pipeline/
├── src/
│   ├── extractors/      # REST API, database, file extractors
│   ├── quality/         # expectation engine + built-in checks
│   ├── transformers/    # declarative transform engine
│   ├── loaders/         # warehouse loader + quarantine
│   ├── metadata/        # run history, metrics, lineage repository
│   ├── alerting/        # email/Slack channels + alert manager
│   ├── orchestration/   # PipelineRunner + retry logic
│   └── utils/           # settings, config loader, logging
├── flows/               # Prefect flow wrapper
├── config/              # sources, targets, quality_rules, pipelines
├── dashboard/           # Streamlit monitoring app
├── examples/            # runnable retail aggregation demo
├── db/                  # metadata schema DDL
├── tests/               # pytest suite
└── docs/                # architecture, configuration, deployment, guides
```

---

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration reference](docs/configuration.md)
- [Deployment guide](docs/deployment.md)
- [Adding a data source](docs/guides/adding-a-source.md)
- [Adding quality rules](docs/guides/adding-quality-rules.md)
- [Transformations](docs/guides/transformations.md)
- [Monitoring & alerting](docs/guides/monitoring-and-alerting.md)

---

## License

Released under the MIT License. See [`LICENSE`](LICENSE).
