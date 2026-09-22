# Configuration reference

Every aspect of the pipeline is driven by YAML files under `config/`. This document
is the complete reference for each configuration type.

```
config/
├── sources/         # SourceConfig     - where data comes from
├── targets/         # TargetConfig     - where clean data goes
├── quality_rules/   # QualityRuleSet   - what "good" looks like
└── pipelines/       # PipelineConfig   - how stages fit together
```

All files are validated against pydantic models when loaded, so a malformed config
fails fast with a clear error rather than deep inside a run.

## Environment interpolation

Any string value may reference an environment variable so secrets stay out of the
files:

```yaml
token: ${POS_API_TOKEN}            # required — empty string if unset
since: ${POS_SINCE:2024-01-01}     # with a default value
```

## Sources

A source file lives at `config/sources/<name>.yaml`.

```yaml
name: <string>          # must match the file name
type: rest_api | postgresql | mysql | csv | json | excel
description: <string>   # optional
options: { ... }        # type-specific (see below)
```

### REST API (`type: rest_api`)

```yaml
options:
  base_url: https://api.example.com
  endpoint: /v2/transactions
  method: GET
  auth:
    type: none | api_key | bearer | basic
    token: ${API_TOKEN}          # bearer
    api_key: ${API_KEY}          # api_key
    header: X-API-Key            # api_key header name
    username: ${USER}            # basic
    password: ${PASS}            # basic
  records_path: data             # dotted path to the list of records
  rate_limit_per_sec: 5          # client-side token-bucket
  pagination:
    type: none | page | offset | cursor
    page_param: page
    size_param: page_size
    page_size: 500
    start_page: 1
  max_retries: 4
  retry_backoff_seconds: 2.0
  max_pages: 1000
  timeout: 30
  params: { since: ${POS_SINCE:2024-01-01} }
  incremental_param: since       # query param used for watermark extraction
```

### Database (`type: postgresql` | `mysql`)

```yaml
options:
  host: ${INVENTORY_DB_HOST:localhost}
  port: 5432
  database: inventory
  user: ${INVENTORY_DB_USER:etl}
  password: ${INVENTORY_DB_PASSWORD}
  query: SELECT * FROM stock_levels
  # or: table: stock_levels
  incremental_column: last_updated   # optional
```

### Files (`type: csv` | `json` | `excel`)

```yaml
options:
  path: examples/retail_aggregation/data/products.csv
  # csv:   delimiter, encoding
  # json:  records_path (dotted path to the record list)
  # excel: sheet_name
```

## Targets

A target file lives at `config/targets/<name>.yaml`.

```yaml
name: retail_warehouse
type: postgresql | mysql | sqlite
connection:
  # postgres/mysql
  host: ${WAREHOUSE_DB_HOST:localhost}
  port: 5432
  database: retail_warehouse
  user: ${WAREHOUSE_DB_USER:etl}
  password: ${WAREHOUSE_DB_PASSWORD}
  # sqlite
  path: examples/retail_aggregation/data/retail_warehouse.db
options:
  schema: public          # optional target schema
```

## Quality rule sets

A rule set lives at `config/quality_rules/<name>.yaml` and is a named collection of
expectations applied to one dataset.

```yaml
name: pos_transactions
dataset: pos_transactions
expectations:
  - name: <string>              # unique, human-readable
    type: <expectation type>    # see table below
    columns: [ ... ]            # columns the check applies to
    severity: error | warning   # error blocks + quarantines; warning is advisory
    params: { ... }             # type-specific
```

### Expectation types

| Type | Key params | Checks |
| --- | --- | --- |
| `schema` | `columns: {name: type}`, `allow_extra` | Column presence and dtype |
| `not_null` / `completeness` | `max_null_rate` | Null rate within threshold |
| `unique` / `primary_key` | `primary_key: bool` | No duplicates (+ non-null for keys) |
| `range` | `min`, `max`, `max_violation_rate` | Numeric bounds |
| `allowed_values` / `categorical` | `values`, `max_violation_rate` | Values in permitted set |
| `outliers` | `method` (iqr\|zscore), `factor`, `max_outlier_rate` | Statistical outliers |
| `freshness` | `max_age_hours`, `reference` | Newest record recent enough |
| `custom` / `business_rule` | `expression`, `max_violation_rate` | Per-row boolean predicate |

`expression` uses pandas `eval` syntax, e.g.
`"abs(total_amount - (quantity * unit_price)) < 0.01"`.

## Pipelines

A pipeline lives at `config/pipelines/<name>.yaml`.

```yaml
name: retail_aggregation
description: <string>
schedule: "0 2 * * *"          # cron, used by the Prefect deployment
max_retries: 2                  # per-stage retry attempts
retry_backoff_seconds: 2.0
stages:
  - name: <string>
    source: <source name>
    target: <target name>
    target_table: <table>        # defaults to the stage name
    load_strategy: full | incremental
    incremental_key: <column>    # required for incremental
    quality_rule_set: <rule set name>   # optional
    depends_on: [ <stage names> ]       # optional
    transforms: [ ... ]                 # optional; see transformations guide
```

Stages are executed in dependency order. A stage whose dependencies did not
succeed is skipped and reported as such.

## Settings (environment)

Runtime settings are read from environment variables prefixed with `ETL_` (a local
`.env` is loaded automatically for development). See [`.env.example`](../.env.example)
for the full list, including database connections, batch sizing, retries, and
alerting credentials.
