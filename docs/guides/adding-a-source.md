# Guide: adding a data source

Adding a new source is a configuration task — no Python required unless the source
speaks a protocol the framework does not yet support.

## 1. Create the source config

Add `config/sources/<name>.yaml`. The `name` field must match the file name. Pick
the `type` and fill in `options`. See the
[configuration reference](../configuration.md#sources) for every option.

### Example: a new CRM REST API

```yaml
name: crm_api
type: rest_api
description: Customer records from the CRM.
options:
  base_url: https://crm.example.com
  endpoint: /api/customers
  auth:
    type: api_key
    api_key: ${CRM_API_KEY}
    header: X-API-Key
  records_path: results
  pagination:
    type: cursor
    cursor_param: next_cursor
    page_size: 200
  rate_limit_per_sec: 10
```

### Example: a new database source

```yaml
name: inventory_db
type: postgresql
options:
  host: ${INVENTORY_DB_HOST:localhost}
  database: inventory
  user: ${INVENTORY_DB_USER:etl}
  password: ${INVENTORY_DB_PASSWORD}
  query: SELECT sku, quantity_on_hand, reorder_level, last_counted FROM stock
  incremental_column: last_counted
```

## 2. Add any secrets

Reference secrets with `${VAR}` and add the variables to your environment /
`.env`. Never hard-code credentials in the YAML.

## 3. Wire it into a pipeline

Point a pipeline stage at the source:

```yaml
stages:
  - name: customers
    source: crm_api
    target: retail_warehouse
    target_table: dim_customer
    quality_rule_set: customers
```

## 4. Test the extraction

Extract in isolation before running the whole pipeline:

```python
from src.utils.config_loader import load_source
from src.extractors import get_extractor

source = load_source("crm_api")
result = get_extractor(source).extract()
print(result.data.head())
print(len(result.data), "rows")
```

## Supporting a brand-new source type

If your source is not REST/DB/file, implement a new extractor:

1. Subclass `BaseExtractor` in `src/extractors/` and implement `extract()`,
   returning an `ExtractionResult`.
2. Register it in the factory in `src/extractors/__init__.py` under a new `type`
   string.
3. Add a test under `tests/`.

Keep the extractor focused on *retrieval*; validation and transformation belong to
their own stages.
