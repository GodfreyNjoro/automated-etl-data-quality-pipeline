# Guide: adding quality rules

Quality rules are declared per dataset in `config/quality_rules/<name>.yaml` and
attached to a pipeline stage via `quality_rule_set`. This guide shows how to build
a rule set and how to add a custom check.

## Anatomy of a rule set

```yaml
name: customers          # matches the file name
dataset: customers
expectations:
  - name: email_present
    type: not_null
    columns: [email]
    severity: error
  - name: email_format
    type: business_rule
    severity: warning
    params:
      expression: "email.str.contains('@')"
```

Each expectation has:

- **`name`** — a unique, readable label (shown in alerts and the dashboard).
- **`type`** — one of the built-in checks (see the table below).
- **`columns`** — the columns it applies to.
- **`severity`** — `error` fails the check and quarantines offending rows;
  `warning` is recorded and trended but never blocks the run.
- **`params`** — type-specific settings.

## Choosing a check

| Goal | Type | Example params |
| --- | --- | --- |
| Columns exist and are typed | `schema` | `columns: {id: string, price: float}` |
| No missing values | `not_null` | `max_null_rate: 0.0` |
| No duplicate keys | `primary_key` | `primary_key: true` |
| Values within bounds | `range` | `min: 0, max: 10000` |
| Values from a fixed set | `allowed_values` | `values: [card, cash, mobile]` |
| Detect statistical outliers | `outliers` | `method: iqr, factor: 1.5` |
| Data is recent | `freshness` | `max_age_hours: 168` |
| Any custom logic | `business_rule` | `expression: "total >= 0"` |

### Tolerances

Most checks accept a `max_violation_rate` (or `max_null_rate` / `max_outlier_rate`)
so you can allow a small, known fraction of imperfect rows without failing the whole
dataset. Set it to `0.0` for zero tolerance.

## Scoring

A dataset's quality score is the fraction of **error-severity** expectations that
passed. Warnings are tracked for trending but do not affect the score or block the
load. Scores are stored per run so the dashboard can chart quality over time.

## Adding a custom expectation in code

If the built-ins are not enough, register a new check at runtime:

```python
from src.quality.engine import register_expectation
from src.quality.results import ExpectationResult

def expect_positive_margin(df, cfg):
    bad = (df["price"] <= df["cost"]).sum()
    return ExpectationResult(
        name=cfg.name,
        type=cfg.type,
        success=bad == 0,
        severity=cfg.severity,
        columns=cfg.columns,
        observed={"violations": int(bad)},
        message="Margins positive" if bad == 0 else f"{bad} non-positive margins",
    )

register_expectation("positive_margin", expect_positive_margin)
```

Then reference it by `type: positive_margin` in a rule set. For a permanent check,
add the function to `src/quality/expectations.py` and register it in
`BUILTIN_EXPECTATIONS`, with a test under `tests/test_quality.py`.
