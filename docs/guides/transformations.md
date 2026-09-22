# Guide: transformations

Transformations run between quality validation and loading. They are declared as an
ordered list of steps on a pipeline stage, and applied by the `Transformer`
(`src/transformers/transformer.py`). Each step has a `type` and a `params` dict, and
steps are applied in order to a copy of the DataFrame (the input is never mutated).

## Declaring transforms

```yaml
stages:
  - name: transactions
    source: pos_transactions
    target: retail_warehouse
    transforms:
      - type: rename_lower
      - type: cast
        params:
          columns:
            quantity: int
            unit_price: float
            transaction_ts: datetime
      - type: derive
        params:
          column: revenue
          expression: "quantity * unit_price"
```

## Available steps

| Type | Params | Effect |
| --- | --- | --- |
| `rename` | `columns: {old: new}` | Rename specific columns |
| `rename_lower` | — | Lower-case + snake-case all column names |
| `cast` | `columns: {col: int\|float\|str\|datetime\|bool}` | Coerce dtypes (safe, error-tolerant) |
| `derive` | `column`, `expression` | Add a computed column via pandas `eval` |
| `drop_columns` | `columns: [..]` | Remove columns |
| `drop_duplicates` | `subset: [..]`, `keep: first\|last` | De-duplicate rows |
| `fillna` | `values: {col: v}` or `value: v` | Fill missing values |
| `filter` | `expression` | Keep rows matching a `query` expression |
| `add_constant` | `column`, `value` | Add a constant column |
| `aggregate` | `group_by: [..]`, `aggregations: {col: func}` | Group and aggregate |

## Notes on behaviour

- **`cast`** is defensive: numeric and datetime casts use error-tolerant coercion so
  malformed values become nulls rather than raising. Integer casts use pandas'
  nullable `Int64` so nulls survive.
- **`derive`** evaluates its expression with the Python engine, which supports
  nullable extension dtypes.
- **`rename_lower`** is a convenient first step to normalise inconsistent source
  headers (`Product ID` → `product_id`).
- Order matters. Cast before you derive if the derived expression needs numeric
  types; rename before you reference the new names downstream.

## Adding a new transform

Add an `_op_<type>` method to the `Transformer` class:

```python
def _op_uppercase(self, df, params):
    for col in params.get("columns", []):
        if col in df.columns:
            df[col] = df[col].str.upper()
    return df
```

It is picked up automatically by `type: uppercase`. Keep transforms deterministic
and side-effect free, and add a test under `tests/test_transformers.py`.
