# Guide: monitoring & alerting

The pipeline is built to be observed. Every run records what it did and how healthy
the data was; the dashboard surfaces that history, and the alerting layer pushes
failures to your team.

## The monitoring dashboard

Launch it with:

```bash
streamlit run dashboard/app.py
```

It reads from the metadata database when configured (`ETL_METADATA_DB_*`) and falls
back to the example's `last_run.json` so it works immediately after running the
example. The dashboard provides:

- **Run history** — status, duration, and row counts for every run, with filters by
  pipeline, status, and date range.
- **Quality trends** — quality scores charted over time per dataset, so gradual
  degradation is visible before it becomes a crisis.
- **Data volume & freshness** — rows extracted/loaded/quarantined per run, and the
  age of the newest records.
- **Failed & quarantined records** — counts and reasons, so bad data can be found
  and corrected.
- **Lineage** — which sources feed which target tables, per run.
- **Alerts** — recent alerts and their suppression state.

## What gets recorded

For every run the `MetadataRepository` persists: the run itself (status, timing,
volumes), per-stage results, quality metrics per expectation, observed schemas,
quarantine events, column-level lineage, and error logs. This is the raw material
for both the dashboard and any downstream audit.

## Alerting

Alerts are raised automatically when:

- an **error-severity** quality expectation fails (with the failing expectations
  and the dataset's score), or
- a **stage or run fails** after exhausting retries.

### Channels

Configure one or both channels via environment variables:

```bash
# Email (SMTP)
ETL_ALERT_EMAIL_ENABLED=true
ETL_SMTP_HOST=smtp.example.com
ETL_SMTP_PORT=587
ETL_SMTP_USER=alerts@example.com
ETL_SMTP_PASSWORD=...
ETL_ALERT_EMAIL_FROM=etl-alerts@example.com
ETL_ALERT_EMAIL_TO=ops@example.com

# Slack (incoming webhook)
ETL_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

If a channel is not configured it is simply skipped — alerting degrades gracefully.

### Suppression & deduplication

Every alert has a stable **key** (for example `quality:retail_aggregation:products`).
Once an alert with a given key fires, further alerts with the same key are suppressed
for `ETL_ALERT_SUPPRESSION_MINUTES` (default 30). This prevents a persistent problem
from paging your team every run while still guaranteeing the first occurrence gets
through.

### Severity & thresholds

Control what pages you by setting expectation `severity` in the quality rule sets:

- `error` — blocks the load, quarantines rows, and raises an alert.
- `warning` — recorded and trended in the dashboard, but never blocks or pages.

Tune per-check tolerances (`max_violation_rate`, `max_null_rate`, etc.) so expected,
harmless imperfections do not generate noise, and reserve `error` severity for the
defects that genuinely must not reach the warehouse.

## Recommended operating rhythm

1. Watch the **quality-trend** charts for slow drift, not just hard failures.
2. Triage **quarantined records** regularly; fix issues at the source.
3. Review **alert volume** and adjust severities/thresholds to keep signal high.
4. Back up the **metadata database** so history and trends survive.
