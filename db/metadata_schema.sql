-- =====================================================================
-- ETL Metadata & Lineage database schema (PostgreSQL)
-- ---------------------------------------------------------------------
-- Tracks pipeline execution history, data quality metrics, source-to-target
-- lineage, schema evolution, error logs and quarantined-record summaries.
-- =====================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        VARCHAR(64) PRIMARY KEY,
    pipeline_name VARCHAR(120) NOT NULL,
    status        VARCHAR(20)  NOT NULL,   -- running | success | failed | partial
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    duration_seconds NUMERIC(12, 3),
    rows_extracted   BIGINT DEFAULT 0,
    rows_loaded      BIGINT DEFAULT 0,
    rows_quarantined BIGINT DEFAULT 0,
    triggered_by  VARCHAR(60) DEFAULT 'manual',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_time
    ON pipeline_runs (pipeline_name, started_at DESC);

CREATE TABLE IF NOT EXISTS stage_runs (
    id            BIGSERIAL PRIMARY KEY,
    run_id        VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    stage_name    VARCHAR(120) NOT NULL,
    source_name   VARCHAR(120),
    target_name   VARCHAR(120),
    target_table  VARCHAR(120),
    load_strategy VARCHAR(20),
    status        VARCHAR(20) NOT NULL,
    rows_extracted   BIGINT DEFAULT 0,
    rows_loaded      BIGINT DEFAULT 0,
    rows_quarantined BIGINT DEFAULT 0,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_stage_runs_run ON stage_runs (run_id);

CREATE TABLE IF NOT EXISTS quality_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    stage_name    VARCHAR(120) NOT NULL,
    dataset       VARCHAR(120),
    rule_set      VARCHAR(120),
    expectation_name VARCHAR(160) NOT NULL,
    expectation_type VARCHAR(60)  NOT NULL,
    severity      VARCHAR(20),
    success       BOOLEAN      NOT NULL,
    observed      JSONB,
    message       TEXT,
    recorded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_run ON quality_metrics (run_id);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_dataset_time
    ON quality_metrics (dataset, recorded_at DESC);

CREATE TABLE IF NOT EXISTS quality_scores (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(64) NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    stage_name  VARCHAR(120) NOT NULL,
    dataset     VARCHAR(120),
    rule_set    VARCHAR(120),
    total       INTEGER,
    passed      INTEGER,
    failed      INTEGER,
    score       NUMERIC(6, 4),
    row_count   BIGINT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quality_scores_dataset_time
    ON quality_scores (dataset, recorded_at DESC);

-- Source-to-target lineage mapping captured per run.
CREATE TABLE IF NOT EXISTS lineage (
    id            BIGSERIAL PRIMARY KEY,
    run_id        VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(120) NOT NULL,
    stage_name    VARCHAR(120) NOT NULL,
    source_name   VARCHAR(120) NOT NULL,
    source_type   VARCHAR(60),
    target_name   VARCHAR(120) NOT NULL,
    target_table  VARCHAR(120) NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Schema evolution history: snapshot of a dataset's columns/types per run.
CREATE TABLE IF NOT EXISTS schema_history (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    dataset     VARCHAR(120) NOT NULL,
    schema_snapshot JSONB NOT NULL,   -- {column: dtype}
    change_type VARCHAR(20),          -- initial | added | removed | changed | unchanged
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_history_dataset
    ON schema_history (dataset, recorded_at DESC);

-- Error logs and quarantine summaries.
CREATE TABLE IF NOT EXISTS error_logs (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    stage_name  VARCHAR(120),
    level       VARCHAR(20) NOT NULL,
    message     TEXT NOT NULL,
    context     JSONB,
    logged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quarantine_log (
    id           BIGSERIAL PRIMARY KEY,
    run_id       VARCHAR(64) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(120),
    stage_name   VARCHAR(120),
    row_count    BIGINT,
    reason       VARCHAR(120),
    file_path    TEXT,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Alert history for suppression / deduplication.
CREATE TABLE IF NOT EXISTS alert_log (
    id            BIGSERIAL PRIMARY KEY,
    alert_key     VARCHAR(200) NOT NULL,
    channel       VARCHAR(40) NOT NULL,
    severity      VARCHAR(20),
    subject       TEXT,
    body          TEXT,
    sent          BOOLEAN NOT NULL DEFAULT TRUE,
    suppressed    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_log_key_time
    ON alert_log (alert_key, created_at DESC);
