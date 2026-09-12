-- Every job in the platform (batch, streaming, dbt) writes one row per run
-- into this table. It lives in the same lakehouse as everything it
-- describes -- the platform observes itself using its own architecture.
--
-- Local dev (Phase 1, no Databricks/Unity Catalog) does not run this DDL --
-- its source of truth is the PIPELINE_RUN_SCHEMA constant in
-- metadata/openlineage_emitter.py. Keep the two in sync by hand.

CREATE TABLE IF NOT EXISTS data_plant.metadata.pipeline_runs (
    run_id            STRING,
    job_name          STRING,
    job_type          STRING,      -- 'batch_ingestion' | 'streaming' | 'dbt' | 'dagster_asset'
    status            STRING,      -- 'running' | 'success' | 'failed'
    started_at        TIMESTAMP,
    ended_at          TIMESTAMP,
    duration_seconds  DOUBLE,
    rows_read         BIGINT,
    rows_written       BIGINT,
    bytes_scanned     BIGINT,
    input_tables      ARRAY<STRING>,
    output_tables     ARRAY<STRING>,
    error_message     STRING,
    -- rough cost proxy: cluster size class x duration, refined later once
    -- real DBU billing data is available via the Databricks billing API
    estimated_cost_usd DOUBLE
)
USING DELTA
PARTITIONED BY (job_type);

-- Separate, append-only log of table reads, used by the wastage models to
-- find tables that are written but never queried downstream. Cloud path:
-- populate from query history (Databricks system tables:
-- system.query.history), catching every ad hoc BI read too. Local dev
-- (metadata/table_reads.py) populates it from two narrower but real
-- sources instead: dbt's own manifest.json (transform/run_dbt.py records
-- each model's actual dependencies -- what real OpenLineage would use
-- with dbt) and serving/dashboard.py recording when it loads a gold
-- table. The emitter's input_tables field remains a job-level fallback
-- on top of this (see unused_tables.sql).
CREATE TABLE IF NOT EXISTS data_plant.metadata.table_reads (
    table_name  STRING,
    read_at     TIMESTAMP,
    reader_job  STRING
)
USING DELTA
PARTITIONED BY (table_name);
