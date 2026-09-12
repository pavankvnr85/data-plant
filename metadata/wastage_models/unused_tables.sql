-- Tables written by a pipeline but never read as an input by any other
-- pipeline run. Candidates for deprecation.
--
-- Local dev: there's no real query-history system table to check against
-- (see schema.sql's `table_reads` comment), so this uses the emitter's own
-- input_tables field as the documented fallback -- if any run ever
-- declared this table as one of its inputs, it isn't "unused."
--
-- Known limitation of that fallback: it's job-level lineage, not
-- column/table-level. A dbt run's own intermediate silver models are
-- consumed *within* the same job (by the same `dbt run` invocation) and
-- never separately declared as another job's input, so they can show up
-- here even though they're not actually unused -- real OpenLineage would
-- get this from dbt's manifest.json instead. Ad hoc BI/dashboard reads
-- (serving/dashboard.py) also aren't tracked, since a dashboard query
-- isn't a "job" that calls the emitter. The cloud path closes both gaps
-- by joining against `table_reads`, populated from Databricks' real
-- system.query.history.
--
-- Assumes a `pipeline_runs` relation is already registered on the calling
-- connection (see metadata/wastage_report.py).

WITH written_unnested AS (
    SELECT unnest(output_tables) AS table_name, ended_at
    FROM pipeline_runs
    WHERE status = 'success' AND output_tables IS NOT NULL
),
written AS (
    SELECT table_name, MAX(ended_at) AS last_written
    FROM written_unnested
    GROUP BY table_name
),
read AS (
    SELECT DISTINCT unnest(input_tables) AS table_name
    FROM pipeline_runs
    WHERE input_tables IS NOT NULL
)
SELECT
    w.table_name,
    w.last_written
FROM written w
LEFT JOIN read r ON w.table_name = r.table_name
WHERE r.table_name IS NULL
ORDER BY w.last_written DESC;
