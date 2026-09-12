-- Tables written by a pipeline but never read -- either as another job's
-- declared input_tables, or (more precisely) via table_reads, populated
-- from dbt's real per-model dependency graph (transform/run_dbt.py parses
-- manifest.json) and from serving/dashboard.py recording when a human
-- actually views a gold table. Candidates for deprecation.
--
-- This used to only check input_tables, which is job-level lineage: it
-- couldn't see that dbt's own daily_revenue model reads
-- stg_orders/stg_customers internally within a single `dbt run`
-- invocation, so those tables were always wrongly flagged as unused even
-- though they plainly aren't. table_reads closes that gap with dbt's
-- actual manifest.json-derived dependency graph -- the same source real
-- OpenLineage would use -- and additionally means a gold table someone is
-- genuinely looking at via the dashboard doesn't get flagged either. The
-- cloud path would populate table_reads from Databricks' real
-- system.query.history instead, catching ad hoc BI reads this local
-- version can only catch for the one dashboard it knows about.
--
-- Assumes `pipeline_runs` and `table_reads` relations are already
-- registered on the calling connection (see metadata/wastage_report.py).

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
    UNION
    SELECT DISTINCT table_name
    FROM table_reads
)
SELECT
    w.table_name,
    w.last_written
FROM written w
LEFT JOIN read r ON w.table_name = r.table_name
WHERE r.table_name IS NULL
ORDER BY w.last_written DESC;
