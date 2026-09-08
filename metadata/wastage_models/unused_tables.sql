-- Tables that pipelines have written to in the last 30 days but that no
-- job has read from in the same window. Candidates for deprecation.
-- Run as a dbt model (transform/dbt_project/models/marts) or standalone SQL.

WITH written AS (
    SELECT DISTINCT output_table AS table_name, MAX(ended_at) AS last_written
    FROM data_plant.metadata.pipeline_runs
    WHERE status = 'success'
      AND output_table IS NOT NULL
      AND ended_at >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY output_table
),
read AS (
    SELECT DISTINCT table_name, MAX(read_at) AS last_read
    FROM data_plant.metadata.table_reads
    WHERE read_at >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY table_name
)
SELECT
    w.table_name,
    w.last_written,
    r.last_read,
    CASE WHEN r.table_name IS NULL THEN true ELSE false END AS is_unused
FROM written w
LEFT JOIN read r ON w.table_name = r.table_name
WHERE r.table_name IS NULL
ORDER BY w.last_written DESC;
