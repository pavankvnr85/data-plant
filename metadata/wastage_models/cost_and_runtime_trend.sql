-- Per-job cost and a simple runtime trend flag: compares each job's average
-- duration over its last 5 runs against its average over the 5 before
-- that. A ratio meaningfully above 1.0 means the job is getting
-- slower/more expensive over time -- a real, cheap signal without needing
-- ML. estimated_cost_usd is a local-dev proxy (duration x a nominal hourly
-- rate -- see openlineage_emitter.py's LOCAL_DEV_HOURLY_RATE_USD), not a
-- real cloud bill; the trend itself is computed off actual recorded
-- durations, so it's a genuine signal even though the dollar figure isn't.
--
-- Assumes a `pipeline_runs` relation is already registered on the calling
-- connection (see metadata/wastage_report.py).

WITH ranked AS (
    SELECT
        job_name,
        run_id,
        duration_seconds,
        estimated_cost_usd,
        ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY ended_at DESC) AS recency_rank
    FROM pipeline_runs
    WHERE status = 'success'
),
recent AS (
    SELECT job_name, AVG(duration_seconds) AS avg_recent_duration
    FROM ranked WHERE recency_rank <= 5
    GROUP BY job_name
),
prior AS (
    SELECT job_name, AVG(duration_seconds) AS avg_prior_duration
    FROM ranked WHERE recency_rank BETWEEN 6 AND 10
    GROUP BY job_name
),
cost AS (
    SELECT job_name, SUM(estimated_cost_usd) AS total_estimated_cost_usd
    FROM pipeline_runs
    WHERE status = 'success'
    GROUP BY job_name
)
SELECT
    r.job_name,
    c.total_estimated_cost_usd,
    r.avg_recent_duration,
    p.avg_prior_duration,
    ROUND(r.avg_recent_duration / NULLIF(p.avg_prior_duration, 0), 2) AS runtime_trend_ratio
FROM recent r
LEFT JOIN prior p ON r.job_name = p.job_name
LEFT JOIN cost c ON r.job_name = c.job_name
ORDER BY runtime_trend_ratio DESC NULLS LAST;
