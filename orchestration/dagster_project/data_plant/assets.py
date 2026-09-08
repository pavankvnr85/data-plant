"""
Dagster asset graph for the local batch lakehouse. Assets model the bronze
-> silver -> gold flow as data, which is what gives you lineage almost for
free -- Dagster already knows which asset depends on which.

Dagster's job here is orchestration and lineage, not computation: each asset
body calls straight into the same functions the manual local walkthrough
uses (ingestion/batch/autoloader_job.py, transform/run_dbt.py), which are
already wrapped with PipelineRunEmitter. So a Dagster-triggered run shows up
in metadata.pipeline_runs exactly like a manually-triggered one -- no
separate metadata path to keep in sync (see docs/build-plan.md Phase 3).

Local dev default (this file): calls the local Python entrypoints directly.
Cloud path: swap each asset body for a Databricks Jobs API call
(w.jobs.run_now(...).wait_get_run_job_terminated_or_skipped()) against jobs
that wrap ingestion/batch and transform/dbt_project on a real cluster; the
asset graph shape, schedule, and emitter integration don't change. See the
"Orchestrator" row in docs/interfaces.md.
"""
import sys
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, ScheduleDefinition, asset, define_asset_job

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(REPO_ROOT / "ingestion" / "batch"))
sys.path.append(str(REPO_ROOT / "transform"))

import autoloader_job  # noqa: E402
import run_dbt  # noqa: E402


@asset(group_name="bronze")
def orders_bronze(context: AssetExecutionContext) -> None:
    """Loads the 'orders' source into bronze (see ingestion/batch/sources.yaml)."""
    autoloader_job.run_source("orders")
    context.log.info("orders_bronze load complete")


@asset(group_name="bronze")
def customers_bronze(context: AssetExecutionContext) -> None:
    """Loads the 'customers' source into bronze (see ingestion/batch/sources.yaml)."""
    autoloader_job.run_source("customers")
    context.log.info("customers_bronze load complete")


@asset(deps=[orders_bronze, customers_bronze], group_name="silver")
def silver_models(context: AssetExecutionContext) -> None:
    """Builds transform/dbt_project/models/staging (stg_customers, stg_orders)."""
    run_dbt.run_dbt_job(job_name="dbt_run_silver", select="staging")
    context.log.info("silver_models build complete")


@asset(deps=[silver_models], group_name="gold")
def daily_revenue_gold(context: AssetExecutionContext) -> None:
    """Builds transform/dbt_project/models/marts/daily_revenue.sql."""
    run_dbt.run_dbt_job(job_name="dbt_run_gold", select="marts")
    context.log.info("daily_revenue_gold build complete")


daily_batch_job = define_asset_job(name="daily_batch_job", selection="*")

daily_schedule = ScheduleDefinition(
    job=daily_batch_job,
    cron_schedule="0 3 * * *",  # 03:00 UTC daily
)

defs = Definitions(
    assets=[orders_bronze, customers_bronze, silver_models, daily_revenue_gold],
    schedules=[daily_schedule],
)
