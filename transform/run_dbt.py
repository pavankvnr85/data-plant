"""
Thin wrapper around `dbt run` that reports one PipelineRun to the shared
metadata emitter, the same way ingestion/batch/autoloader_job.py does for
bronze loads. This is Phase 2's actual gap: bronze ingestion already wrote
rows into pipeline_runs, but dbt itself ran invisibly. Invoking dbt through
this script (instead of the bare `dbt run` CLI) is what makes the transform
layer show up in pipeline_runs too.

Uses dbt-core's programmatic `dbtRunner` API rather than a dbt on-run-end
hook: dbt hooks only run SQL against the adapter connection, and there's no
clean way to reach a Python emitter from inside one. `dbtRunner` gives back
a typed result object (per-model status/timing) we can feed straight into
the emitter instead.

Run from anywhere: python transform/run_dbt.py
"""
import os
import sys
from pathlib import Path
from typing import Optional

import duckdb
from dbt.cli.main import dbtRunner

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "transform" / "dbt_project"
WAREHOUSE_PATH = REPO_ROOT / "lakehouse" / "warehouse.duckdb"

sys.path.append(str(REPO_ROOT / "metadata"))
sys.path.append(str(REPO_ROOT / "ingestion" / "batch"))
from openlineage_emitter import PipelineRunEmitter  # noqa: E402
from autoloader_job import load_sources  # noqa: E402


def _bronze_table_paths() -> list[str]:
    """The same absolute paths autoloader_job.py records as each source's
    output_tables -- computed from sources.yaml rather than hardcoded, so
    this can't silently drift out of sync with what bronze ingestion
    actually wrote (it used to: a hardcoded relative-path string here never
    matched the absolute path recorded on the write side, so dbt's declared
    "input" never matched bronze's declared "output" and the Phase 7
    unused-tables check flagged bronze as unused even though dbt reads it
    every run)."""
    return [str(REPO_ROOT / s["bronze_table"]) for s in load_sources()]


def count_rows(relations: list[tuple[str, str]]) -> int:
    """Sum row counts across the (schema, name) relations dbt just built.

    dbt-duckdb's adapter_response doesn't report rows_affected for view/table
    creates, so row counts are read back directly from the warehouse instead.
    """
    total = 0
    with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as con:
        for schema, name in relations:
            total += con.sql(f'select count(*) from "{schema}"."{name}"').fetchone()[0]
    return total


def _invoke_dbt(select: Optional[str]) -> tuple[int, list[str]]:
    # dbt resolves profiles.yml's relative warehouse path -- and the bronze
    # sources' relative delta_scan() path -- against the current working
    # directory, so both the dbt run and the row-count query below have to
    # happen from inside the project dir (matches `cd transform/dbt_project
    # && dbt run`) rather than via --project-dir.
    args = ["run"] + (["--select", select] if select else [])
    original_cwd = os.getcwd()
    os.chdir(DBT_PROJECT_DIR)
    try:
        result = dbtRunner().invoke(args)
        if not result.success:
            raise RuntimeError(f"dbt run failed: {result.exception}")

        relations = [(r.node.schema, r.node.name) for r in result.result.results]
        rows_written = count_rows(relations)
    finally:
        os.chdir(original_cwd)

    output_tables = [f"{schema}.{name}" for schema, name in relations]
    return rows_written, output_tables


def run_dbt_job(
    job_name: str = "dbt_run",
    select: Optional[str] = None,
    emitter: Optional[PipelineRunEmitter] = None,
) -> None:
    """Run dbt (optionally scoped to a subdirectory via `select`, e.g.
    "staging" or "marts") and emit one PipelineRun. Used both by main()'s
    full-project run and by Dagster's per-layer assets (silver_models,
    daily_revenue_gold), so both paths share the same emitter contract."""
    if emitter is None:
        emitter = PipelineRunEmitter(
            metadata_table=str(REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs")
        )
    run = emitter.start_run(
        job_name=job_name,
        job_type="dbt",
        input_tables=_bronze_table_paths(),
    )

    try:
        rows_written, output_tables = _invoke_dbt(select)
    except Exception as e:
        emitter.fail_run(job_name=job_name, error=str(e), run=run)
        raise

    emitter.end_run(
        run,
        status="success",
        rows_written=rows_written,
        output_tables=output_tables,
    )
    print(f"{job_name} OK -- {rows_written} total rows -> {', '.join(output_tables)}")


def main():
    run_dbt_job()


if __name__ == "__main__":
    main()
