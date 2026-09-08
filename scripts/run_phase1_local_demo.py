"""
One-command reproduction of the Phase 1 + Phase 2 local-dev demo artifacts:
a gold table, a screenshot-able proof of Delta time travel, and real rows in
metadata.pipeline_runs from every job that just ran.

Seeds data twice (so bronze gets two genuinely different Delta versions),
runs dbt through transform/run_dbt.py (not bare `dbt run` -- that wrapper is
what makes the dbt run itself show up in pipeline_runs), then prints the
bronze history, a sample of the gold table, and the metadata table.

Run from the repo root: python scripts/run_phase1_local_demo.py
"""
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except AttributeError:
    pass

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ingestion" / "batch"))

import autoloader_job  # noqa: E402
import seed_data  # noqa: E402

from deltalake import DeltaTable  # noqa: E402
import duckdb  # noqa: E402


def main():
    print("== seeding batch 1 ==")
    seed_data.main()
    print("\n== bronze ingestion run 1 (Delta version 0) ==")
    autoloader_job.main()

    print("\n== seeding batch 2 ==")
    seed_data.main()
    print("\n== bronze ingestion run 2 (Delta version 1) ==")
    autoloader_job.main()

    print("\n== dbt run (silver + gold), instrumented via transform/run_dbt.py ==")
    subprocess.run([sys.executable, str(REPO_ROOT / "transform" / "run_dbt.py")], check=True)

    print("\n== time travel demo: lakehouse/bronze/orders ==")
    orders_path = str(REPO_ROOT / "lakehouse" / "bronze" / "orders")
    dt = DeltaTable(orders_path)
    print(dt.history())
    v0_rows = len(DeltaTable(orders_path, version=0).to_pandas())
    latest_rows = len(dt.to_pandas())
    print(f"orders rows at version 0: {v0_rows}")
    print(f"orders rows at latest version: {latest_rows}")

    print("\n== gold table sample: main_gold.daily_revenue ==")
    con = duckdb.connect(str(REPO_ROOT / "lakehouse" / "warehouse.duckdb"))
    print(con.sql("select * from main_gold.daily_revenue limit 10"))

    print("\n== pipeline metadata: metadata.pipeline_runs (Phase 2) ==")
    pipeline_runs_path = (REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs").as_posix()
    print(
        con.sql(
            f"""
            select job_name, job_type, status, started_at, duration_seconds,
                   rows_written, output_table
            from delta_scan('{pipeline_runs_path}')
            order by started_at
            """
        )
    )


if __name__ == "__main__":
    main()
