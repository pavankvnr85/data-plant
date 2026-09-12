"""
Append-only log of which job (or dashboard view) actually read which
table -- closes the gap `metadata/wastage_models/unused_tables.sql`'s
`input_tables`-fallback can't: job-level lineage doesn't know that dbt's
own `daily_revenue` model reads `stg_orders`/`stg_customers` internally
within a single `dbt run` invocation, and nothing marks a table as "read"
when a human looks at it via the dashboard rather than through a tracked
pipeline run.

Local dev populates this two ways (see call sites):
  - transform/run_dbt.py parses dbt's own manifest.json after each run to
    get the real per-model dependency graph, and records one row per
    model-to-model or model-to-source edge -- this is exactly what real
    OpenLineage does with dbt (see docs/interfaces.md's "Wastage
    detection" row for the cloud-path equivalent, populated there from
    Databricks' system.query.history instead).
  - serving/dashboard.py records a read each time it actually loads a
    gold table, so a table someone is genuinely looking at doesn't get
    flagged as unused just because no pipeline re-reads it.

See metadata/schema.sql for the cloud-DDL equivalent of the schema below.
"""
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

REPO_ROOT = Path(__file__).resolve().parents[1]
TABLE_READS_PATH = str(REPO_ROOT / "lakehouse" / "metadata" / "table_reads")

TABLE_READS_SCHEMA = pa.schema(
    [
        pa.field("table_name", pa.string()),
        pa.field("read_at", pa.timestamp("us", tz="UTC")),
        pa.field("reader_job", pa.string()),
    ]
)


def record_table_read(table_name: str, reader_job: str) -> None:
    row = {
        "table_name": table_name,
        "read_at": pd.Timestamp.now(tz="UTC"),
        "reader_job": reader_job,
    }
    table = pa.Table.from_pylist([row], schema=TABLE_READS_SCHEMA)
    write_deltalake(TABLE_READS_PATH, table, mode="append")
