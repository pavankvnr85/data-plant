"""
Phase 7's demo artifact: a single report listing real "wasteful" pipelines
found in pipeline_runs, each with a plain-English reason. Runs all four
checks from docs/build-plan.md's Phase 7:

  1. unused_tables.sql       -- written but never read by another job
  2. cost_and_runtime_trend.sql -- rising runtime + cost per job
  3. near-duplicate output schemas -- column-overlap between distinct
     output tables, done in Python (below) rather than SQL, since it needs
     to introspect each table's actual schema, and our outputs are a mix
     of DuckDB-native tables (silver/gold) and standalone Delta paths
     (bronze, streaming gold) with different introspection mechanisms
  4. cost is folded into check 2 (same file, same underlying pipeline_runs
     rows) rather than a separate query

Local dev: everything reads from the local pipeline_runs Delta table
(lakehouse/metadata/pipeline_runs), registered as a `pipeline_runs` view on
an in-memory DuckDB connection via delta_scan() -- the same pattern
serving/dashboard.py uses for the streaming gold table, so this never
touches warehouse.duckdb's file lock either.

Run: python metadata/wastage_report.py
"""
import itertools
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_RUNS_PATH = (REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs").as_posix()
TABLE_READS_PATH = REPO_ROOT / "lakehouse" / "metadata" / "table_reads"
WAREHOUSE_PATH = REPO_ROOT / "lakehouse" / "warehouse.duckdb"
WASTAGE_MODELS_DIR = Path(__file__).parent / "wastage_models"

RUNTIME_TREND_THRESHOLD = 1.2  # flag jobs >=20% slower recently than before
SCHEMA_OVERLAP_THRESHOLD = 0.5  # flag output pairs sharing >=50% of columns


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql(f"create view pipeline_runs as select * from delta_scan('{PIPELINE_RUNS_PATH}')")
    if TABLE_READS_PATH.exists():
        con.sql(f"create view table_reads as select * from delta_scan('{TABLE_READS_PATH.as_posix()}')")
    else:
        # No dbt run has happened yet to populate it -- an empty relation
        # with the right columns so unused_tables.sql's UNION still works.
        con.sql(
            "create view table_reads as select "
            "cast(null as varchar) as table_name, "
            "cast(null as timestamp) as read_at, "
            "cast(null as varchar) as reader_job "
            "where false"
        )
    return con


def _run_sql_file(con: duckdb.DuckDBPyConnection, name: str):
    return con.sql((WASTAGE_MODELS_DIR / name).read_text())


def check_unused_tables(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = _run_sql_file(con, "unused_tables.sql").df().to_dict(orient="records")
    return [
        {
            "finding": row["table_name"],
            "reason": f"written by a pipeline (last: {row['last_written']}) but never read as an input by any other tracked run",
        }
        for row in rows
    ]


def check_cost_and_runtime_trend(con: duckdb.DuckDBPyConnection) -> list[dict]:
    df = _run_sql_file(con, "cost_and_runtime_trend.sql").df()
    findings = []
    for _, row in df.iterrows():
        ratio = row["runtime_trend_ratio"]
        if ratio is not None and ratio >= RUNTIME_TREND_THRESHOLD:
            pct = round((ratio - 1) * 100)
            findings.append(
                {
                    "finding": row["job_name"],
                    "reason": (
                        f"runtime trending up: recent runs average "
                        f"{row['avg_recent_duration']:.2f}s vs {row['avg_prior_duration']:.2f}s "
                        f"before ({pct:+d}%), total est. cost so far ${row['total_estimated_cost_usd']:.4f}"
                    ),
                }
            )
    return findings


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str] | None:
    """Best-effort column lookup across the two storage shapes local dev
    actually has: DuckDB-native relations (main_silver.*, main_gold.*) via
    warehouse.duckdb, and standalone Delta paths (bronze, streaming gold)
    via delta_scan(). Returns None if neither resolves (e.g. a raw-file
    input path like seed_data/orders/, which isn't a table at all)."""
    if "." in table_name and WAREHOUSE_PATH.exists():
        try:
            with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as wcon:
                cols = wcon.sql(f"select * from {table_name} limit 0").columns
                return set(cols)
        except duckdb.Error:
            pass
    try:
        cols = con.sql(f"select * from delta_scan('{table_name}') limit 0").columns
        return set(cols)
    except duckdb.Error:
        return None


def check_duplicate_schemas(con: duckdb.DuckDBPyConnection) -> list[dict]:
    output_tables = (
        con.sql(
            "select distinct unnest(output_tables) as t from pipeline_runs "
            "where output_tables is not null"
        )
        .df()["t"]
        .tolist()
    )
    schemas = {t: _table_columns(con, t) for t in output_tables}
    schemas = {t: cols for t, cols in schemas.items() if cols}

    findings = []
    for table_a, table_b in itertools.combinations(schemas, 2):
        cols_a, cols_b = schemas[table_a], schemas[table_b]
        overlap = len(cols_a & cols_b) / len(cols_a | cols_b)
        if overlap >= SCHEMA_OVERLAP_THRESHOLD:
            findings.append(
                {
                    "finding": f"{table_a} <-> {table_b}",
                    "reason": f"{overlap:.0%} of columns overlap ({sorted(cols_a & cols_b)}) -- possible redundant pipelines",
                }
            )
    return findings


def main():
    con = _connect()

    sections = [
        ("Unused tables", check_unused_tables(con)),
        ("Rising cost / runtime trend", check_cost_and_runtime_trend(con)),
        ("Near-duplicate output schemas", check_duplicate_schemas(con)),
    ]

    print("=== Wastage report ===\n")
    total = 0
    for title, findings in sections:
        print(f"-- {title} ({len(findings)}) --")
        if not findings:
            print("  (none found)")
        for f in findings:
            print(f"  - {f['finding']}: {f['reason']}")
        print()
        total += len(findings)

    print(f"{total} total finding(s) across {len(sections)} checks.")


if __name__ == "__main__":
    main()
