"""
One-command reproduction of the Phase 4 streaming demo artifact: starts the
clickstream producer in the background, runs the windowed consumer job
(structured_streaming_job.py) in the foreground for a bounded window, then
prints the gold table it built and the metadata trail it left behind.

Prerequisite: `docker compose up -d` (Redpanda on localhost:19092) must
already be running -- this script doesn't manage Docker itself.

Run from the repo root: python scripts/run_phase4_streaming_demo.py
"""
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except AttributeError:
    pass

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ingestion" / "streaming"))

import structured_streaming_job  # noqa: E402
import duckdb  # noqa: E402

DEMO_RUNTIME_SECONDS = 90


def main():
    print("== starting clickstream producer in the background ==")
    producer = subprocess.Popen(
        [
            sys.executable,
            str(REPO_ROOT / "ingestion" / "streaming" / "kafka_producer_clickstream.py"),
        ]
    )
    try:
        print(f"\n== consuming + windowing for {DEMO_RUNTIME_SECONDS}s ==")
        structured_streaming_job.run_streaming_job(max_runtime_seconds=DEMO_RUNTIME_SECONDS)
    finally:
        print("\n== stopping producer ==")
        producer.terminate()
        try:
            producer.wait(timeout=10)
        except subprocess.TimeoutExpired:
            producer.kill()

    con = duckdb.connect()

    print("\n== gold table: lakehouse/gold_streaming/session_activity_1min ==")
    gold_path = (REPO_ROOT / "lakehouse" / "gold_streaming" / "session_activity_1min").as_posix()
    print(con.sql(f"select * from delta_scan('{gold_path}') order by window_start, event_type"))

    print("\n== pipeline metadata: streaming job runs (Phase 2's emitter, reused) ==")
    pipeline_runs_path = (REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs").as_posix()
    print(
        con.sql(
            f"""
            select job_name, status, started_at, rows_read, rows_written
            from delta_scan('{pipeline_runs_path}')
            where job_type = 'streaming'
            order by started_at
            """
        )
    )


if __name__ == "__main__":
    main()
