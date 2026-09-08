"""
Shared metadata emitter used by every job in the platform (batch, streaming,
dbt via transform/run_dbt.py's dbtRunner wrapper, Dagster assets).

This is deliberately simple: it writes directly to the pipeline_runs Delta
table rather than standing up a full OpenLineage/Marquez server. That's a
legitimate first version -- swap `_write` for a real OpenLineage HTTP
transport later without changing any call site, since every job only talks
to this class, never to the storage mechanism directly. That swap point is
the "replaceable component" story for this layer.

Local dev (no Spark/Databricks) uses delta-rs (the `deltalake` package) to
perform the same upsert-by-run_id merge that the cloud path does via Spark
SQL `MERGE INTO`. See metadata/schema.sql for the cloud-DDL equivalent of
the schema defined below.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

PIPELINE_RUN_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string()),
        pa.field("job_name", pa.string()),
        pa.field("job_type", pa.string()),
        pa.field("status", pa.string()),
        pa.field("started_at", pa.timestamp("us", tz="UTC")),
        pa.field("ended_at", pa.timestamp("us", tz="UTC")),
        pa.field("duration_seconds", pa.float64()),
        pa.field("rows_read", pa.int64()),
        pa.field("rows_written", pa.int64()),
        pa.field("bytes_scanned", pa.int64()),
        pa.field("input_tables", pa.list_(pa.string())),
        pa.field("output_table", pa.string()),
        pa.field("error_message", pa.string()),
        pa.field("estimated_cost_usd", pa.float64()),
    ]
)


@dataclass
class PipelineRun:
    run_id: str
    job_name: str
    job_type: str
    started_at: datetime
    input_tables: list[str] = field(default_factory=list)


class PipelineRunEmitter:
    def __init__(self, metadata_table: str):
        self.metadata_table = metadata_table

    def start_run(
        self, job_name: str, job_type: str, input_tables: Optional[list[str]] = None
    ) -> PipelineRun:
        run = PipelineRun(
            run_id=str(uuid.uuid4()),
            job_name=job_name,
            job_type=job_type,
            started_at=datetime.now(timezone.utc),
            input_tables=input_tables or [],
        )
        self._write(
            run_id=run.run_id,
            job_name=job_name,
            job_type=job_type,
            status="running",
            started_at=run.started_at,
            ended_at=None,
            duration_seconds=None,
            rows_read=None,
            rows_written=None,
            bytes_scanned=None,
            input_tables=run.input_tables,
            output_table=None,
            error_message=None,
            estimated_cost_usd=None,
        )
        return run

    def end_run(
        self,
        run: PipelineRun,
        status: str,
        rows_written: Optional[int] = None,
        rows_read: Optional[int] = None,
        bytes_scanned: Optional[int] = None,
        output_table: Optional[str] = None,
        estimated_cost_usd: Optional[float] = None,
    ) -> None:
        ended_at = datetime.now(timezone.utc)
        duration = (ended_at - run.started_at).total_seconds()
        self._write(
            run_id=run.run_id,
            job_name=run.job_name,
            job_type=run.job_type,
            status=status,
            started_at=run.started_at,
            ended_at=ended_at,
            duration_seconds=duration,
            rows_read=rows_read,
            rows_written=rows_written,
            bytes_scanned=bytes_scanned,
            input_tables=run.input_tables,
            output_table=output_table,
            error_message=None,
            estimated_cost_usd=estimated_cost_usd,
        )

    def fail_run(self, job_name: str, error: str, run: Optional[PipelineRun] = None) -> None:
        started_at = run.started_at if run else datetime.now(timezone.utc)
        self._write(
            run_id=run.run_id if run else str(uuid.uuid4()),
            job_name=job_name,
            job_type=run.job_type if run else "unknown",
            status="failed",
            started_at=started_at,
            ended_at=datetime.now(timezone.utc),
            duration_seconds=None,
            rows_read=None,
            rows_written=None,
            bytes_scanned=None,
            input_tables=run.input_tables if run else [],
            output_table=None,
            error_message=error,
            estimated_cost_usd=None,
        )

    def _write(self, **row) -> None:
        # UPSERT via delta-rs MERGE so the "running" row placed at start_run
        # gets updated in place by end_run/fail_run rather than duplicated.
        table = pa.Table.from_pylist([row], schema=PIPELINE_RUN_SCHEMA)

        if not DeltaTable.is_deltatable(self.metadata_table):
            write_deltalake(self.metadata_table, table, mode="error")
            return

        (
            DeltaTable(self.metadata_table)
            .merge(
                source=table,
                predicate="target.run_id = source.run_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
