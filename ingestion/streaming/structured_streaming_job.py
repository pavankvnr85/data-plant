"""
Reads the clickstream Kafka/Redpanda topic, computes 1-minute windowed
event-type aggregates with a watermark, and merges finalized windows into a
local Delta gold table via delta-rs.

No Spark/JVM: same "no cloud account, no Spark" local design as the batch
lakehouse (see docs/build-plan.md's Phase 1 local-first path) -- a plain
Python Kafka consumer plus a small in-process windowing buffer stands in
for Spark Structured Streaming's readStream/groupBy(window(...))/writeStream.
Each poll cycle is one micro-batch, instrumented with the same
PipelineRunEmitter used by batch and dbt jobs (job_type="streaming"), so a
long streaming run shows up in pipeline_runs as a trail of heartbeat rows
instead of a single start/end pair.

Simplification vs. real Structured Streaming: window state (which windows
are still open, the watermark) lives in process memory, not a checkpoint,
so it does not survive a restart -- any windows still open when the process
exits are force-flushed at shutdown instead. Fine for a bounded local demo,
not for production; see docs/interfaces.md's "Stream processing" row for
the swap back to real Structured Streaming + checkpointing.

Local dev: `docker compose up -d` first (Redpanda on localhost:19092), then
run kafka_producer_clickstream.py in another terminal, then this script --
or run both together via scripts/run_phase4_streaming_demo.py.
"""
import json
import sys
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from kafka import KafkaConsumer

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(REPO_ROOT / "metadata"))
from openlineage_emitter import PipelineRunEmitter  # noqa: E402

TOPIC = "clickstream"
BOOTSTRAP_SERVERS = ["localhost:19092"]
CONSUMER_GROUP = "clickstream_sessionization"
GOLD_TABLE_PATH = str(REPO_ROOT / "lakehouse" / "gold_streaming" / "session_activity_1min")

WINDOW = timedelta(minutes=1)
WATERMARK_LAG = timedelta(seconds=30)
POLL_INTERVAL_SECONDS = 15

GOLD_SCHEMA = pa.schema(
    [
        pa.field("window_start", pa.timestamp("us", tz="UTC")),
        pa.field("window_end", pa.timestamp("us", tz="UTC")),
        pa.field("event_type", pa.string()),
        pa.field("event_count", pa.int64()),
        pa.field("distinct_sessions", pa.int64()),
    ]
)


def _merge_gold(rows: list[dict]) -> None:
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=GOLD_SCHEMA)
    if not DeltaTable.is_deltatable(GOLD_TABLE_PATH):
        write_deltalake(GOLD_TABLE_PATH, table, mode="error")
        return
    (
        DeltaTable(GOLD_TABLE_PATH)
        .merge(
            source=table,
            predicate=(
                "target.window_start = source.window_start "
                "AND target.window_end = source.window_end "
                "AND target.event_type = source.event_type"
            ),
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def _finalize_windows(
    buffer: dict[tuple, list[dict]], watermark: Optional[pd.Timestamp], force: bool = False
) -> list[dict]:
    """Pop and aggregate every window whose end has passed the watermark
    (or every window, if force=True at shutdown) so each window is merged
    into gold exactly once, the moment it closes -- mirroring what
    withWatermark()+groupBy(window(...))+outputMode("append") gives you in
    real Structured Streaming, without needing incremental merge arithmetic
    for values (like distinct-session counts) that can't just be summed
    across micro-batches.
    """
    ready_keys = [key for key in buffer if force or (watermark is not None and key[1] <= watermark)]
    rows = []
    for key in ready_keys:
        window_start, window_end, event_type = key
        events = buffer.pop(key)
        rows.append(
            {
                "window_start": window_start,
                "window_end": window_end,
                "event_type": event_type,
                "event_count": len(events),
                "distinct_sessions": len({e["session_id"] for e in events}),
            }
        )
    return rows


def run_streaming_job(
    max_runtime_seconds: int = 120, emitter: Optional[PipelineRunEmitter] = None
) -> None:
    if emitter is None:
        emitter = PipelineRunEmitter(
            metadata_table=str(REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs")
        )

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=POLL_INTERVAL_SECONDS * 1000,
    )

    buffer: dict[tuple, list[dict]] = defaultdict(list)
    max_event_time: Optional[pd.Timestamp] = None
    watermark: Optional[pd.Timestamp] = None
    deadline = time.monotonic() + max_runtime_seconds
    job_name = "clickstream_sessionization"

    print(
        f"Consuming '{TOPIC}' as group '{CONSUMER_GROUP}' for up to "
        f"{max_runtime_seconds}s, ~{POLL_INTERVAL_SECONDS}s micro-batches. Ctrl+C to stop early."
    )
    try:
        while time.monotonic() < deadline:
            cycle_deadline = min(deadline, time.monotonic() + POLL_INTERVAL_SECONDS)
            run = emitter.start_run(
                job_name=job_name, job_type="streaming", input_tables=[f"kafka:{TOPIC}"]
            )
            rows: list[dict] = []
            batch_events = 0
            try:
                for message in consumer:
                    event = message.value
                    event_time = pd.Timestamp(event["event_time"])
                    window_start = event_time.floor(WINDOW)
                    key = (window_start, window_start + WINDOW, event["event_type"])
                    buffer[key].append(event)
                    batch_events += 1
                    if max_event_time is None or event_time > max_event_time:
                        max_event_time = event_time
                    if time.monotonic() >= cycle_deadline:
                        break

                if max_event_time is not None:
                    watermark = max_event_time - WATERMARK_LAG
                rows = _finalize_windows(buffer, watermark)
                _merge_gold(rows)
                consumer.commit()
            except Exception as e:
                emitter.fail_run(job_name=job_name, error=str(e), run=run)
                raise

            emitter.end_run(
                run,
                status="success",
                rows_read=batch_events,
                rows_written=len(rows),
                output_table=GOLD_TABLE_PATH,
            )
            print(f"micro-batch: {batch_events} raw events, {len(rows)} window(s) finalized")
    except KeyboardInterrupt:
        print("Stopping (Ctrl+C)...")
    finally:
        # Force-finalize any windows still open so the demo doesn't end with
        # data invisibly stuck in memory. Real Structured Streaming would
        # instead leave this state in a checkpoint for the next run. Wrapped
        # in its own run so pipeline_runs' rows_written total still adds up
        # to what actually landed in the gold table.
        leftover = _finalize_windows(buffer, watermark=None, force=True)
        if leftover:
            flush_run = emitter.start_run(
                job_name=job_name, job_type="streaming", input_tables=[f"kafka:{TOPIC}"]
            )
            _merge_gold(leftover)
            consumer.commit()
            emitter.end_run(
                flush_run, status="success", rows_written=len(leftover), output_table=GOLD_TABLE_PATH
            )
            print(f"final flush: merged {len(leftover)} remaining window(s)")
        consumer.close()


def main():
    run_streaming_job()


if __name__ == "__main__":
    main()
