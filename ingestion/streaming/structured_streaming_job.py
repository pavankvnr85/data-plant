"""
Reads the clickstream topic, computes 1-minute windowed session aggregates,
and merges into a gold Delta/Iceberg table. Run on Databricks (or local
Spark against Redpanda for dev).

Instrumented with the same PipelineRunEmitter used by batch jobs so
streaming and batch pipelines show up in one metadata table.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, approx_count_distinct
from pyspark.sql.types import StructType, StringType, TimestampType

sys.path.append("../../metadata")
from openlineage_emitter import PipelineRunEmitter  # noqa: E402

KAFKA_BOOTSTRAP = "localhost:19092"  # override for prod broker
TOPIC = "clickstream"
GOLD_TABLE = "data_plant.gold.session_activity_1min"
CHECKPOINT = "s3://<your-bucket>/checkpoints/session_activity/"

EVENT_SCHEMA = (
    StructType()
    .add("event_id", StringType())
    .add("session_id", StringType())
    .add("user_id", StringType())
    .add("event_type", StringType())
    .add("page", StringType())
    .add("event_time", TimestampType())
)


def build_query(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    windowed = (
        events.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("event_type"))
        .agg(
            count("*").alias("event_count"),
            approx_count_distinct("session_id").alias("distinct_sessions"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count"),
            col("distinct_sessions"),
        )
    )

    return (
        windowed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .toTable(GOLD_TABLE)
    )


def main():
    spark = SparkSession.builder.getOrCreate()
    emitter = PipelineRunEmitter(spark, metadata_table="data_plant.metadata.pipeline_runs")
    run = emitter.start_run(job_name="clickstream_sessionization", job_type="streaming")

    query = build_query(spark)

    # In production this runs continuously; the emitter logs periodic
    # heartbeats rather than a single start/end pair. For a demo, let it
    # run for a bounded window and then stop cleanly.
    try:
        query.awaitTermination(timeout=600)  # 10 min demo window
    finally:
        query.stop()
        rows_written = spark.table(GOLD_TABLE).count()
        emitter.end_run(run, status="success", rows_written=rows_written, output_table=GOLD_TABLE)


if __name__ == "__main__":
    main()
