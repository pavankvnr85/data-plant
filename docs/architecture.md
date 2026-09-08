# Architecture

```
Sources (batch + streaming)
      |
      v
Ingestion (Auto Loader / Kafka + Structured Streaming)
      |
      v
Lakehouse storage (S3 + Iceberg/Delta, bronze/silver/gold)
      |
      v
Processing (Databricks + dbt, Structured Streaming)
      |
      v
Serving (BI/warehouse for analytics, Vector DB + LLM for AI/RAG)
      |
      v
Metadata platform (lineage, cost, quality -- fed by every layer above)
      |
      v
Ops chatbot (RAG over metadata + docs)
```

## Layer notes

**Sources.** One batch source (seeded Postgres or a public API) plus one
streaming source (synthetic clickstream via Kafka/Redpanda). Deliberately
minimal -- the point of the project is depth in the metadata/AI layer, not
breadth of connectors.

**Ingestion.** Config-driven: `ingestion/batch/sources.yaml` describes each
source declaratively so adding one is a config change. Streaming uses a
synthetic producer standing in for a real event source.

**Lakehouse storage.** Delta tables in Unity Catalog, UniForm-enabled for
Iceberg read compatibility -- gives the "vendor-neutral lakehouse" claim
without giving up Databricks' native tooling.

**Processing.** Spark for both batch and streaming; dbt for SQL
transformations in silver/gold, since dbt's own test/lineage metadata
becomes another input to the metadata platform for free.

**Serving.** Two consumption paths sharing the same gold layer: BI/SQL for
analytics, and a vector store + LLM for the AI/RAG use case.

**Metadata platform.** The differentiator. Every job -- batch, streaming,
dbt, Dagster asset -- writes one row per run into `metadata.pipeline_runs`
via a shared emitter (`metadata/openlineage_emitter.py`). Wastage models
(`metadata/wastage_models/`) derive unused-table and cost/runtime-trend
signals from that table.

**Ops chatbot.** RAG over the metadata table (via deterministic SQL tools,
not vector search, for numeric questions) plus vector search over docs for
open-ended "why did X happen" questions. See `docs/interfaces.md` for how
every one of these pieces is swappable.

See `docs/build-plan.md` for the phased build order and
`docs/interfaces.md` for the replaceability contract.
