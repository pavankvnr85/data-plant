# Data Plant

An enterprise data and AI platform demonstrating a lakehouse architecture with
batch and streaming pipelines, a self-observing metadata layer, and a
retrieval-augmented ops chatbot that answers questions about pipeline health,
cost, and waste.

## Why this exists

Most portfolio lakehouse projects stop at "ingest data, transform it, show a
dashboard." Data Plant adds the layer that actual platform teams get paid to
build: every pipeline emits run metadata (rows processed, bytes scanned,
duration, freshness) into its own Iceberg table, and a RAG chatbot sits on
top of that metadata so you can ask things like "which pipelines wrote
tables nobody has queried in 30 days" in plain English.

## Architecture

```
Sources (batch + streaming)
      -> Ingestion (Auto Loader / Kafka)
      -> Lakehouse storage (S3 + Iceberg, bronze/silver/gold)
      -> Processing (Databricks + dbt, Structured Streaming)
      -> Serving (BI/warehouse, Vector DB + LLM)
      -> Metadata platform (lineage, cost, quality)
      -> Ops chatbot (RAG over metadata)
```

Every layer talks to the next one through a narrow interface (see
`docs/interfaces.md`) so any component can be swapped without touching
pipeline logic — e.g. pgvector for Databricks Vector Search, Airflow for
Dagster, Snowflake for Databricks.

## Repo layout

| Path | Purpose |
|---|---|
| `infra/aws` | S3 buckets, IAM roles, Terraform for the AWS side |
| `infra/databricks` | Unity Catalog, external locations, cluster policies |
| `ingestion/batch` | Config-driven Auto Loader jobs |
| `ingestion/streaming` | Kafka producer + Spark Structured Streaming job |
| `transform/dbt_project` | dbt models for silver/gold and wastage detection |
| `orchestration/dagster_project` | Dagster assets wiring everything together |
| `metadata` | Metadata table schema + OpenLineage emitter |
| `ai_rag` | Vector store interface + ops chatbot |
| `docs` | Architecture notes, interface contracts, build log |

## Build order

See `docs/build-plan.md` for the full phased plan. Short version:

1. Cloud accounts + Unity Catalog + S3 (this doc, below)
2. Batch lakehouse (bronze/silver/gold on one source)
3. Pipeline metadata capture (OpenLineage-style emitter -> its own table)
4. Orchestration (Dagster asset graph + schedule)
5. Streaming use case (Kafka -> Structured Streaming -> gold)
6. Analytics serving (Databricks SQL + BI)
7. AI/RAG serving (embeddings + vector store)
8. Wastage detection models
9. Ops chatbot (RAG over metadata)

Items 2-4 above (`docs/build-plan.md`'s Phases 1-3) are done and runnable
entirely locally today -- no cloud account needed, see "Local dev" below.

## 1. Account setup (do this first)

### AWS
1. Create an AWS account (or use an existing one) at https://aws.amazon.com/free — the always-free tier covers S3 storage for this project's scale.
2. Create an IAM user or role with programmatic access; do not use the root account for anything after this step.
3. Note your account ID and pick a region (e.g. `us-east-1`) — use the same region for every resource in this project.
4. Set a **billing alert** at $5 and $20 immediately. This is the single most important step — free trials do not stop cloud spend once cloud-side resources (S3, EC2, networking) are running.

### Databricks
Databricks currently offers two paths — use the cloud-linked one:
- **Free Edition / Express signup** — no cloud account needed, but gives you a serverless workspace with default storage, not your own S3. Fine for learning the UI, not for this project.
- **Free trial linked to your AWS account** — sign up at https://www.databricks.com/try-databricks, choose "sign up with your existing AWS account" (or go through AWS Marketplace). This gives 14 days and up to $400 of Databricks usage credit, deployed against your own AWS account, which is what lets Unity Catalog point at your S3 bucket.

Because the trial is time-boxed, don't activate it until you've done step 2 below (S3 + IAM) so you're not burning trial days on AWS console work.

### Order of operations
1. Create the S3 bucket and IAM role for Unity Catalog (`infra/aws/README.md`).
2. Start the Databricks free trial linked to that AWS account.
3. Create a Unity Catalog metastore pointing at the S3 bucket (`infra/databricks/README.md`).
4. Create one small cluster (or use serverless SQL) and confirm you can read/write an Iceberg-compatible Delta table through Unity Catalog.
5. Only after that works, move to `ingestion/batch`.

## Local dev (no cloud needed for iteration)

Phases 1-3 (batch lakehouse, pipeline metadata, orchestration) run entirely
on your laptop, no AWS/Databricks account required: `deltalake` (delta-rs)
for bronze, `dbt-duckdb` for silver/gold, Dagster for the asset graph and
schedule. See the "Local-first path" note under each phase in
`docs/build-plan.md` for the full walkthrough, or run it in one shot:

```powershell
pip install -r requirements.txt

# phases 1 + 2: seed, ingest to bronze (Delta), transform to silver/gold
# (DuckDB), print pipeline_runs
python scripts/run_phase1_local_demo.py

# phase 3: same jobs, run as a Dagster asset graph instead of ad hoc scripts
cd orchestration/dagster_project
dagster dev -m data_plant.assets   # UI at http://127.0.0.1:3000
```

`docker-compose.yml` separately spins up Kafka (via Redpanda) and Postgres
with pgvector, for the streaming (Phase 5) and RAG (Phase 7) work later --
those still need Docker regardless of the local/cloud choice above. Point
Spark/Databricks jobs at the cloud only when you're ready to run them for
real.

```bash
docker compose up -d
```
