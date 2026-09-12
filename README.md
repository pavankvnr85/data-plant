# Data Plant

An enterprise data and AI platform demonstrating a lakehouse architecture with
batch and streaming pipelines, a self-observing metadata layer, and a
retrieval-augmented ops chatbot that answers questions about pipeline health,
cost, and waste.

## Purpose

Most portfolio lakehouse projects stop at "ingest data, transform it, show a
dashboard." Data Plant adds the layer that actual platform teams get paid to
build: every pipeline emits run metadata (rows processed, duration, status,
lineage) into its own table, and a RAG chatbot sits on top of that metadata
so you can ask things like "which pipelines cost the most" or "what tables
are unused" in plain English -- and get a real, correct answer back. Not
aspirational: see "Proof it works" below for the actual numbers from an
actual run.

The whole thing runs entirely on a laptop, no cloud account or API keys
required -- every layer also has a documented, narrow-interface swap to its
real cloud/production equivalent (Databricks, Unity Catalog, S3, OpenAI),
so the local version is a genuine substitute for the cloud path, not a toy.

**What's included** (see `docs/build-plan.md` for the full phased build log):
1. Batch lakehouse -- bronze/silver/gold on two sources
2. Pipeline metadata capture -- OpenLineage-style emitter into its own table
3. Orchestration -- Dagster asset graph + schedule
4. Streaming -- Kafka/Redpanda -> windowed aggregation -> gold
5. Analytics serving -- a dashboard over both batch and streaming gold
6. AI/RAG serving -- embeddings + vector store over the project's own docs
7. Wastage detection -- unused tables, cost, and runtime-trend models
8. Ops chatbot -- RAG over pipeline metadata + docs

All 8 are built, demoed, and runnable locally today.

## Proof it works

Real numbers from one actual end-to-end run of every local phase (8 batch
demo runs + 1 streaming run + 1 deliberately-triggered failure), captured
2026-09-13. Reproducible via the Usage commands below -- these aren't fixed
facts about the project, they're what one real run of it produces:

- **48 pipeline runs tracked** across 3 job types (33 batch ingestion, 8
  dbt, 7 streaming) -- 47 succeeded, 1 failed. The failure was deliberately
  triggered (a source pointing at a missing raw-data directory) to prove
  the metadata layer actually captures failures, not just happy paths --
  and the ops chatbot answered "why did a pipeline fail recently?" with
  that exact real error message.
- **Real Delta time travel**: bronze `orders` grew from 5,000 to 80,000
  rows across 16 Delta versions as the demo re-ran; `version=0` still
  reads back exactly 5,000 rows.
- **Wastage detection correctly flagged 2 genuinely unused tables**
  (`daily_revenue`, `session_activity_1min` -- neither had been read by
  another pipeline or viewed in the dashboard yet) and **a real +28%
  runtime regression** on `autoloader_orders` as its input data grew --
  not fabricated, just what happens when you feed a full-refresh job more
  data on every run. It also correctly *cleared* `stg_orders`/
  `stg_customers`/bronze after a bug fix (see `docs/build-plan.md`'s Phase
  7 section for the false-positive this replaced).
- **4/4 dbt tests passing** on every run (`not_null`/`unique` on staging,
  `not_null` on both bronze sources).
- **62 chunks embedded** from this repo's own `docs/*.md` into pgvector,
  fully offline (Ollama `nomic-embed-text` + `llama3.2:3b`, no API key) --
  the ops chatbot answered real questions like "what tables are unused"
  and "which pipelines cost the most" correctly, both from live SQL over
  `pipeline_runs` and from vector search over the docs themselves.

## Architecture

Full diagram + layer-by-layer notes: [`docs/architecture.md`](docs/architecture.md).
Every layer talks to the next one through a narrow interface (see
[`docs/interfaces.md`](docs/interfaces.md)) so any component can be swapped
without touching pipeline logic — e.g. pgvector for Databricks Vector
Search, Airflow for Dagster, Snowflake for Databricks.

## Repo layout

| Path | Purpose |
|---|---|
| `infra/aws` | S3 buckets, IAM roles, Terraform for the AWS side |
| `infra/databricks` | Unity Catalog, external locations, cluster policies |
| `ingestion/batch` | Config-driven Auto Loader jobs |
| `ingestion/streaming` | Kafka producer + windowed streaming consumer (local dev: plain Python, no Spark; see `docs/interfaces.md`) |
| `transform/dbt_project` | dbt models for silver/gold |
| `orchestration/dagster_project` | Dagster assets wiring everything together |
| `metadata` | Metadata table schema, OpenLineage emitter, wastage detection (`wastage_report.py`) |
| `serving` | Analytics dashboard over gold tables (local dev: Streamlit; see `docs/interfaces.md`) |
| `ai_rag` | Vector store interface, embedding ingestion, retrieval + answer script, ops chatbot (local dev: Ollama; see `docs/interfaces.md`) |
| `docs` | Architecture notes, interface contracts, build log |

## Setup

### Local (recommended -- no cloud account, no API keys)

Prerequisites:
- Python 3.11+
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for Redpanda/Kafka and Postgres/pgvector)
- [Ollama](https://ollama.com) (for local embeddings + LLM, needed from Phase 6 onward)

Install and start everything:

```powershell
pip install -r requirements.txt

docker compose up -d              # Redpanda (Kafka) + Postgres/pgvector

winget install --id Ollama.Ollama # or download from ollama.com
ollama pull nomic-embed-text      # embedding model, ~274MB
ollama pull llama3.2:3b           # answer LLM, ~2GB
```

You're now ready to run any phase -- see Usage below.

### Cloud (optional -- the production target every local piece is a swap-in for)

#### AWS
1. Create an AWS account (or use an existing one) at https://aws.amazon.com/free — the always-free tier covers S3 storage for this project's scale.
2. Create an IAM user or role with programmatic access; do not use the root account for anything after this step.
3. Note your account ID and pick a region (e.g. `us-east-1`) — use the same region for every resource in this project.
4. Set a **billing alert** at $5 and $20 immediately. This is the single most important step — free trials do not stop cloud spend once cloud-side resources (S3, EC2, networking) are running.

#### Databricks
Databricks currently offers two paths — use the cloud-linked one:
- **Free Edition / Express signup** — no cloud account needed, but gives you a serverless workspace with default storage, not your own S3. Fine for learning the UI, not for this project.
- **Free trial linked to your AWS account** — sign up at https://www.databricks.com/try-databricks, choose "sign up with your existing AWS account" (or go through AWS Marketplace). This gives 14 days and up to $400 of Databricks usage credit, deployed against your own AWS account, which is what lets Unity Catalog point at your S3 bucket.

Because the trial is time-boxed, don't activate it until you've done the S3 + IAM step above so you're not burning trial days on AWS console work.

#### Order of operations
1. Create the S3 bucket and IAM role for Unity Catalog (`infra/aws/README.md`).
2. Start the Databricks free trial linked to that AWS account.
3. Create a Unity Catalog metastore pointing at the S3 bucket (`infra/databricks/README.md`).
4. Create one small cluster (or use serverless SQL) and confirm you can read/write an Iceberg-compatible Delta table through Unity Catalog.
5. Only after that works, move to `ingestion/batch` and follow `docs/interfaces.md`'s swap notes to point each component at the cloud instead.

## Usage

Everything below assumes Setup (Local) is done. See the "Local-first path"
note under each phase in `docs/build-plan.md` for the full explanation of
each step; this is just the commands.

```powershell
# phases 1 + 2: seed, ingest to bronze (Delta), transform to silver/gold
# (DuckDB), print pipeline_runs
python scripts/run_phase1_local_demo.py

# phase 3: same jobs, run as a Dagster asset graph instead of ad hoc scripts
cd orchestration/dagster_project
dagster dev -m data_plant.assets   # UI at http://127.0.0.1:3000
cd ../..

# phase 4: clickstream producer + windowed consumer merging into a
# streaming gold Delta table, ~90s end to end
python scripts/run_phase4_streaming_demo.py

# phase 5: dashboard over both the batch and streaming gold tables
streamlit run serving/dashboard.py   # http://localhost:8501

# phase 6: local RAG over this repo's own docs
python ai_rag/ingest_embeddings.py --corpus-path "docs/*.md"
python ai_rag/ask.py "What message broker does the streaming phase use locally?"

# phase 7: wastage detection over real pipeline_runs history -- run the
# batch/streaming demos a few times first so there's a real trend to find
python metadata/wastage_report.py

# phase 8: ops chatbot -- SQL tools over pipeline_runs (reusing phase 7's
# wastage queries) or doc search (reusing phase 6's corpus), routed by
# keyword match; prints the raw data behind the answer too, not just the
# LLM's summary
python ai_rag/chatbot.py "Which pipelines cost the most last week?"
python ai_rag/chatbot.py "Why did a pipeline fail recently?"
```

Point Spark/Databricks/OpenAI at the cloud only when you're ready to run
them for real -- everything above is free and offline once the one-time
Docker images and Ollama models are pulled.
