# Build plan

Each phase should end with something you can screenshot or demo. Don't move
to the next phase until the current one produces real output — this is what
makes the finished project defensible in an interview.

## Phase 0 — Accounts and foundation (this week)
- [ ] AWS account, IAM role, S3 bucket (`infra/aws/README.md`)
- [ ] Billing alerts set
- [ ] Databricks free trial linked to AWS account
- [ ] Unity Catalog metastore created, pointing at the S3 bucket
- [ ] One cluster or SQL warehouse running, can create/query a table
- **Demo artifact:** screenshot of a table created via Databricks SQL, queryable, backed by your S3 bucket.

## Phase 1 — Batch lakehouse (bronze/silver/gold)

**Local-first path (recommended starting point):** this phase can be done
entirely on your laptop with no AWS/Databricks account -- `deltalake`
(delta-rs) for bronze, `dbt-duckdb` for silver/gold. See the "local dev" row
in `docs/interfaces.md` and the walkthrough below. The cloud checklist below
remains the eventual target and needs no code changes beyond
`sources.yaml` paths/table names and `profiles.yml`'s target.

- [ ] Pick one real batch source (e.g. a public dataset via API, or a Postgres OLTP DB you seed yourself with fake orders/customers)
- [ ] `ingestion/batch/sources.yaml` describes the source declaratively
- [ ] Auto Loader job lands raw data into bronze (`ingestion/batch/autoloader_job.py`)
- [ ] dbt models clean into silver, aggregate into gold (`transform/dbt_project`)
- [ ] Confirm Iceberg/Delta time travel works (`DESCRIBE HISTORY`, query an old version)
- **Demo artifact:** a gold table + a screenshot of time travel returning an older row.

### Local dev walkthrough (no cloud account needed)

```powershell
# from repo root
pip install -r requirements.txt

# 1. seed raw data
python ingestion/batch/seed_data.py
# 2. first bronze ingestion run -> Delta version 0
python ingestion/batch/autoloader_job.py
# 3. seed a second batch (adds new files, doesn't overwrite)
python ingestion/batch/seed_data.py
# 4. second bronze ingestion run -> Delta version 1
python ingestion/batch/autoloader_job.py

# 5. transform: silver views + gold tables in warehouse.duckdb
# (wraps `dbt run` with the Phase 2 metadata emitter -- see below. Plain
# `dbt run` from transform/dbt_project/ still works, it just won't show up
# in pipeline_runs)
python transform/run_dbt.py

# 6. time-travel demo (the literal acceptance artifact)
python -c "from deltalake import DeltaTable; dt = DeltaTable('lakehouse/bronze/orders'); print(dt.history()); print('v0 rows:', len(DeltaTable('lakehouse/bronze/orders', version=0).to_pandas())); print('latest rows:', len(dt.to_pandas()))"

# 7. confirm the gold table (note: dbt's schema macro produces main_gold, not gold)
python -c "import duckdb; con = duckdb.connect('lakehouse/warehouse.duckdb'); print(con.sql('select * from main_gold.daily_revenue limit 10'))"

# 8. confirm pipeline metadata (Phase 2 acceptance artifact): one row per
# ingestion run plus one per dbt run, upserted by run_id
python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"select job_name, job_type, status, rows_written, output_table from delta_scan('lakehouse/metadata/pipeline_runs') order by started_at\"))"
```

Or run all of the above in one shot: `python scripts/run_phase1_local_demo.py`.

Time travel is demoed on the **bronze** `orders` table specifically: in this
local design only bronze is Delta-backed (silver/gold live in a DuckDB
file), so bronze is the only layer that can actually prove Delta's
versioning mechanics locally. The cloud path additionally gets Delta-backed
silver/gold with their own history for free.

## Phase 2 — Pipeline metadata capture v1

**Local-first path:** already mostly built as a side effect of Phase 1 --
`ingestion/batch/autoloader_job.py` was wrapped with the emitter from the
start. The one real gap was dbt: running bare `dbt run` doesn't call any
Python, so the transform layer was invisible to `pipeline_runs`. Closed by
`transform/run_dbt.py`, a wrapper around dbt-core's programmatic `dbtRunner`
API (not a dbt on-run-end hook -- those only run SQL against the adapter
connection, with no clean path to a Python emitter) that reports one
`job_type="dbt"` row per invocation: status, duration, and total rows
written across the models dbt just built (read back from the warehouse,
since dbt-duckdb's adapter response doesn't report rows_affected for
view/table creates).

- [x] Create `metadata.pipeline_runs` table (`metadata/schema.sql` for the
      cloud DDL; local dev's source of truth is `PIPELINE_RUN_SCHEMA` in
      `metadata/openlineage_emitter.py`)
- [x] Wrap each dbt/Spark job with the OpenLineage emitter
      (`metadata/openlineage_emitter.py`, called from
      `ingestion/batch/autoloader_job.py` and `transform/run_dbt.py`)
- [x] Every batch run from Phase 1 now writes a row: job name, start/end,
      rows in/out, status (`bytes_scanned` stays null locally -- there's no
      cloud billing API to source it from until Phase 5)
- **Demo artifact:** query `metadata.pipeline_runs` and show real rows from
  real runs -- see the local walkthrough above, step 5 now uses
  `python transform/run_dbt.py` instead of bare `dbt run`, and
  `python scripts/run_phase1_local_demo.py` prints the table at the end.

## Phase 3 — Orchestration

**Local-first path:** `orchestration/dagster_project/data_plant/assets.py` models
the four local Phase 1/2 jobs (`orders_bronze`, `customers_bronze`,
`silver_models`, `daily_revenue_gold`) as a Dagster asset graph. Each asset
body calls straight into the same functions the manual walkthrough uses
(`autoloader_job.run_source()`, `run_dbt.run_dbt_job()`) instead of
reimplementing anything, so a Dagster-triggered run is wrapped by
`PipelineRunEmitter` exactly like a manually-triggered one -- satisfying the
"don't duplicate, reuse the emitter" requirement below by construction
rather than by extra code. The cloud swap (Databricks Jobs API instead of
local function calls) is documented in `docs/interfaces.md`'s
"Orchestrator" row; the asset graph shape and schedule don't change.

- [x] Stand up Dagster (`orchestration/dagster_project`)
- [x] Model bronze -> silver -> gold as a Dagster asset graph (2 bronze
      assets, `silver_models` depends on both, `daily_revenue_gold` depends
      on `silver_models`)
- [x] Schedule it (`daily_schedule`, 03:00 UTC daily -- see `assets.py`)
- [x] Dagster's own run metadata feeds into `pipeline_runs` too (each asset
      calls the shared emitter directly; no separate Dagster-side metadata
      path exists to keep in sync)
- **Demo artifact:** Dagster asset graph UI screenshot, a scheduled run that succeeded.

### Local dev walkthrough

```powershell
# from repo root -- seed + a first bronze load so the asset graph has
# something to build on (or skip straight to materializing; orders_bronze/
# customers_bronze will do their own load)
python ingestion/batch/seed_data.py

cd orchestration/dagster_project

# option A: launch the UI (the actual screenshot artifact) -- opens
# http://127.0.0.1:3000 with the asset graph, lineage, and a "Materialize
# all" button
dagster dev -m data_plant.assets

# option B: materialize from the CLI, no UI (asset names spelled out
# instead of `--select "*"` -- Git Bash on Windows expands `*` against the
# current directory's files before dagster ever sees it)
dagster asset materialize -m data_plant.assets --select "orders_bronze,customers_bronze,silver_models,daily_revenue_gold"

cd ../..
```

Either path writes into the same `lakehouse/` used by the rest of the local
demo, so `python -c "import duckdb; ..."` (see Phase 2's step 8 above) shows
Dagster's runs sitting in `pipeline_runs` next to CLI-triggered ones, with
distinct job names (`dbt_run_silver`/`dbt_run_gold` vs. plain `dbt_run`) so
they're easy to tell apart.

## Phase 4 — Streaming use case

**Local-first path:** `docker-compose.yml`'s Redpanda service is a real
Kafka-wire-protocol broker (kafka-python talks to it exactly like it would
talk to Apache Kafka), so the producer needed no changes.
`structured_streaming_job.py` was rewritten off Spark Structured
Streaming onto plain Python: a `KafkaConsumer` polls in ~15s micro-batches,
buffers events by 1-minute tumbling window, and only merges a window into
the local Delta gold table once a watermark says it's closed (mirroring
`withWatermark()` + `groupBy(window(...))` + `outputMode("append")`) --
avoiding the need to incrementally merge things like distinct-session
counts across batches, which plain SQL arithmetic can't do correctly.
Simplification: window state lives in process memory, not a checkpoint, so
it doesn't survive a restart; windows still open at shutdown are
force-flushed instead (see the module docstring). See
`docs/interfaces.md`'s "Stream processing" row for the swap back to real
Structured Streaming + checkpointing.

- [x] `ingestion/streaming/kafka_producer_clickstream.py` generates synthetic events into Redpanda/Kafka
- [x] `ingestion/streaming/structured_streaming_job.py` reads the topic, windows and aggregates, merges into a gold Delta table
- [x] Same metadata emitter instruments this job -- batch and streaming show up in one place (each micro-batch is its own heartbeat row, `job_type="streaming"`)
- **Demo artifact:** a streaming gold table updating in near-real-time while the producer runs.

### Local dev walkthrough

```powershell
# from repo root -- Redpanda must be up first
docker compose up -d

# option A: one command, ~90s end to end (producer in the background,
# consumer in the foreground, then prints the gold table + metadata trail)
python scripts/run_phase4_streaming_demo.py

# option B: two terminals, to actually watch it update in near-real-time
# terminal 1
python ingestion/streaming/kafka_producer_clickstream.py
# terminal 2 (Ctrl+C to stop early; defaults to a 120s bounded run)
python ingestion/streaming/structured_streaming_job.py

# either way, inspect the result same as any other Delta table:
python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"select * from delta_scan('lakehouse/gold_streaming/session_activity_1min') order by window_start, event_type\"))"
```

## Phase 5 — Analytics serving

**Local-first path:** `serving/dashboard.py`, a small Streamlit app --
chosen over Evidence/Rill/Superset/Metabase/Grafana after comparing DuckDB
connectivity, setup weight, and toolchain fit (see `docs/interfaces.md`'s
"BI/serving" row for the comparison summary). It reads two sources through
two separate connections so the dashboard can never hold a lock that blocks
a batch write: `main_gold.daily_revenue` via a `read_only` connection to
`warehouse.duckdb`, and `gold_streaming/session_activity_1min` via
`delta_scan()` on a fresh in-memory connection that never touches
`warehouse.duckdb` at all. Verified live: ran `transform/run_dbt.py` (a
real write to `warehouse.duckdb`) while the dashboard server was up, with
no lock conflict, because each dashboard query opens and closes its
connection immediately rather than holding one open.

- [x] Local BI layer over gold tables (Streamlit + DuckDB, in place of a Databricks SQL warehouse)
- [x] Build 2-3 dashboards: revenue over time, revenue by region, streaming event activity by window/type
- **Demo artifact:** a dashboard screenshot built on both the batch and streaming gold tables.

### Local dev walkthrough

```powershell
# from repo root -- needs both the batch and streaming demos run at least
# once so there's data to show
python scripts/run_phase1_local_demo.py
python scripts/run_phase4_streaming_demo.py

streamlit run serving/dashboard.py   # opens http://localhost:8501
```

## Phase 6 — AI/RAG serving
- [ ] Pick a document corpus relevant to the domain you chose (product docs, support tickets, whatever fits)
- [ ] `ai_rag/ingest_embeddings.py` chunks and embeds into pgvector (local) — swap to Databricks Vector Search later if you want the "stays in platform" story
- [ ] `ai_rag/vector_store.py` interface, pgvector implementation
- [ ] Simple retrieval + LLM answer script
- **Demo artifact:** a question answered correctly with a citation back to the source doc.

## Phase 7 — Wastage detection
- [ ] `metadata/wastage_models/` — SQL/dbt models over `pipeline_runs`:
  - tables written but never read downstream (join against query history if available, or track reads via the emitter too)
  - jobs with a rising runtime trend over the last N runs
  - near-duplicate pipelines producing overlapping output schemas
  - estimated cost per pipeline (DBU usage x list price, or cluster size x runtime as a proxy)
- **Demo artifact:** a table of real or synthetic "wasteful" pipelines with a plain-English reason for each.

## Phase 8 — Ops chatbot
- [ ] Reuse `ai_rag/vector_store.py` against `pipeline_runs` + the wastage models + pipeline docs
- [ ] `ai_rag/chatbot.py` — ask "which pipelines cost the most last week", "why did X fail last night", "what tables are unused"
- **Demo artifact:** a short recorded Q&A session, 5-6 real questions with correct answers.

## Phase 9 — Polish for resume
- [ ] Architecture diagram in `docs/architecture.md`
- [ ] `docs/interfaces.md` documenting every swap point (vector store, orchestrator, warehouse)
- [ ] Record a 3-5 minute demo video
- [ ] Write real numbers into the README (e.g. "identified N% of pipelines producing tables with zero downstream reads")
