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
python -c "import duckdb; con = duckdb.connect(); print(con.sql(\"select job_name, job_type, status, rows_written, output_tables from delta_scan('lakehouse/metadata/pipeline_runs') order by started_at\"))"
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

**Local-first path:** corpus is this repo's own `docs/*.md` -- on-domain,
free, no external corpus to source. Both the embedding model
(`nomic-embed-text`) and the answer LLM (`llama3.2:3b`) run locally via
[Ollama](https://ollama.com), which serves OpenAI-compatible endpoints on
`localhost:11434` -- so `ingest_embeddings.py` and the new `ask.py` still
go through the `openai` Python client already in `requirements.txt`, just
pointed at localhost with a dummy key, instead of adding a new dependency.
No API key, no cost. Storage is the `postgres`/pgvector container from
`docker-compose.yml` (already used for nothing until this phase);
`PgVectorStore` now creates its own extension + table on first use instead
of requiring a manual migration step. See `docs/interfaces.md`'s
"Embedding provider" and "Chatbot LLM" rows for the swap back to OpenAI.

`chunk_text()` is structure-aware, not a blind character-window slide:
markdown table rows (each a self-contained idea -- one row of
`docs/interfaces.md`'s swap-point table fully describes one component) are
kept as atomic chunks, and only a block still too long on its own falls
back to a sliding window. Found this the hard way in Phase 8: naive
800-char windows were splitting/blending table rows together, so a
chatbot question about "orchestrator" failed to retrieve the row that
plainly answers it (diluted with neighboring rows about Kafka instead) --
re-ingesting with row-aware chunking fixed it, verified by re-asking the
same question and getting the right row back as its own clean chunk.

- [x] Pick a document corpus relevant to the domain you chose (product docs, support tickets, whatever fits) -- used this repo's own `docs/*.md`
- [x] `ai_rag/ingest_embeddings.py` chunks and embeds into pgvector (local) — swap to Databricks Vector Search later if you want the "stays in platform" story
- [x] `ai_rag/vector_store.py` interface, pgvector implementation
- [x] Simple retrieval + LLM answer script (`ai_rag/ask.py`)
- **Demo artifact:** a question answered correctly with a citation back to the source doc.

### Local dev walkthrough

```powershell
# one-time setup
winget install --id Ollama.Ollama            # or download from ollama.com
ollama pull nomic-embed-text                 # embedding model, ~274MB
ollama pull llama3.2:3b                      # answer LLM, ~2GB

docker compose up -d                         # postgres/pgvector

# ingest the corpus (re-run after truncating `documents` if you re-ingest --
# chunk ids are random per run, so repeats duplicate rather than replace)
python ai_rag/ingest_embeddings.py --corpus-path "docs/*.md"

# ask a question
python ai_rag/ask.py "What message broker does the streaming phase use locally?"
```

Known tradeoff of the small local LLM: it answers correctly but is less
consistent than a hosted model (e.g. GPT-4o-mini) about following
formatting instructions like inline citations -- `ask.py` prints the
retrieved sources itself, deterministically, rather than depending on the
model to always mention them in prose.

## Phase 7 — Wastage detection

**Local-first path:** `metadata/wastage_report.py` runs all four checks
against the real `pipeline_runs` Delta table and prints one unified
findings table -- the literal demo artifact. Two of the checks are plain
DuckDB SQL (`metadata/wastage_models/*.sql`, assuming a `pipeline_runs`
view is already registered -- see the script); the schema-overlap check is
Python, since it needs to introspect each output table's actual columns
across two different storage shapes (DuckDB-native silver/gold vs.
standalone Delta paths for bronze/streaming-gold), which isn't naturally
one SQL query.

Fixed two real bugs surfaced while building this: (1) `run_dbt.py`
declared its bronze inputs as hardcoded relative-path strings that never
matched the absolute paths `autoloader_job.py` actually wrote, so bronze
tables were always (wrongly) flagged as unused -- now derived from
`sources.yaml` via `autoloader_job.load_sources()`, one source of truth;
(2) a dbt run building multiple models jammed them into one comma-joined
`output_table` string (order not even stable across runs, since dbt's
model execution order varies), so `pipeline_runs`' schema changed
`output_table` (string) to `output_tables` (list), matching `input_tables`'s
existing design -- one row per table, same as everywhere else.

Verified live: ran the batch demo 8x (growing bronze data 5,000 -> 40,000
orders) plus the streaming demo once, then ran the report against that
real history -- it correctly cleared bronze (genuinely read by every dbt
run) while flagging the streaming gold table (genuinely never read
downstream) and all three dbt-built tables (flagged due to a known,
documented limitation: job-level lineage can't see that dbt's own
`daily_revenue` model consumes `stg_orders`/`stg_customers` internally,
only that no *other* job declared them as an input -- real OpenLineage
would get this from dbt's manifest.json instead), and a genuine (not
fabricated) rising-runtime trend on `autoloader_orders` from the growing
data volume.

- [x] `metadata/wastage_models/` — SQL/dbt models over `pipeline_runs`:
  - [x] tables written but never read downstream (join against query history if available, or track reads via the emitter too) -- local dev uses the emitter's `input_tables` fallback (see `unused_tables.sql`'s comment for the known limitation)
  - [x] jobs with a rising runtime trend over the last N runs (last-5-vs-prior-5 average, `cost_and_runtime_trend.sql`)
  - [x] near-duplicate pipelines producing overlapping output schemas (`wastage_report.py`'s `check_duplicate_schemas`, column-overlap based)
  - [x] estimated cost per pipeline (`openlineage_emitter.py`'s `LOCAL_DEV_HOURLY_RATE_USD` proxy -- duration x a nominal rate, computed automatically in `end_run()` for every job type)
- **Demo artifact:** a table of real or synthetic "wasteful" pipelines with a plain-English reason for each.

### Local dev walkthrough

```powershell
# needs real run history to find anything -- the more runs (and the more
# growing data volume), the more genuine the runtime-trend signal
python scripts/run_phase1_local_demo.py   # repeat a few times
python scripts/run_phase4_streaming_demo.py

python metadata/wastage_report.py
```

## Phase 8 — Ops chatbot

**Local-first path:** `ai_rag/chatbot.py`'s router picks between two paths
per question -- `structured_lookup()` runs one of four SQL tools against
`pipeline_runs` (reusing the exact same `metadata/wastage_models/*.sql`
files Phase 7 built, unchanged), or `doc_search()` falls back to vector
search over the Phase 6 corpus (reusing `ai_rag/ask.py`'s
`retrieve()`/`build_context()`, so there's one embedding call site across
both scripts, not two). Same Ollama setup as Phase 6, no API key.

Since a 3B local model is inconsistent about fully enumerating a
multi-row SQL result in its prose (verified: asked it to summarize a
4-row "unused tables" result, it mentioned 2), `ask()` returns the raw
tool context alongside the LLM's answer and `__main__` prints both --
the module's own docstring already claimed the SQL path is "deterministic
and auditable"; this is what actually makes that true instead of aspirational.

- [x] Reuse `ai_rag/vector_store.py` against `pipeline_runs` + the wastage models + pipeline docs
- [x] `ai_rag/chatbot.py` — ask "which pipelines cost the most last week", "why did X fail last night", "what tables are unused"
- **Demo artifact:** a short recorded Q&A session, 5-6 real questions with correct answers.

### Local dev walkthrough

```powershell
# needs real pipeline_runs history (see Phase 7) and an ingested corpus
# (see Phase 6) to have anything to answer from
python ai_rag/chatbot.py "Which pipelines cost the most last week?"
python ai_rag/chatbot.py "Why did a pipeline fail recently?"
python ai_rag/chatbot.py "What tables are unused or wasteful?"
python ai_rag/chatbot.py "Is any pipeline getting slower over time?"
python ai_rag/chatbot.py "Why does the local dev path avoid Spark?"
```

Verified live against real history: the failure question correctly
surfaced an actually-triggered failure (a source pointing at a missing raw
data directory) with its real error message; the cost/trend questions
matched `wastage_report.py`'s own numbers exactly, since both read the
same `pipeline_runs` table the same way.

## Phase 9 — Polish for resume
- [ ] Architecture diagram in `docs/architecture.md`
- [ ] `docs/interfaces.md` documenting every swap point (vector store, orchestrator, warehouse)
- [ ] Record a 3-5 minute demo video
- [ ] Write real numbers into the README (e.g. "identified N% of pipelines producing tables with zero downstream reads")
