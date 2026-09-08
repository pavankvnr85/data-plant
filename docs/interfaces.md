# Interfaces and swap points

The "easy to replace any component" requirement is only credible if it's
concrete. Here's every deliberate boundary in Data Plant and what it costs
to swap.

| Layer | Interface | Default | Swap example | What changes |
|---|---|---|---|---|
| Storage format | Delta/Iceberg via Unity Catalog | Delta (UniForm-compatible with Iceberg reads) | Pure Iceberg + AWS Glue catalog | Table DDL + catalog config only; ingestion/transform code unchanged |
| Storage + compute (local dev) | Bronze: `deltalake` (delta-rs) tables on local disk; Silver/Gold: DuckDB via `dbt-duckdb` reading bronze through `delta_scan()` | Default for Phase 1 local iteration, no cloud account required | Databricks + Unity Catalog + S3 (see "Compute engine" row) | Swap `ingestion/batch/autoloader_job.py`'s `write_deltalake()` calls for Auto Loader `writeStream`, and `profiles.yml`'s `local` target for `databricks`; `sources.yaml`, the dbt SQL models, and `metadata/openlineage_emitter.py`'s call sites are unchanged |
| Compute engine | Spark on Databricks | Databricks jobs/clusters | OSS Spark on EMR, or Snowflake for the SQL layer | `infra/databricks` replaced by `infra/emr`; `ingestion/*` scripts are already plain PySpark so they mostly port as-is |
| dbt invocation (local dev) | `transform/run_dbt.py` wraps dbt-core's programmatic `dbtRunner` API | Reports one `PipelineRunEmitter` row per `dbt run` invocation | Bare `dbt run` CLI, or (cloud) a Dagster-triggered dbt Cloud/Databricks job | Cloud path emits metadata the same way a Databricks job step would -- wrap the job trigger with `start_run`/`end_run`, same as `run_dbt.py` does; the dbt project itself (`transform/dbt_project`) is unchanged either way |
| Orchestrator | Dagster asset graph | Dagster, local dev default: asset bodies call local Python functions directly (`ingestion/batch/autoloader_job.run_source()`, `transform/run_dbt.run_dbt_job()`) | Airflow DAGs; or, staying on Dagster, swap each asset body for a Databricks Jobs API call (`WorkspaceClient.jobs.run_now(...).wait_get_run_job_terminated_or_skipped()`) against jobs wrapping the same scripts on a real cluster | `orchestration/dagster_project` replaced for the Airflow swap; for the Databricks-jobs swap, only the four `@asset` function bodies in `assets.py` change -- the graph shape, schedule, and `PipelineRunEmitter` calls (made by the underlying scripts either way) are unchanged |
| Metadata sink | `PipelineRunEmitter` writing to a Delta table | Direct MERGE into `pipeline_runs` (Spark SQL on Databricks; delta-rs `DeltaTable.merge()` for local dev) | Real OpenLineage + Marquez server | Only `_write()` in `openlineage_emitter.py` changes; every call site (`start_run`/`end_run`/`fail_run`) stays identical |
| Vector store | `VectorStore` ABC | `PgVectorStore` | `DatabricksVectorSearchStore` | Add one class in `ai_rag/vector_store.py` implementing `upsert`/`query`; `ingest_embeddings.py` and `chatbot.py` don't change |
| Embedding provider | `embed_texts()` / `embed()` functions | OpenAI `text-embedding-3-small` | Any other embedding API | One function body per file |
| Chatbot LLM | OpenAI chat completion | `gpt-4o-mini` | Any LLM API (incl. Claude) | One client call in `chatbot.py`; the SQL-vs-doc-search routing logic is provider-agnostic |
| BI/serving | Databricks SQL warehouse | Databricks SQL | Snowflake, Redshift, or a BI tool pointed at either | Serving layer only reads gold tables; no upstream change needed |

The pattern throughout: **every component talks to the next one through a
narrow interface it owns, never reaches past it.** Ingestion doesn't know
about Dagster. Dagster doesn't know how jobs compute, only that they run
and report status. The chatbot doesn't know if `PgVectorStore` is backed by
Postgres or something else. That discipline is what makes the swap claims
true instead of aspirational.
