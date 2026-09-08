"""
Config-driven batch ingestion into local Delta Lake tables (delta-rs, no
Spark/JVM required). Reads sources.yaml and runs one full-refresh load per
source into bronze. Every source is a config entry, not a new script --
that's the "easy to extend" story for this layer.

Each run emits a PipelineRun record via the shared metadata emitter so this
job shows up in the pipeline_runs Delta table alongside streaming and dbt
runs.

For the cloud path (Databricks Auto Loader, s3:// sources), see
docs/build-plan.md's Phase 1 cloud checklist -- this script is the local
default described there.
"""
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from deltalake import write_deltalake

REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.append(str(REPO_ROOT / "metadata"))
from openlineage_emitter import PipelineRunEmitter  # noqa: E402


def load_sources(config_path: str = "sources.yaml") -> list[dict]:
    with open(Path(__file__).parent / config_path) as f:
        return yaml.safe_load(f)["sources"]


def read_raw(source: dict) -> pd.DataFrame:
    raw_dir = REPO_ROOT / source["raw_path"]
    fmt = source["format"]
    pattern = "*.json" if fmt == "json" else f"*.{fmt}"
    files = sorted(raw_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No {fmt} files found under {raw_dir}")

    if fmt == "json":
        frames = [pd.read_json(f, lines=True) for f in files]
    elif fmt == "csv":
        frames = [pd.read_csv(f) for f in files]
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    df = pd.concat(frames, ignore_index=True)
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    return df


def ingest_source(source: dict, emitter: PipelineRunEmitter) -> None:
    job_name = f"autoloader_{source['name']}"
    # start_run happens here, in the same scope as the try/except below, so
    # a failure can fail_run() *this exact* run_id instead of leaving its
    # "running" row orphaned and writing a second, unrelated "failed" row.
    run = emitter.start_run(
        job_name=job_name, job_type="batch_ingestion", input_tables=[source["raw_path"]]
    )
    try:
        bronze_path = str(REPO_ROOT / source["bronze_table"])
        df = read_raw(source)
        write_deltalake(bronze_path, df, mode="overwrite")
    except Exception as e:
        emitter.fail_run(job_name=job_name, error=str(e), run=run)
        raise

    emitter.end_run(
        run,
        status="success",
        rows_written=len(df),
        output_table=bronze_path,
    )


def run_source(source_name: str, emitter: Optional[PipelineRunEmitter] = None) -> None:
    """Ingest a single named source. Used by Dagster (one asset per source)
    as well as main()'s all-sources loop, so both paths share one code path
    and one PipelineRunEmitter contract."""
    if emitter is None:
        emitter = PipelineRunEmitter(
            metadata_table=str(REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs")
        )
    sources_by_name = {s["name"]: s for s in load_sources()}
    ingest_source(sources_by_name[source_name], emitter)


def main():
    emitter = PipelineRunEmitter(
        metadata_table=str(REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs")
    )
    for source in load_sources():
        run_source(source["name"], emitter=emitter)


if __name__ == "__main__":
    main()
