"""
The payoff piece: ask questions about pipeline health, cost, and waste in
plain English.

Design: most "wastage" questions are really SQL questions
("which pipelines cost the most") -- forcing those through vector search
alone gives worse answers than just querying pipeline_runs directly. So this
chatbot does two things and lets keyword routing decide which it needs:

  1. structured_lookup(): runs one of a small set of parameterized SQL
     queries against pipeline_runs, reusing the exact same
     metadata/wastage_models/*.sql files Phase 7 built (they already
     assume a `pipeline_runs` view is registered, which is exactly what
     _connect() below does)
  2. doc_search(): vector search over docs/READMEs for unstructured
     context an SQL query can't answer -- reuses ai_rag/ask.py's
     retrieve()/build_context() so there's one embedding call site, not two

This keeps the SQL path deterministic and auditable (you can always see the
exact query that produced a number) while still letting the bot answer
free-form "why did X fail" questions from doc/log context.

Local dev: same Ollama setup as ask.py (no API key), same in-memory
DuckDB + delta_scan() pattern as metadata/wastage_report.py for
pipeline_runs (never touches warehouse.duckdb's file lock).
"""
import os
import sys
from pathlib import Path

import duckdb

from ask import CHAT_MODEL, OLLAMA_BASE_URL, build_context, retrieve
from vector_store import PgVectorStore

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_RUNS_PATH = (REPO_ROOT / "lakehouse" / "metadata" / "pipeline_runs").as_posix()
TABLE_READS_PATH = REPO_ROOT / "lakehouse" / "metadata" / "table_reads"
WASTAGE_MODELS_DIR = REPO_ROOT / "metadata" / "wastage_models"

SQL_TOOLS = {
    "top_cost_pipelines": """
        SELECT job_name, SUM(estimated_cost_usd) AS total_cost
        FROM pipeline_runs
        WHERE status = 'success' AND ended_at >= current_timestamp - INTERVAL 7 DAY
        GROUP BY job_name ORDER BY total_cost DESC LIMIT 10
    """,
    "recent_failures": """
        SELECT job_name, ended_at, error_message
        FROM pipeline_runs
        WHERE status = 'failed' AND ended_at >= current_timestamp - INTERVAL 3 DAY
        ORDER BY ended_at DESC
    """,
    "unused_tables": WASTAGE_MODELS_DIR / "unused_tables.sql",
    "runtime_trend": WASTAGE_MODELS_DIR / "cost_and_runtime_trend.sql",
}


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def structured_lookup(con: duckdb.DuckDBPyConnection, tool_name: str) -> list[dict]:
    # Registered lazily, here, rather than in _connect(): a pure doc_search
    # question never touches pipeline_runs, and shouldn't fail just because
    # no pipeline has run yet (e.g. a fresh clone with an empty lakehouse/).
    con.sql(f"create or replace view pipeline_runs as select * from delta_scan('{PIPELINE_RUNS_PATH}')")
    if TABLE_READS_PATH.exists():
        con.sql(
            f"create or replace view table_reads as select * from delta_scan('{TABLE_READS_PATH.as_posix()}')"
        )
    else:
        con.sql(
            "create or replace view table_reads as select "
            "cast(null as varchar) as table_name, "
            "cast(null as timestamp) as read_at, "
            "cast(null as varchar) as reader_job "
            "where false"
        )
    query = SQL_TOOLS[tool_name]
    if isinstance(query, Path):
        query = query.read_text()
    return con.sql(query).df().to_dict(orient="records")


def doc_search(store: PgVectorStore, question: str) -> str:
    results = retrieve(store, question)
    return build_context(results)


ROUTER_SYSTEM_PROMPT = """You are the Data Plant ops assistant. You answer
questions about pipeline cost, health, and waste using two tools:

- structured_lookup(tool_name): exact numbers from pipeline_runs.
  tool_name must be one of: top_cost_pipelines, recent_failures,
  unused_tables, runtime_trend.
- doc_search(question): unstructured context from docs, for "why" questions
  the SQL tools can't answer.

Decide which tool(s) the question needs, use them, and answer concisely
with real numbers. If a question needs data outside these tools, say so
rather than guessing.
"""


def ask(question: str, con: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    """Minimal, non-agentic router: pick a tool based on keyword overlap.

    A real version would let the LLM call these as function-calling tools;
    this keeps the reference implementation dependency-light and easy to
    read. Swap in proper tool-calling once you're past the demo stage.

    Returns (answer, context) rather than just the answer: this module's
    own docstring promises the SQL path is "deterministic and auditable",
    which isn't actually true unless the raw data is surfaced somewhere --
    a 3B local model summarizing a 4-row result set is prone to dropping
    rows (verified: it does), so __main__ below prints the raw context
    too rather than asking the reader to trust the summary.
    """
    q = question.lower()

    if any(k in q for k in ["cost", "expensive", "spend"]):
        data = structured_lookup(con, "top_cost_pipelines")
        context = f"Top cost pipelines (last 7 days): {data}"
    elif any(k in q for k in ["fail", "error", "broke"]):
        data = structured_lookup(con, "recent_failures")
        context = f"Recent failures: {data}"
    elif any(k in q for k in ["unused", "waste", "orphan"]):
        data = structured_lookup(con, "unused_tables")
        context = f"Unused tables: {data}"
    elif any(k in q for k in ["slow", "trend", "degrad"]):
        data = structured_lookup(con, "runtime_trend")
        context = f"Runtime trend: {data}"
    else:
        store = PgVectorStore(
            dsn=os.environ.get(
                "PGVECTOR_DSN", "postgresql://data_plant:data_plant@localhost:5432/data_plant"
            )
        )
        context = doc_search(store, question)

    from openai import OpenAI

    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\nContext:\n{context}"},
        ],
    )
    return response.choices[0].message.content, context


if __name__ == "__main__":
    question = " ".join(sys.argv[1:]) or "Which pipelines cost the most last week?"
    answer_text, context = ask(question, _connect())
    print(answer_text)
    print(f"\n[raw data behind this answer]\n{context}")
