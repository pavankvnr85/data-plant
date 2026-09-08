"""
The payoff piece: ask questions about pipeline health, cost, and waste in
plain English.

Design: most "wastage" questions are really SQL questions
("which pipelines cost the most") -- forcing those through vector search
alone gives worse answers than just querying pipeline_runs directly. So this
chatbot does two things and lets the LLM decide which it needs:

  1. structured_lookup(): runs one of a small set of parameterized SQL
     queries against metadata.pipeline_runs / the wastage models
  2. doc_search(): vector search over docs/READMEs/incident notes for
     unstructured context an SQL query can't answer

This keeps the SQL path deterministic and auditable (you can always see the
exact query that produced a number) while still letting the bot answer
free-form "why did X fail" questions from doc/log context.
"""
import os

from vector_store import PgVectorStore

# Swap this import for anthropic if you want Claude to power the chatbot --
# the rest of this file is provider-agnostic aside from the client call.
from openai import OpenAI

SQL_TOOLS = {
    "top_cost_pipelines": """
        SELECT job_name, SUM(estimated_cost_usd) AS total_cost
        FROM data_plant.metadata.pipeline_runs
        WHERE status = 'success' AND ended_at >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY job_name ORDER BY total_cost DESC LIMIT 10
    """,
    "recent_failures": """
        SELECT job_name, ended_at, error_message
        FROM data_plant.metadata.pipeline_runs
        WHERE status = 'failed' AND ended_at >= current_timestamp() - INTERVAL 3 DAYS
        ORDER BY ended_at DESC
    """,
    "unused_tables": "metadata/wastage_models/unused_tables.sql",
    "runtime_trend": "metadata/wastage_models/cost_and_runtime_trend.sql",
}


def structured_lookup(spark, tool_name: str):
    query = SQL_TOOLS[tool_name]
    if query.endswith(".sql"):
        with open(query) as f:
            query = f.read()
    return spark.sql(query).toPandas().to_dict(orient="records")


def doc_search(store: PgVectorStore, question_embedding: list[float], top_k: int = 4) -> str:
    results = store.query(question_embedding, top_k=top_k)
    return "\n\n---\n\n".join(f"[{r.document.metadata.get('source')}]\n{r.document.text}" for r in results)


def embed(text: str) -> list[float]:
    client = OpenAI()
    return client.embeddings.create(model="text-embedding-3-small", input=[text]).data[0].embedding


ROUTER_SYSTEM_PROMPT = """You are the Data Plant ops assistant. You answer
questions about pipeline cost, health, and waste using two tools:

- structured_lookup(tool_name): exact numbers from metadata.pipeline_runs.
  tool_name must be one of: top_cost_pipelines, recent_failures,
  unused_tables, runtime_trend.
- doc_search(question): unstructured context from docs and incident notes,
  for "why" questions the SQL tools can't answer.

Decide which tool(s) the question needs, use them, and answer concisely
with real numbers. If a question needs data outside these tools, say so
rather than guessing.
"""


def ask(question: str, spark) -> str:
    """Minimal, non-agentic router: pick a tool based on keyword overlap.

    A real version would let the LLM call these as function-calling tools;
    this keeps the reference implementation dependency-light and easy to
    read. Swap in proper tool-calling once you're past the demo stage.
    """
    q = question.lower()

    if any(k in q for k in ["cost", "expensive", "spend"]):
        data = structured_lookup(spark, "top_cost_pipelines")
        context = f"Top cost pipelines (last 7 days): {data}"
    elif any(k in q for k in ["fail", "error", "broke"]):
        data = structured_lookup(spark, "recent_failures")
        context = f"Recent failures: {data}"
    elif any(k in q for k in ["unused", "waste", "orphan"]):
        data = structured_lookup(spark, "unused_tables")
        context = f"Unused tables: {data}"
    elif any(k in q for k in ["slow", "trend", "degrad"]):
        data = structured_lookup(spark, "runtime_trend")
        context = f"Runtime trend: {data}"
    else:
        store = PgVectorStore(dsn=os.environ.get("PGVECTOR_DSN", "postgresql://localhost:5432/data_plant"))
        context = doc_search(store, embed(question))

    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\nContext:\n{context}"},
        ],
    )
    return response.choices[0].message.content


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    question = " ".join(sys.argv[1:]) or "Which pipelines cost the most last week?"
    print(ask(question, spark))
