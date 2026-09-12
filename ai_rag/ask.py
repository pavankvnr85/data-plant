"""
Phase 6's demo artifact: ask a question, get an answer grounded in the
ingested corpus with a citation back to the source file.

Deliberately simpler than chatbot.py (Phase 8): no SQL-vs-doc-search
routing, just straight retrieval-augmented generation over whatever
ai_rag/ingest_embeddings.py has embedded into pgvector. chatbot.py reuses
this same VectorStore for its doc_search() path later.

Local dev default: Ollama for both the embedding call (via embed_texts(),
imported from ingest_embeddings.py so there's one embedding call site, not
two) and the answer LLM -- fully offline, no API key.

Run: python ai_rag/ask.py "How does the local dev path avoid Spark?"
"""
import argparse
import os

from ingest_embeddings import OLLAMA_BASE_URL, embed_texts
from vector_store import PgVectorStore

CHAT_MODEL = "llama3.2:3b"

SYSTEM_PROMPT = """You are a helpful assistant answering questions about the
Data Plant project using only the provided context chunks. Each chunk is
labeled with its source file. Answer concisely, and cite the source
file(s) you used, e.g. "(from docs/build-plan.md)". If the context doesn't
contain the answer, say so rather than guessing.
"""


def retrieve(store: PgVectorStore, question: str, top_k: int = 4) -> list:
    [question_embedding] = embed_texts([question])
    return store.query(question_embedding, top_k=top_k)


def build_context(results: list) -> str:
    return "\n\n---\n\n".join(
        f"[{r.document.metadata.get('source')}]\n{r.document.text}" for r in results
    )


def answer(question: str, store: PgVectorStore) -> tuple[str, list]:
    from openai import OpenAI

    results = retrieve(store, question)
    if not results:
        return "No documents have been ingested yet -- run ingest_embeddings.py first.", []
    context = build_context(results)

    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Question: {question}\n\nContext:\n{context}"},
        ],
    )
    return response.choices[0].message.content, results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="+")
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "PGVECTOR_DSN", "postgresql://data_plant:data_plant@localhost:5432/data_plant"
        ),
    )
    args = parser.parse_args()

    store = PgVectorStore(dsn=args.dsn)
    answer_text, results = answer(" ".join(args.question), store)
    print(answer_text)

    if results:
        # Printed separately, not left to the LLM's prose -- a 3B local
        # model is inconsistent about actually including the citation
        # format asked for in the system prompt, but the demo artifact
        # ("answered correctly with a citation") shouldn't depend on that.
        sources = dict.fromkeys(r.document.metadata.get("source") for r in results)
        print("\nSources:")
        for source in sources:
            print(f"  - {source}")


if __name__ == "__main__":
    main()
