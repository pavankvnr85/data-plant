"""
Chunks a document corpus and embeds it into the configured vector store.

Two corpora feed this platform:
  --corpus product   : whatever domain docs you picked for the product RAG demo
  --corpus metadata  : pipeline docs + a text rendering of recent pipeline_runs
                        rows, so the ops chatbot can retrieve over both
                        structured stats (via SQL, see chatbot.py) and
                        unstructured context (READMEs, incident notes).

Uses Anthropic's embedding-compatible flow is not applicable here -- Claude
doesn't serve embeddings, so this uses OpenAI's embedding API as the
default. Swap `embed_texts` for any provider; nothing else changes.
"""
import argparse
import glob
import os
import uuid

from vector_store import Document, PgVectorStore

CHUNK_SIZE = 800
CHUNK_OVERLAP = 100


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end])
        start = end - overlap
    return chunks


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Swap this for whatever embedding provider you're using."""
    from openai import OpenAI

    client = OpenAI()
    response = client.embeddings.create(model="text-embedding-3-small", input=texts)
    return [d.embedding for d in response.data]


def load_corpus(path_glob: str) -> list[tuple[str, str]]:
    """Returns list of (source_path, text)."""
    out = []
    for path in glob.glob(path_glob, recursive=True):
        with open(path, "r", errors="ignore") as f:
            out.append((path, f.read()))
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus-path", required=True, help="Glob for source docs, e.g. 'docs/**/*.md'")
    parser.add_argument("--dsn", default=os.environ.get("PGVECTOR_DSN", "postgresql://localhost:5432/data_plant"))
    args = parser.parse_args()

    store = PgVectorStore(dsn=args.dsn)

    all_chunks: list[str] = []
    all_docs: list[Document] = []

    for source_path, text in load_corpus(args.corpus_path):
        for i, chunk in enumerate(chunk_text(text)):
            all_docs.append(
                Document(
                    id=str(uuid.uuid4()),
                    text=chunk,
                    metadata={"source": source_path, "chunk_index": i},
                )
            )
            all_chunks.append(chunk)

    print(f"Embedding {len(all_chunks)} chunks from {args.corpus_path}...")
    embeddings = embed_texts(all_chunks)
    store.upsert(all_docs, embeddings)
    print(f"Upserted {len(all_docs)} chunks.")


if __name__ == "__main__":
    main()
