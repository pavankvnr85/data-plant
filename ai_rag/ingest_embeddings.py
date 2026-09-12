"""
Chunks a document corpus and embeds it into the configured vector store.

Two corpora feed this platform:
  --corpus product   : whatever domain docs you picked for the product RAG demo
  --corpus metadata  : pipeline docs + a text rendering of recent pipeline_runs
                        rows, so the ops chatbot can retrieve over both
                        structured stats (via SQL, see chatbot.py) and
                        unstructured context (READMEs, incident notes).

Local dev default: Ollama's `nomic-embed-text`, running fully offline, no
API key. Ollama serves an OpenAI-compatible `/v1/embeddings` endpoint, so
this still goes through the `openai` client already in requirements.txt --
just pointed at localhost with a dummy key -- rather than needing a new
Python dependency. Swap `embed_texts` for a real provider (e.g. OpenAI's
`text-embedding-3-small`) later; nothing else changes, aside from also
updating `PgVectorStore`'s `dims` to match the new model's output size.
"""
import argparse
import glob
import os
import uuid

from vector_store import Document, PgVectorStore

CHUNK_SIZE = 800
CHUNK_OVERLAP = 100

OLLAMA_BASE_URL = "http://localhost:11434/v1"
EMBEDDING_MODEL = "nomic-embed-text"


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

    client = OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
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
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "PGVECTOR_DSN", "postgresql://data_plant:data_plant@localhost:5432/data_plant"
        ),
    )
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
