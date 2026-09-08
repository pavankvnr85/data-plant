"""
Vector store interface. Every RAG component (ops chatbot, product RAG)
talks to this interface, never to pgvector or Databricks Vector Search
directly. That's the concrete "swap a component without touching pipeline
logic" story for this layer -- add a DatabricksVectorSearchStore class
later and nothing else in ai_rag/ changes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Document:
    id: str
    text: str
    metadata: dict


@dataclass
class ScoredDocument:
    document: Document
    score: float


class VectorStore(ABC):
    @abstractmethod
    def upsert(self, documents: list[Document], embeddings: list[list[float]]) -> None:
        ...

    @abstractmethod
    def query(self, embedding: list[float], top_k: int = 5) -> list[ScoredDocument]:
        ...


class PgVectorStore(VectorStore):
    """Local/self-hosted implementation backed by Postgres + pgvector.

    Requires the pgvector extension and a table matching the schema below:

    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE TABLE documents (
        id TEXT PRIMARY KEY,
        text TEXT,
        metadata JSONB,
        embedding VECTOR(1536)
    );
    """

    def __init__(self, dsn: str, table: str = "documents", dims: int = 1536):
        import psycopg2  # local import so this module imports cleanly without the driver installed

        self.conn = psycopg2.connect(dsn)
        self.table = table
        self.dims = dims

    def upsert(self, documents: list[Document], embeddings: list[list[float]]) -> None:
        import json

        with self.conn.cursor() as cur:
            for doc, emb in zip(documents, embeddings):
                cur.execute(
                    f"""
                    INSERT INTO {self.table} (id, text, metadata, embedding)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET text = EXCLUDED.text,
                        metadata = EXCLUDED.metadata,
                        embedding = EXCLUDED.embedding
                    """,
                    (doc.id, doc.text, json.dumps(doc.metadata), emb),
                )
        self.conn.commit()

    def query(self, embedding: list[float], top_k: int = 5) -> list[ScoredDocument]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, text, metadata, 1 - (embedding <=> %s::vector) AS score
                FROM {self.table}
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (embedding, embedding, top_k),
            )
            rows = cur.fetchall()

        return [
            ScoredDocument(document=Document(id=r[0], text=r[1], metadata=r[2]), score=r[3])
            for r in rows
        ]


# Add a DatabricksVectorSearchStore(VectorStore) here later if you want the
# "stays inside the Databricks platform" version instead of self-hosted
# pgvector -- same interface, same call sites in chatbot.py.
