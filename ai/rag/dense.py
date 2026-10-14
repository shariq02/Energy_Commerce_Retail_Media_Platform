"""Dense retriever: nearest chunks of a stored collection.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The query is embedded with the model that built the collection. The score is
`1 - cosine distance`; higher is better. The collection is only read. A query over
the model limit gives no hits and the status `query_too_long`; it is never cut.
Library only.
"""

from __future__ import annotations

from ai.embeddings import embedder as emb

OK = "ok"
EMPTY_QUERY = "empty_query"
QUERY_TOO_LONG = "query_too_long"


class DenseRetriever:
    def __init__(self, collection, embedder, count_tokens):
        self.collection = collection
        self.embedder = embedder
        self.count_tokens = count_tokens

    def search(
        self, text: str, where: dict | None, k: int, min_score: float | None
    ) -> tuple[str, list[tuple[str, float]]]:
        """Return (status, [(chunk id, score)]) best first; ties by chunk id."""
        if not text:
            return EMPTY_QUERY, []
        if self.count_tokens(text) > emb.MAX_TOKENS:
            return QUERY_TOO_LONG, []
        total = self.collection.count()
        if total == 0:
            return OK, []
        vector = self.embedder.embed([text])[0]
        found = self.collection.query(
            query_embeddings=[vector.tolist()],
            n_results=min(k, total),
            where=where,
            include=["distances"],
        )
        pairs = [
            (chunk_id, 1.0 - float(distance))
            for chunk_id, distance in zip(
                found["ids"][0], found["distances"][0], strict=True
            )
        ]
        if min_score is not None:
            pairs = [pair for pair in pairs if pair[1] >= min_score]
        return OK, sorted(pairs, key=lambda pair: (-pair[1], pair[0]))
