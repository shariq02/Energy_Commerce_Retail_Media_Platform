"""Keyword retriever over the curated corpus (BM25, in memory).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A keyword hit is a chunk that holds at least one query term; a chunk with none is
never returned, whatever its score. The filter is applied before ranking. The
larger Wikipedia corpus uses the SQLite index in `keyword_fts`. Library only.
"""

from __future__ import annotations

from ai.rag import filters as flt

OK = "ok"
NO_TERMS = "no_terms"


class Bm25Keyword:
    def __init__(self, chunk_ids: list[str], bm25, metadata: dict[str, dict]):
        self.chunk_ids = chunk_ids
        self.bm25 = bm25
        self.metadata = metadata

    def search(
        self,
        terms: tuple[str, ...],
        filters: dict[str, list],
        k: int,
        min_score: float | None,
    ) -> tuple[str, list[tuple[str, float]]]:
        """Return (status, [(chunk id, score)]) best first; ties by chunk id."""
        if not terms:
            return NO_TERMS, []
        scores = self.bm25.get_scores(list(terms))
        pairs = []
        for position, chunk_id in enumerate(self.chunk_ids):
            if not any(term in self.bm25.doc_freqs[position] for term in terms):
                continue
            if not flt.matches(self.metadata[chunk_id], filters):
                continue
            score = float(scores[position])
            if min_score is None or score >= min_score:
                pairs.append((chunk_id, score))
        pairs.sort(key=lambda pair: (-pair[1], pair[0]))
        return OK, pairs[:k]
