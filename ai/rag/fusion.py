"""Reciprocal Rank Fusion of the dense and keyword rankings.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A chunk scores the sum of `1 / (constant + rank)` over the rankings that hold it
(rank from 1). The order is the score, then the chunk id, so the same input always
gives the same output. Library only.
"""

from __future__ import annotations


def fuse(
    rankings: dict[str, list[str]], constant: int, limit: int
) -> list[tuple[str, float, dict[str, int]]]:
    """Return [(chunk id, fused score, {retriever: rank})], best first, at most `limit`."""
    scores: dict[str, float] = {}
    ranks: dict[str, dict[str, int]] = {}
    for name in sorted(rankings):
        for rank, chunk_id in enumerate(rankings[name], 1):
            scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (constant + rank)
            ranks.setdefault(chunk_id, {})[name] = rank
    ordered = sorted(scores, key=lambda chunk_id: (-scores[chunk_id], chunk_id))
    return [
        (chunk_id, scores[chunk_id], ranks[chunk_id]) for chunk_id in ordered[:limit]
    ]
