"""Cached reranker scores and the reorder step of retrieval.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A cross-encoder scores (query, chunk text) pairs. The scores are made in a
Databricks notebook and stored in a cache file keyed by the model, the query hash and
the chunk text hash, so a query on a build gives the same result every time and the
laptop never runs the model. When a candidate has no stored score the order of the
fusion is kept and the status says how many scores were missing; nothing is guessed.
Library only.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.knowledge import _unit_common as uc

CACHE_NAME = "scores.jsonl"
RERANKED = "ok"
MISSING = "rerank_missing"


def cache_path(ai_dir: Path) -> Path:
    return ai_dir / "vector_store" / "data" / "rerank" / CACHE_NAME


def pair_key(model: str, query: str, text: str) -> str:
    """The key of a pair: model id, query hash and chunk text hash."""
    return uc.sha256_text(f"{model}|{uc.sha256_text(query)}|{uc.sha256_text(text)}")


def load_scores(path: Path) -> dict[str, float]:
    if not path.is_file():
        return {}
    scores = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            scores[row["key"]] = float(row["score"])
    return scores


def save_scores(path: Path, scores: dict[str, float]) -> None:
    lines = [
        json.dumps({"key": key, "score": scores[key]}, sort_keys=True)
        for key in sorted(scores)
    ]
    uc.write_text(path, "\n".join(lines) + ("\n" if lines else ""))


def reorder(
    model: str,
    query: str,
    fused: list,
    texts: dict[str, str],
    scores: dict[str, float],
    final_k: int,
) -> tuple[list, str, int]:
    """(fused entries in the new order, status, missing scores).

    `fused` is [(chunk id, fused score, ranks)]; with every score present the entries
    are ordered by the cross-encoder score, then the chunk id, and cut to `final_k`.
    """
    keys = {c: pair_key(model, query, texts[c]) for c, _, _ in fused}
    missing = [c for c in keys if keys[c] not in scores]
    if missing:
        return fused[:final_k], MISSING, len(missing)
    ordered = sorted(fused, key=lambda row: (-scores[keys[row[0]]], row[0]))
    return ordered[:final_k], RERANKED, 0
