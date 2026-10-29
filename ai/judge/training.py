"""The labelled data of the local judge, made from the evaluation of the tuning questions.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The labels come from the approved questions, not from a new annotation: a positive
question names the unit that answers it, a negative question is one the corpus cannot
answer. The evaluation outcomes carry the top score of each retriever and the
reranker scores of the served chunks. Each part is fitted only when the data is large
enough; a part that is not fitted is listed with its reason. Library only.
"""

from __future__ import annotations

import numpy as np

from ai.judge import fit


def unit_of(chunk_id: str) -> str:
    return chunk_id.rsplit("#", 1)[0]


def gate_data(outcomes) -> tuple[list[dict], list[int]]:
    return [o.top_score for o in outcomes], [
        1 if o.expected_unit_id else 0 for o in outcomes
    ]


def relevance_data(outcomes) -> tuple[list[float], list[int]]:
    """One row per reranked chunk of a positive question: the score and whether the
    chunk belongs to the expected unit."""
    scores, labels = [], []
    for o in outcomes:
        if not o.expected_unit_id:
            continue
        for chunk_id, score in (o.notes.get("rerank", {}).get("scores", {})).items():
            scores.append(score)
            labels.append(1 if unit_of(chunk_id) == o.expected_unit_id else 0)
    return scores, labels


def router_data(store, questions, embed):
    """(centroids by ecosystem, vectors of the questions, their ecosystem labels).
    `shared` units serve every ecosystem, so they are neither a centroid nor a label."""
    page = store.collection.get(include=["embeddings", "metadatas"])
    vectors = np.asarray(page["embeddings"], dtype=float)
    groups: dict[str, list[int]] = {}
    unit_area = {}
    for index, meta in enumerate(page["metadatas"]):
        area = meta.get("ecosystem")
        unit_area[meta.get("unit_id")] = area
        if area and area != "shared":
            groups.setdefault(area, []).append(index)
    centroids = {area: vectors[rows].mean(axis=0) for area, rows in groups.items()}
    chosen = [q for q in questions if unit_area.get(q.expected_unit_id) in centroids]
    if not chosen:
        return centroids, np.zeros((0, vectors.shape[1])), []
    query = np.asarray(embed([q.text for q in chosen]), dtype=float)
    query = query / np.linalg.norm(query, axis=1, keepdims=True)
    return centroids, query, [unit_area[q.expected_unit_id] for q in chosen]


def fit_all(outcomes, questions, store=None, embed=None) -> tuple[dict, dict]:
    """(parameters of the parts that could be fitted, {part: reason} of the others)."""
    params: dict = {}
    skipped: dict = {}
    rows, labels = gate_data(outcomes)
    for part, build in (
        ("gate", lambda: fit.fit_gate(rows, labels)),
        ("relevance", lambda: fit.fit_platt(*relevance_data(outcomes))),
    ):
        try:
            params[part] = build()
        except fit.NotEnoughData as error:
            skipped[part] = str(error)
    if store is None or embed is None or getattr(store, "corpus_id", None) != "curated":
        skipped["router"] = "only the curated corpus has a router"
    else:
        try:
            centroids, vectors, area_labels = router_data(store, questions, embed)
            params["router"] = fit.fit_router(centroids, vectors, area_labels)
        except fit.NotEnoughData as error:
            skipped["router"] = str(error)
    return params, skipped
