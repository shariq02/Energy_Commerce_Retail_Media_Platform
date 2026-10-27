"""Evaluation of every retrieval type on the approved questions.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Four types are judged on what they would serve, as an ordered list of units:
dense and keyword (the first `final_k` chunks of each), hybrid (the fused top
chunks) and hybrid with graph (the same, then the related units). A positive
question is a hit at rank r when its unit is the r-th unit of the list. The metrics
are Recall@1, Recall@5, Recall@context (the unit is anywhere in what is served),
MRR and nDCG@5 (one relevant unit), counted per type, per unit kind and per query
class. A negative question is answered `no_match` by a type that serves nothing.
The candidate collection is only read. With extra embedders in the config each one
also gets its own type `dense:<slug>` (its first `final_k` chunks), and the hybrid
types fuse it. Library only.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from ai.knowledge_quality.questions import Question
from ai.rag import retrieve as rv
from ai.rag import store as st
from ai.rag.config import RetrievalConfig

DENSE = "dense"
KEYWORD = "keyword"
HYBRID = "hybrid"
HYBRID_GRAPH = "hybrid_graph"
MODES = (DENSE, KEYWORD, HYBRID, HYBRID_GRAPH)
TOP = 5


def unit_of(chunk_id: str) -> str:
    return chunk_id.rsplit("#", 1)[0]


def units_in_order(chunk_ids) -> list[str]:
    return list(dict.fromkeys(unit_of(chunk_id) for chunk_id in chunk_ids))


def rank_of(units: list[str], expected: str | None) -> int | None:
    return units.index(expected) + 1 if expected in units else None


def best_score(pairs: list[tuple[str, float]], expected: str | None) -> float | None:
    scores = [score for chunk_id, score in pairs if unit_of(chunk_id) == expected]
    return max(scores) if scores else None


@dataclass
class Outcome:
    question_id: str
    cls: str
    kind: str | None
    expected_unit_id: str | None
    split: str
    # Rank of the expected unit in the served list of each type (None: not served).
    ranks: dict = field(default_factory=dict)
    # The units served by each type, for the findings.
    served: dict = field(default_factory=dict)
    # A negative question: True when the type serves nothing.
    no_match: dict = field(default_factory=dict)
    # Best score of the expected unit, and of any chunk, per retriever (no threshold).
    expected_score: dict = field(default_factory=dict)
    top_score: dict = field(default_factory=dict)
    cohort: str = "base"
    # What the reranker and the judge steps recorded for this question.
    notes: dict = field(default_factory=dict)


def evaluate_question(
    store: st.Store,
    question: Question,
    config: RetrievalConfig,
    scores=None,
    judge=None,
) -> Outcome:
    filters, notes = rv.judge_filters(judge, config, question.text, {})
    searched = rv.search_all(store, question.text, filters, config)
    direct, related, _ = rv.serve(store, searched, config, scores)
    direct, related, after = rv.judge_after(
        judge, config, question.text, direct, related, searched
    )
    notes = {**searched.notes, **notes, **after}
    expected = question.expected_unit_id
    top = config.final_k
    hybrid = units_in_order(hit.chunk_id for hit in direct)
    graph = hybrid + [
        unit
        for unit in units_in_order(h.chunk_id for h in related)
        if unit not in hybrid
    ]
    served = {
        DENSE: units_in_order(c for c, _ in searched.dense[:top]),
        KEYWORD: units_in_order(c for c, _ in searched.keyword[:top]),
        HYBRID: hybrid,
        HYBRID_GRAPH: graph,
    }
    found = {DENSE: searched.dense, KEYWORD: searched.keyword}
    for slug, (_status, hits) in searched.extra.items():
        name = rv.extra_name(slug)
        served[name] = units_in_order(c for c, _ in hits[:top])
        found[name] = hits
    return Outcome(
        question_id=question.id,
        cls=question.cls,
        kind=question.kind,
        expected_unit_id=expected,
        split=question.split,
        ranks={mode: rank_of(units, expected) for mode, units in served.items()},
        served={mode: units[:8] for mode, units in served.items()},
        no_match={mode: not units for mode, units in served.items()},
        expected_score={n: best_score(p, expected) for n, p in found.items()},
        top_score={n: max((s for _, s in p), default=None) for n, p in found.items()},
        cohort=question.cohort,
        notes=notes,
    )


def evaluate(
    store: st.Store,
    questions: list[Question],
    config: RetrievalConfig,
    progress=None,
    scores=None,
    judge=None,
) -> list[Outcome]:
    outcomes = []
    for number, question in enumerate(questions, 1):
        outcomes.append(evaluate_question(store, question, config, scores, judge))
        if progress and number % 10 == 0:
            progress(f"  [questions] {number}/{len(questions)}")
    return outcomes


def metrics(outcomes: list[Outcome], mode: str) -> dict:
    """Counts for one type; the rates are computed where they are shown."""
    positive = [o for o in outcomes if o.expected_unit_id]
    negative = [o for o in outcomes if not o.expected_unit_id]
    ranks = [o.ranks[mode] for o in positive]
    return {
        "positive": len(positive),
        "recall_1": sum(1 for r in ranks if r == 1),
        "recall_5": sum(1 for r in ranks if r is not None and r <= TOP),
        "recall_context": sum(1 for r in ranks if r is not None),
        "mrr_sum": sum(1 / r for r in ranks if r),
        "ndcg_5_sum": sum(1 / math.log2(r + 1) for r in ranks if r and r <= TOP),
        "negative": len(negative),
        "no_match": sum(1 for o in negative if o.no_match[mode]),
    }


def grouped(outcomes: list[Outcome]) -> dict:
    """Metrics for all questions, per unit kind and per class, for every type."""
    groups: dict[str, list[Outcome]] = {"all": outcomes}
    for outcome in outcomes:
        if outcome.expected_unit_id:
            groups.setdefault(f"kind:{outcome.kind}", []).append(outcome)
        groups.setdefault(f"class:{outcome.cls}", []).append(outcome)
    modes = list(outcomes[0].ranks) if outcomes else list(MODES)
    return {
        name: {mode: metrics(items, mode) for mode in modes}
        for name, items in sorted(groups.items())
    }


def run_id(corpus_id: str, split: str, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"{corpus_id}-{split}-{stamp}"


def run_record(
    store: st.Store,
    corpus_id: str,
    split: str,
    outcomes: list[Outcome],
    question_set,
    config: RetrievalConfig,
    config_version: int | None,
    judge: dict | None = None,
) -> dict:
    """The stored record of an evaluation run; it holds every outcome."""
    record = store.record
    return {
        "run_id": run_id(corpus_id, split),
        "created": datetime.now(UTC).isoformat(timespec="seconds"),
        "corpus_id": corpus_id,
        "split": split,
        "build_id": record["build_id"],
        "collection": record["collection"],
        "corpus_version": record["corpus_version"],
        "corpus_fingerprint": record["corpus_fingerprint"],
        "model": record["model"],
        "chunk_count": record["chunk_count"],
        "config": config.as_dict(),
        "config_version": config_version,
        "question_set": {
            "version": question_set.version,
            "hash": question_set.set_hash,
            "questions": len(outcomes),
        },
        "extra_collections": {
            slug: found["collection"] for slug, found in store.extra_records.items()
        },
        "judge": judge,
        "rerank_missing": sum(
            o.notes.get("rerank", {}).get("missing", 0) for o in outcomes
        ),
        "metrics": grouped(outcomes),
        "outcomes": [asdict(o) for o in outcomes],
    }
