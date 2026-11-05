"""Retrieval parameters in one object.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Every value that changes a result is here, and the whole object is copied into
each result. A score threshold of None is off. The fields after
`expansion_terms_max` belong to the extension (a second dense retriever, a reranker,
a judge); each is off by default, so a config without them gives the old result.
Library only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class RetrievalConfig:
    dense_k: int = 20
    keyword_k: int = 20
    rrf_constant: int = 60
    final_k: int = 5
    related_max: int = 3
    dense_min_score: float | None = None
    keyword_min_score: float | None = None
    expansion_terms_max: int = 8
    # Extension switches, all off by default.
    embedders: tuple = ()  # slugs of the extra dense retrievers
    extra_min_scores: dict | None = None  # slug -> score threshold
    rerank: bool = False
    rerank_candidates: int = 20
    rerank_model: str = "cross-encoder/ms-marco-MiniLM-L6-v2"
    # The local judge, fitted on the build; each switch is off by default.
    judge_filter: bool = False
    judge_filter_min: float = 0.7
    judge_gate: bool = False
    judge_gate_min: float = 0.5
    judge_relevance: bool = False
    judge_relevance_min: float = 0.5

    def __post_init__(self) -> None:
        object.__setattr__(self, "embedders", tuple(self.embedders))
        for name in ("dense_k", "keyword_k", "rrf_constant", "final_k"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.related_max < 0 or self.expansion_terms_max < 0:
            raise ValueError("related_max and expansion_terms_max must not be negative")
        if self.rerank and self.rerank_candidates < self.final_k:
            raise ValueError("rerank_candidates must be at least final_k")

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> RetrievalConfig:
        return cls(**data)
