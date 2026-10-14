"""Retrieval parameters in one object.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Every value that changes a result is here, and the whole object is copied into
each result. A score threshold of None is off. Library only.
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

    def __post_init__(self) -> None:
        for name in ("dense_k", "keyword_k", "rrf_constant", "final_k"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.related_max < 0 or self.expansion_terms_max < 0:
            raise ValueError("related_max and expansion_terms_max must not be negative")

    def as_dict(self) -> dict:
        return asdict(self)
