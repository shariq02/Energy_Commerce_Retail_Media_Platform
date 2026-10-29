"""The local judge: calibrated answers from the fitted parameters of a build.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

It answers the three questions that retrieval asks of a judge, with a probability or a
list of ecosystems, and gives None when the part was not fitted or the input is
missing, so retrieval keeps the rule-based result. It makes no network call. The
parameters are read from the file that `ai.judge.factory` stores in the build folder.
Library only.
"""

from __future__ import annotations

import numpy as np

from ai.judge.fit import sigmoid


class LocalJudge:
    name = "local"

    def __init__(self, params: dict, embed=None):
        self.params = params
        self.embed = embed

    def parts(self) -> list[str]:
        return sorted(p for p in ("relevance", "gate", "router") if self.params.get(p))

    def relevance(
        self, query: str, text: str, rerank_score: float | None
    ) -> float | None:
        part = self.params.get("relevance")
        if not part or rerank_score is None:
            return None
        return float(sigmoid(part["a"] * rerank_score + part["b"]))

    def answerable(self, query: str, context: str, top_scores: dict) -> float | None:
        part = self.params.get("gate")
        if not part:
            return None
        values = np.array(
            [top_scores.get(name) or 0.0 for name in part["features"]], dtype=float
        )
        z = (values - np.array(part["mean"])) / np.array(part["scale"])
        return float(sigmoid(z @ np.array(part["coefficients"]) + part["intercept"]))

    def area_filter(self, query: str, minimum: float) -> list[str] | None:
        part = self.params.get("router")
        if not part or self.embed is None or not query.strip():
            return None
        vector = np.asarray(self.embed([query])[0], dtype=float)
        z = (np.array(part["centroids"]) @ vector) / part["temperature"]
        z = z - z.max()
        probs = np.exp(z) / np.exp(z).sum()
        best = int(probs.argmax())
        if probs[best] < minimum:
            return None
        return sorted({part["labels"][best], "shared"})

    def summary(self) -> dict:
        return {"backend": self.name, "parts": self.parts()}
