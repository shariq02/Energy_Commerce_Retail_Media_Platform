"""The retrieval stack of a corpus: which switches of the extension are on.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

One tracked file, `ai/knowledge_quality/config/<corpus>/stack.yml`, names the extra
dense retrievers, the reranker and the judge switches. The parent commands read it, so
they need no options. A key that is missing keeps its default (the second embedder on
for the curated corpus, everything else off); an unknown key or embedder is an error.
Library only.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from ai.embeddings import embedder as emb
from ai.knowledge_quality.thresholds import config_dir
from ai.rag.config import RetrievalConfig

STACK_FILE = "stack.yml"
JUDGE_SWITCHES = ("judge_filter", "judge_gate", "judge_relevance")
DEFAULT_EMBEDDERS = {"curated": [emb.GTE_MODERNBERT.slug]}


def stack_path(ai_dir: Path, corpus_id: str) -> Path:
    return config_dir(ai_dir, corpus_id) / STACK_FILE


def defaults(corpus_id: str) -> dict:
    return {
        "embedders": list(DEFAULT_EMBEDDERS.get(corpus_id, [])),
        "rerank": False,
        "rerank_candidates": None,
        **{name: False for name in JUDGE_SWITCHES},
    }


def load(ai_dir: Path, corpus_id: str) -> dict:
    """The stack of the corpus: the defaults, changed by the file when it exists."""
    stack = defaults(corpus_id)
    path = stack_path(ai_dir, corpus_id)
    if path.is_file():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        unknown = sorted(set(data) - set(stack))
        if unknown:
            raise ValueError(f"{path}: unknown key(s) {unknown}; use {sorted(stack)}")
        stack.update(data)
    known = {s for s in emb.PROFILES if s != emb.MINILM.slug}
    bad = sorted(set(stack["embedders"]) - known)
    if bad:
        raise ValueError(f"{path}: unknown embedder(s) {bad}; use {sorted(known)}")
    if stack["embedders"] and corpus_id != "curated":
        raise ValueError(
            f"{path}: only the curated corpus has a second dense collection"
        )
    return stack


def judge_on(stack: dict) -> bool:
    return any(stack[name] for name in JUDGE_SWITCHES)


def apply(config: RetrievalConfig, stack: dict) -> RetrievalConfig:
    """The config with the switches of the stack; a threshold of a removed embedder goes."""
    embedders = tuple(stack["embedders"])
    scores = {
        k: v for k, v in (config.extra_min_scores or {}).items() if k in embedders
    }
    changes = {
        "embedders": embedders,
        "extra_min_scores": scores or None,
        "rerank": bool(stack["rerank"]),
        **{name: bool(stack[name]) for name in JUDGE_SWITCHES},
    }
    if stack["rerank_candidates"]:
        changes["rerank_candidates"] = int(stack["rerank_candidates"])
    return RetrievalConfig.from_dict({**config.as_dict(), **changes})
