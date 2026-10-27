"""Score thresholds from the tuning questions, and the retrieval config versions.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

For the dense and for the keyword retriever the threshold is the highest score that
still keeps at least 95% of the expected hits of the tuning questions (a hit: the
expected unit is among the candidates). Then the negative questions that the
threshold would turn into `no_match` are counted. When their share is below the
target (4 of 5, the rate of the acceptance rule) the two goals conflict; both
numbers are recorded and the owner decides. A config version is a file in
`ai/knowledge_quality/config/<corpus>/`; it is never edited, a new version is
written. Library only.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

from ai.knowledge_quality.evaluation import DENSE, KEYWORD, Outcome
from ai.rag.config import RetrievalConfig
from scripts.knowledge import _unit_common as uc

RETENTION = 0.95
NEGATIVE_TARGET = 4 / 5
FIELDS = {DENSE: "dense_min_score", KEYWORD: "keyword_min_score"}
_VERSION_FILE = re.compile(r"^retrieval_config_v([0-9]{3})\.json$")


def config_dir(ai_dir: Path, corpus_id: str) -> Path:
    return ai_dir / "knowledge_quality" / "config" / corpus_id


def threshold_for(scores: list[float], retention: float = RETENTION) -> float | None:
    """The highest score (to 4 places) that keeps `retention` of the scores."""
    if not scores:
        return None
    ordered = sorted(scores, reverse=True)
    value = ordered[math.ceil(retention * len(ordered)) - 1]
    # Rounded down to 4 places: the kept share does not fall, and float noise in
    # a holdout score cannot push a hit just under the threshold.
    return math.floor(round(value * 10**4, 6)) / 10**4


def establish(outcomes: list[Outcome], overrides: dict | None = None) -> dict:
    """For each retriever: the threshold, the hits it keeps and the negatives it stops.

    `overrides` maps a retriever to a value chosen by the owner. The retention rule
    still gives its value (`computed_threshold`); the chosen value is used, its
    effect is counted, and a conflict of goals is not reported for it, because the
    owner decided.
    """
    overrides = overrides or {}
    result = {}
    positive = [o for o in outcomes if o.expected_unit_id]
    negative = [o for o in outcomes if not o.expected_unit_id]
    names = [DENSE, KEYWORD]
    for outcome in outcomes:
        names += [n for n in outcome.expected_score if n not in names]
    for retriever in names:
        scores = [
            o.expected_score[retriever]
            for o in positive
            if o.expected_score[retriever] is not None
        ]
        computed = threshold_for(scores)
        owner_set = overrides.get(retriever) is not None
        value = round(overrides[retriever], 4) if owner_set else computed
        kept = sum(1 for score in scores if value is not None and score >= value)
        stopped = sum(
            1
            for o in negative
            if value is not None
            and (o.top_score[retriever] is None or o.top_score[retriever] < value)
        )
        rate = stopped / len(negative) if negative else None
        result[retriever] = {
            "threshold": value,
            "computed_threshold": computed,
            "set_by": "owner" if owner_set else "rule",
            "expected_hits": len(scores),
            "hits_kept": kept,
            "negatives": len(negative),
            "negatives_no_match": stopped,
            "negative_rate": rate,
            "conflict": rate is not None and rate < NEGATIVE_TARGET and not owner_set,
        }
    return result


def versions(ai_dir: Path, corpus_id: str) -> list[int]:
    folder = config_dir(ai_dir, corpus_id)
    found = [
        _VERSION_FILE.match(p.name) for p in folder.glob("retrieval_config_v*.json")
    ]
    return sorted(int(m.group(1)) for m in found if m)


def read_version(ai_dir: Path, corpus_id: str, number: int) -> dict:
    path = config_dir(ai_dir, corpus_id) / f"retrieval_config_v{number:03d}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def latest(ai_dir: Path, corpus_id: str) -> tuple[int | None, RetrievalConfig]:
    """The newest config version, or the defaults when there is none."""
    found = versions(ai_dir, corpus_id)
    if not found:
        return None, RetrievalConfig()
    data = read_version(ai_dir, corpus_id, found[-1])
    return found[-1], RetrievalConfig.from_dict(data["config"])


def write_version(
    ai_dir: Path, corpus_id: str, base: RetrievalConfig, found: dict, meta: dict
) -> tuple[int, dict]:
    """Write the next config version: `base` with the established thresholds."""
    values = {FIELDS[name]: found[name]["threshold"] for name in FIELDS}
    extra = {
        name.split(":", 1)[1]: item["threshold"]
        for name, item in found.items()
        if name.startswith("dense:") and item["threshold"] is not None
    }
    if extra:
        values["extra_min_scores"] = extra
    config = RetrievalConfig.from_dict({**base.as_dict(), **values})
    number = (versions(ai_dir, corpus_id) or [0])[-1] + 1
    data = {
        "version": number,
        "config": config.as_dict(),
        "retention": RETENTION,
        "negative_target": NEGATIVE_TARGET,
        "thresholds": found,
        **meta,
    }
    text = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    path = config_dir(ai_dir, corpus_id) / f"retrieval_config_v{number:03d}.json"
    uc.write_text(path, text)
    return number, data
