"""Score thresholds from the tuning questions, and the retrieval config versions.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The thresholds of all retrievers are searched together on the tuning questions. A
hybrid list is withheld when any retriever has no candidate above its threshold, so
the search keeps as many positive questions in the top 5 as possible while at least
85% of the negative questions end in `no_match` (the acceptance rule asks for 4 of
5). When the hits that stay fall under 85% of the hits without thresholds the two
goals conflict; the value of the 95% retention rule per retriever is then recorded
and the owner decides. A threshold chosen by the owner replaces the search for that
retriever. A config version is a file in `ai/knowledge_quality/config/<corpus>/`; it
is never edited, a new version is written. Library only.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

from ai.knowledge_quality.evaluation import DENSE, HYBRID_GRAPH, KEYWORD, TOP, Outcome
from ai.rag.config import RetrievalConfig
from scripts.knowledge import _unit_common as uc

RETENTION = 0.95
MIN_RETENTION = 0.85
NEGATIVE_TARGET = 4 / 5
SEARCH_TARGET = 0.85
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


def _just_above(value: float) -> float:
    """The lowest 4-place threshold above `value`: it turns `value` into a miss."""
    return (math.floor(round(value * 10**4, 6)) + 1) / 10**4


def _lowest(value: float) -> float:
    """`value` rounded down to 4 places: a threshold that turns nothing under it away."""
    return math.floor(round(value * 10**4, 6)) / 10**4


@dataclass
class Joint:
    """The thresholds of all retrievers chosen together, and what they do to the hybrid
    list: the positive questions that stay in the top 5 (`hits`, against `base_hits`
    without thresholds) and the negative questions that end in `no_match`."""

    values: dict
    hits: int
    base_hits: int
    stopped: int
    failed: bool


def search_thresholds(
    positive: list[Outcome],
    negative: list[Outcome],
    names: list[str],
    fixed: dict[str, float],
    floor: float = MIN_RETENTION,
    target: float = SEARCH_TARGET,
) -> Joint:
    """Choose one threshold per retriever so that the hybrid list keeps as many hits as
    possible while `target` of the negative questions end in `no_match`.

    A hybrid list is withheld when any retriever has no candidate above its threshold
    (the best score of the retriever is under it), so the thresholds act together and
    are chosen together, from the best score of every tuning question. Start: each
    threshold at the lowest best score seen (it withholds nothing); then raise, one
    retriever at a time, the step that stops most negatives per hit lost, until the
    target is met; then lower each threshold again while the target holds and a hit
    comes back. `fixed` holds values chosen by the owner. `failed` is True when the
    hits that stay are under `floor` of the hits without thresholds. The effect on the
    hybrid list is read from the ranks of a run without thresholds; the removal of
    single candidates below a threshold is not simulated.
    """
    top = {
        n: (
            [o.top_score.get(n) for o in positive],
            [o.top_score.get(n) for o in negative],
        )
        for n in names
    }
    hits = [
        o.ranks.get(HYBRID_GRAPH) is not None and o.ranks[HYBRID_GRAPH] <= TOP
        for o in positive
    ]
    base_hits = sum(hits)
    need = math.ceil(round(target * len(negative), 9))

    def lost(scores, limits):
        # True for each question that a retriever withholds from the hybrid list.
        return [
            any(s[i] is None or s[i] < limits[n] for n, s in scores.items())
            for i in range(len(next(iter(scores.values()))))
        ]

    def effect(limits):
        out_p = lost({n: top[n][0] for n in names}, limits)
        out_n = lost({n: top[n][1] for n in names}, limits)
        kept = sum(1 for hit, gone in zip(hits, out_p, strict=True) if hit and not gone)
        return kept, sum(out_n)

    seen = {
        n: sorted({s for s in top[n][0] + top[n][1] if s is not None}) for n in names
    }
    steps = {n: [_just_above(s) for s in seen[n]] for n in names if n not in fixed}
    limits = {n: fixed.get(n) for n in names}
    for n in names:
        if n not in fixed:
            limits[n] = _lowest(seen[n][0]) if seen[n] else -math.inf
    kept, stopped = effect(limits)
    while stopped < need:
        best = None
        for n, options in steps.items():
            for value in options:
                if value <= limits[n]:
                    continue
                kept2, stopped2 = effect({**limits, n: value})
                gain = stopped2 - stopped
                if gain > 0:
                    key = (gain / (kept - kept2 + 1), -value)
                    if best is None or key > best[0]:
                        best = (key, n, value)
        if best is None:
            break
        _, n, value = best
        limits[n] = value
        kept, stopped = effect(limits)
    improved = True
    while improved:
        improved = False
        for n, options in steps.items():
            lower = [_lowest(seen[n][0]) if seen[n] else -math.inf]
            for value in lower + [v for v in options if v < limits[n]]:
                kept2, stopped2 = effect({**limits, n: value})
                if stopped2 >= need and kept2 > kept:
                    limits[n], kept, stopped, improved = value, kept2, stopped2, True
    failed = stopped < need or kept < floor * base_hits
    values = {n: (None if limits[n] == -math.inf else limits[n]) for n in names}
    return Joint(values, kept, base_hits, stopped, failed)


def establish(
    outcomes: list[Outcome],
    overrides: dict | None = None,
    floor: float = MIN_RETENTION,
) -> dict:
    """For each retriever: the threshold, the hits it keeps and the negatives it stops.

    With negative questions the thresholds are searched together (`search_thresholds`;
    `set_by` is `search`). The retention rule still gives a value per retriever
    (`computed_threshold`); it is used, with the conflict reported, only when the
    search cannot keep `floor` of the hybrid hits and stop the negatives. `overrides`
    maps a retriever to a value chosen by the owner: it is used, the others are still
    searched, and no conflict is reported for the owner's value.
    """
    overrides = overrides or {}
    result = {}
    positive = [o for o in outcomes if o.expected_unit_id]
    negative = [o for o in outcomes if not o.expected_unit_id]
    names = [DENSE, KEYWORD]
    for outcome in outcomes:
        names += [n for n in outcome.expected_score if n not in names]
    fixed = {n: round(overrides[n], 4) for n in names if overrides.get(n) is not None}
    joint = None
    hybrid = None
    if negative:
        joint = search_thresholds(positive, negative, names, fixed, floor)
        hybrid = {
            "hits": joint.hits,
            "hits_without_thresholds": joint.base_hits,
            "negatives_no_match": joint.stopped,
        }
    for retriever in names:
        scores = [
            o.expected_score[retriever]
            for o in positive
            if o.expected_score[retriever] is not None
        ]
        computed = threshold_for(scores)
        owner_set = retriever in fixed
        value, set_by = computed, "rule"
        if owner_set:
            value, set_by = fixed[retriever], "owner"
        elif joint is not None and not joint.failed:
            value, set_by = joint.values[retriever], "search"
        kept = sum(1 for score in scores if value is None or score >= value)
        stopped = sum(
            1
            for o in negative
            if o.top_score[retriever] is None
            or (value is not None and o.top_score[retriever] < value)
        )
        rate = stopped / len(negative) if negative else None
        failed = joint is not None and joint.failed and not owner_set
        result[retriever] = {
            "threshold": value,
            "computed_threshold": computed,
            "set_by": set_by,
            "expected_hits": len(scores),
            "hits_kept": kept,
            "negatives": len(negative),
            "negatives_no_match": stopped,
            "negative_rate": rate,
            "conflict": failed,
            "hybrid": hybrid,
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
        "min_retention": MIN_RETENTION,
        "negative_target": NEGATIVE_TARGET,
        "thresholds": found,
        **meta,
    }
    text = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    path = config_dir(ai_dir, corpus_id) / f"retrieval_config_v{number:03d}.json"
    uc.write_text(path, text)
    return number, data
