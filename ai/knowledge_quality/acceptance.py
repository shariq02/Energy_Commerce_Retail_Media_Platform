"""The acceptance gate: may a candidate become the designated build?

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The rule is judged on the holdout run of the candidate, for hybrid with graph (what
production serves). Provisional values, as counts on a holdout of about 25 positive
and 5 negative questions, scaled to the real holdout size: at least 23/25 of the
positive questions found in the top 5; at most 1 additional miss against the run of
the designated build (not applicable to the first designation or a changed question
set); at least 4/5 of the negative questions answered `no_match`; every integrity
check passes; and the owner approves. A candidate that fails a check cannot be
designated. Library only.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from ai.knowledge_quality.evaluation import HYBRID_GRAPH, TOP

POSITIVE_RATE = 23 / 25
NEGATIVE_RATE = 4 / 5
MAX_EXTRA_MISSES = 1
MODE = HYBRID_GRAPH


@dataclass
class Decision:
    checks: list[dict] = field(default_factory=list)

    def add(self, name: str, passed: bool | None, detail: str) -> None:
        self.checks.append({"check": name, "passed": passed, "detail": detail})

    @property
    def ok(self) -> bool:
        return all(c["passed"] is not False for c in self.checks)


def needed(rate: float, count: int) -> int:
    """The least number of hits for a rate; rounded first so 0.92 x 25 is 23."""
    return math.ceil(round(rate * count, 9))


def misses(run: dict) -> set[str]:
    """Positive questions whose unit is not in the top 5 of the served list."""
    return {
        o["question_id"]
        for o in run["outcomes"]
        if o["expected_unit_id"]
        and not (o["ranks"][MODE] is not None and o["ranks"][MODE] <= TOP)
    }


def decide(
    run: dict,
    previous_run: dict | None,
    integrity_problems: list[str],
    base_run: dict | None = None,
    baseline_runs: tuple = (),
) -> Decision:
    decision = Decision()
    total = run["metrics"]["all"][MODE]
    need_positive = needed(POSITIVE_RATE, total["positive"])
    found = total["recall_5"]
    decision.add(
        "positive questions in the top 5",
        found >= need_positive,
        f"{found} of {total['positive']} found, at least {need_positive} needed",
    )
    need_negative = needed(NEGATIVE_RATE, total["negative"])
    decision.add(
        "negative questions answered no_match",
        total["no_match"] >= need_negative,
        f"{total['no_match']} of {total['negative']} answered, at least "
        f"{need_negative} needed",
    )
    if previous_run is None:
        decision.add(
            "additional misses against the previous build",
            None,
            "not applicable: first designation",
        )
    elif previous_run["question_set"]["hash"] != run["question_set"]["hash"]:
        decision.add(
            "additional misses against the previous build",
            None,
            "not applicable: the question set changed",
        )
    else:
        extra = sorted(misses(run) - misses(previous_run))
        decision.add(
            "additional misses against the previous build",
            len(extra) <= MAX_EXTRA_MISSES,
            f"{len(extra)} additional miss(es), at most {MAX_EXTRA_MISSES} allowed"
            + (f": {extra}" if extra else ""),
        )
    if base_run is not None and baseline_runs:
        before = set().union(*(misses(r) for r in baseline_runs))
        extra = sorted(misses(base_run) - before)
        decision.add(
            "old questions: additional misses against the designated build",
            len(extra) <= MAX_EXTRA_MISSES,
            f"{len(extra)} additional miss(es) on {len(base_run['outcomes'])} old "
            f"questions (seen data), at most {MAX_EXTRA_MISSES} allowed"
            + (f": {extra}" if extra else ""),
        )
    elif base_run is not None:
        decision.add(
            "old questions: additional misses against the designated build",
            None,
            "not applicable: no designated build to compare with",
        )
    decision.add(
        "integrity checks",
        not integrity_problems,
        "all pass" if not integrity_problems else "; ".join(integrity_problems[:3]),
    )
    return decision
