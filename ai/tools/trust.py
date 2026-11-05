"""Trust annotation of the metric tools: the newest quality result of a source.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

For a source system the lookup reads the newest row per component and check of the
quality log in the stages analytics and gold. The trust is the worst status found:
fail over warn over pass; no rows gives unknown. The names of the checks at the worst
level are listed (at most five). The answer is kept for the life of the process, so
one source costs one query. A lookup that fails gives unknown and a warning; it never
stops the metric tool. Library only.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

from ai.tools import common as cm

STAGES = ("analytics", "gold")
FAIL = frozenset({"FAIL", "ERROR"})
PASS = frozenset({"PASS"})
ORDER = {"pass": 0, "unknown": 1, "warn": 2, "fail": 3}
TRUST_SQL = (
    "SELECT component, metric_name, status, recorded_at FROM ("
    "SELECT component, metric_name, status, recorded_at, ROW_NUMBER() OVER ("
    "PARTITION BY component, metric_name ORDER BY recorded_at DESC) AS rn "
    "FROM [[quality.quality_audit_log]] "
    "WHERE source = :source AND stage IN ('analytics', 'gold')) WHERE rn = 1"
)


def level(status: object) -> str:
    """pass, warn or fail for one status text. A status that is not known counts as
    warn, because a result nobody can read must not look clean."""
    text = str(status).upper()
    if text in FAIL:
        return "fail"
    return "pass" if text in PASS else "warn"


def _stamp(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)


def from_rows(rows: Iterable[Mapping[str, Any]]) -> cm.Trust:
    """The trust of the newest check rows of one source."""
    found = [(level(r["status"]), r) for r in rows]
    if not found:
        return cm.Trust(status="unknown")
    worst = max((lv for lv, _ in found), key=ORDER.__getitem__)
    names = sorted(
        {f"{r['component']}.{r['metric_name']}" for lv, r in found if lv == worst}
    )
    stamps = [s for s in (_stamp(r.get("recorded_at")) for _, r in found) if s]
    return cm.Trust(
        status=worst,
        failed_checks=[] if worst == "pass" else names[:5],
        checked_at=max(stamps) if stamps else None,
    )


def worst(items: Iterable[cm.Trust]) -> cm.Trust:
    """The worst trust of several sources; the checks of the worst level are joined."""
    items = list(items)
    if not items:
        return cm.Trust(status="unknown")
    top = max(items, key=lambda t: ORDER[t.status])
    same = [t for t in items if t.status == top.status]
    names = sorted({name for t in same for name in t.failed_checks})
    stamps = [t.checked_at for t in items if t.checked_at]
    return cm.Trust(
        status=top.status,
        failed_checks=names[:5],
        checked_at=max(stamps) if stamps else None,
    )


class TrustLookup:
    """The trust of source systems, read through the scope of one server."""

    def __init__(self, scope: cm.ServerScope) -> None:
        self._scope = scope
        self._template = scope.register(
            cm.SqlTemplate("trust lookup", TRUST_SQL, ("source",))
        )
        self._kept: dict[str, cm.Trust] = {}

    def get(self, source: str) -> tuple[cm.Trust, list[str]]:
        """(trust, warnings) for one source system."""
        if source in self._kept:
            return self._kept[source], []
        try:
            result = cm.get_warehouse().run(
                self._scope, self._template, {"source": source}
            )
        except Exception as exc:
            detail = cm.redact(f"{type(exc).__name__}: {exc}")
            sys.stderr.write(f"trust lookup for {source} failed: {detail}\n")
            return cm.Trust(status="unknown"), [
                f"trust of {source} could not be read; it is shown as unknown"
            ]
        trust = from_rows(result.rows)
        self._kept[source] = trust
        notes = []
        if trust.status == "unknown":
            notes.append(f"the quality log has no analytics or gold rows for {source}")
        return trust, notes

    def get_many(self, sources: Iterable[str]) -> tuple[cm.Trust, list[str]]:
        """The worst trust of several sources, with the warnings of all lookups."""
        trusts: list[cm.Trust] = []
        notes: list[str] = []
        for source in sources:
            trust, found = self.get(source)
            trusts.append(trust)
            notes += found
        return worst(trusts), notes
