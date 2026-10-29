"""A warehouse double for the tool tests: answers by template name, records calls.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026
"""

from __future__ import annotations

from ai.tools import common as cm


class FakeWarehouse:
    """`answers` maps a template name to its rows, or to a function of the params."""

    def __init__(self, answers: dict):
        self.answers = answers
        self.calls: list[tuple[str, dict, int]] = []

    def run(self, scope, template, params=None, *, row_cap=cm.ROW_CAP):
        scope.register(template)
        given = dict(params or {})
        self.calls.append((template.name, given, row_cap))
        rows = self.answers[template.name]
        rows = rows(given) if callable(rows) else rows
        return cm.QueryResult(
            rows=list(rows[:row_cap]),
            truncated=len(rows) > row_cap,
            elapsed_ms=1,
        )

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]
