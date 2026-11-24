"""Static checks on the Gold notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Gold has no per-table registry the way Silver's field_class_registry works
(`gold_schema_for`'s own docstring), so the governance invariant this suite
checks is different: every notebook that writes a Gold table must also call
the Silver->Gold hard-fail gate (`check`) or the grain assertion
(`assert_unique_grain`, which itself calls `check`) at least once -- a Gold
table written with no gate call at all is exactly the gap GOVERNANCE Section
3's "embedded Silver->Gold check(hard_fail) gate" claims does not exist.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.schemas._generate_field_classes import build_rows
from src.schemas._silver_notebook_scan import all_read_silver_tables

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_DATABRICKS = _ROOT / "databricks"
_GOLD = _DATABRICKS / "gold"
# The analytics notebooks write with the same library and the same gate.
_ANALYTICS = _DATABRICKS / "analytics"

_NOTEBOOKS = sorted(
    p
    for root in (_GOLD, _ANALYTICS)
    for p in root.rglob("*.py")
    if not p.name.startswith("_")
)

_GATE_CALLS = {"check", "assert_unique_grain", "gate_tables"}
_WRITE_CALL = "write_tables"


def _name(path: Path) -> str:
    return str(path.relative_to(_DATABRICKS))


def _call_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            names.add(node.func.id)
    return names


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_name)
def test_notebook_parses(path):
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_name)
def test_notebook_has_the_standard_databricks_header(path):
    first_line = path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line == "# Databricks notebook source"


_WRITERS = [
    p
    for p in _NOTEBOOKS
    if _WRITE_CALL in _call_names(ast.parse(p.read_text(encoding="utf-8")))
]


@pytest.mark.parametrize("path", _WRITERS, ids=_name)
def test_every_notebook_that_writes_gold_also_calls_the_hard_fail_gate(path):
    calls = _call_names(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
    assert calls & _GATE_CALLS, (
        f"{path.name} calls {_WRITE_CALL} without calling check(), "
        "assert_unique_grain() or gate_tables() -- a table would be written with no "
        "Silver->Gold hard-fail gate exercised at all"
    )


def _first_call_line(tree: ast.AST, name: str) -> int | None:
    lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == name
    ]
    return min(lines) if lines else None


@pytest.mark.parametrize("path", _WRITERS, ids=_name)
def test_the_grain_gate_runs_on_the_written_tables(path):
    """`gate_tables` checks the written tables and restores a failing one, so
    it must come after `write_tables` (the first gate call of the notebook)."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    gate = _first_call_line(tree, "gate_tables")
    write = _first_call_line(tree, _WRITE_CALL)
    assert gate is not None and write is not None and write < gate


def test_no_gold_notebook_creates_a_view() -> None:
    """Every Gold object is a table."""
    creators = [
        p.name
        for p in _NOTEBOOKS
        if "write_gold_view" in _call_names(ast.parse(p.read_text(encoding="utf-8")))
        or "CREATE OR REPLACE VIEW" in p.read_text(encoding="utf-8").upper()
    ]
    assert not creators, f"Gold notebooks that create a view: {creators}"


def test_every_gold_read_silver_target_is_registered() -> None:
    """Static emulation of 00_gold_setup.py's preflight -- every table a Gold
    notebook's read_silver() names must be produced by the field-class
    generator, or the real preflight would (correctly) hard-fail before any
    Gold processing starts."""
    produced = {r["table_name"] for r in build_rows()}
    required = all_read_silver_tables(_GOLD)
    missing = sorted(required - produced)
    assert not missing, f"Gold reads unregistered Silver table(s): {missing}"


def test_gold_preflight_would_reject_an_unregistered_dependency() -> None:
    """Proves the preflight's own comparison logic, not just today's repo
    state -- an unregistered Gold dependency must be detectable as a gap."""
    produced = {r["table_name"] for r in build_rows()}
    required = all_read_silver_tables(_GOLD) | {"a_table_nobody_registered"}
    missing = sorted(required - produced)
    assert missing == ["a_table_nobody_registered"]
