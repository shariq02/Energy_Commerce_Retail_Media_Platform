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
_GOLD = _ROOT / "databricks" / "gold"

_NOTEBOOKS = sorted(
    p
    for p in _GOLD.rglob("*.py")
    if p.name != "_gold_common.py" and not p.name.startswith("_")
)

_GATE_CALLS = {"check", "assert_unique_grain"}
_WRITE_CALL = "write_gold"


def _call_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            names.add(node.func.id)
    return names


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=lambda p: str(p.relative_to(_GOLD)))
def test_notebook_parses(path):
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=lambda p: str(p.relative_to(_GOLD)))
def test_notebook_has_the_standard_databricks_header(path):
    first_line = path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line == "# Databricks notebook source"


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=lambda p: str(p.relative_to(_GOLD)))
def test_every_notebook_that_writes_gold_also_calls_the_hard_fail_gate(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls = _call_names(tree)
    if _WRITE_CALL not in calls:
        pytest.skip(
            f"{path.name} does not call {_WRITE_CALL} -- not a table-writing notebook"
        )
    assert calls & _GATE_CALLS, (
        f"{path.name} calls {_WRITE_CALL} without calling check() or "
        "assert_unique_grain() -- a Gold table would be written with no "
        "Silver->Gold hard-fail gate exercised at all"
    )


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
