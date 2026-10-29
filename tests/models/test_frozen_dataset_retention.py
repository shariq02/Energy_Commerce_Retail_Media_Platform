"""Static checks on the frozen dataset retention notebook -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Every write switch must default to False, and the drop cell must check the retention
property and the switch before it drops a table.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_PATH = (
    Path(__file__).resolve().parents[2]
    / "databricks"
    / "utility"
    / "11_frozen_dataset_retention.py"
)
_TEXT = _PATH.read_text(encoding="utf-8")
_CELLS = _TEXT.split("# COMMAND ----------")
_DOC_REFERENCES = re.compile(r"docs/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")


def _constants() -> dict[str, object]:
    found = {}
    for node in ast.parse(_TEXT).body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            try:
                found[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                continue
    return found


def _cell_with(text: str) -> str:
    cells = [c for c in _CELLS if text in c]
    assert len(cells) == 1, f"{len(cells)} cell(s) hold {text!r}"
    return cells[0]


def test_notebook_parses_and_has_the_databricks_header():
    assert _TEXT.splitlines()[0] == "# Databricks notebook source"


def test_notebook_stays_within_the_cell_limit_and_cites_no_documents():
    assert len(_CELLS) <= 30
    assert not _DOC_REFERENCES.findall(_TEXT)


@pytest.mark.parametrize("name", ["APPLY_RETENTION", "APPLY_CLONE", "APPLY_DROP"])
def test_write_switches_default_to_false(name):
    assert _constants()[name] is False


def test_drop_covers_the_energy_clones_only():
    assert _constants()["DROP_ECOSYSTEMS"] == ("energy",)


def test_drop_cell_checks_retention_and_the_switch_before_it_drops():
    cell = _cell_with("DROP TABLE")
    drop = cell.index("DROP TABLE")
    assert cell.index("deletedFileRetentionDuration") < drop
    assert cell.index("APPLY_DROP") < drop
    assert cell.index("DROP_ECOSYSTEMS") < drop


def test_drop_cell_only_touches_clone_names():
    cell = _cell_with("DROP TABLE")
    assert "DROP TABLE {name}" in cell
    assert "in clone_name.items()" in cell


def test_clone_cell_is_guarded_by_its_switch_and_the_quota():
    cell = _cell_with("DEEP CLONE")
    order = [cell.index(t) for t in ("APPLY_CLONE", "_quota_guard", "DEEP CLONE")]
    assert order == sorted(order)
