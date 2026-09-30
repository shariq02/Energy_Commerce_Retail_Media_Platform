"""Static checks on the ML notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Every notebook must parse, carry the standard header, stay within the cell
limit, keep to the allowed set of driver-side conversions, reference only
known split calendars and registry tables, and any notebook that writes an ML
structure must also call the hard-fail gate.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests._notebook_loader import load_ml_common

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_ML = _ROOT / "databricks" / "ml"
_FILES = sorted(_ML.rglob("*.py"))
_NOTEBOOKS = [p for p in _FILES if not p.name.startswith("_")]

_MAX_CELLS = 30
_GATE_CALLS = {
    "check",
    "assert_unique_grain",
    "record_check",
    "assert_values_present",
    "assert_no_forbidden_columns",
}
_WRITE_CALLS = {"write_ml", "write_ml_view"}
# Small entity tables only (guarded by a size assertion) or tiny aggregates.
_TO_PANDAS_ALLOWED = {
    "utility/01_gap_limit_evaluation.py",
    "energy/features/05_hub_height_fill.py",
    "energy/features/09_zone_weather_acceptance.py",
}
_DOC_REFERENCES = re.compile(r"docs/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")


def _rel(p: Path) -> str:
    return str(p.relative_to(_ML))


def _calls(tree):
    return {
        n.func.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_file_parses_and_has_the_databricks_header(path):
    text = path.read_text(encoding="utf-8")
    ast.parse(text, filename=str(path))
    assert text.splitlines()[0] == "# Databricks notebook source"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_file_stays_within_the_cell_limit(path):
    n = path.read_text(encoding="utf-8").count("# COMMAND ----------") + 1
    assert n <= _MAX_CELLS, f"{_rel(path)} has {n} cells"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_file_carries_no_document_references(path):
    hits = _DOC_REFERENCES.findall(path.read_text(encoding="utf-8"))
    assert not hits, f"{_rel(path)} cites planning documents: {hits}"


_WRITERS = [
    p
    for p in _NOTEBOOKS
    if _calls(ast.parse(p.read_text(encoding="utf-8"))) & _WRITE_CALLS
]


@pytest.mark.parametrize("path", _WRITERS, ids=_rel)
def test_writing_notebooks_call_the_hard_fail_gate(path):
    calls = _calls(ast.parse(path.read_text(encoding="utf-8")))
    assert calls & _GATE_CALLS, f"{_rel(path)} writes without a gate call"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_to_pandas_is_limited_to_the_small_table_notebooks(path):
    text = path.read_text(encoding="utf-8")
    if ".toPandas()" in text:
        assert _rel(path) in _TO_PANDAS_ALLOWED, f"{_rel(path)} calls toPandas"


def test_split_notebooks_reference_only_defined_calendars():
    calendars = set(load_ml_common()["SPLIT_CALENDARS"])
    pat = re.compile(r"(?:calendar_partition|rolling_fold)\([^,]+,\s*\"(\w+)\"")
    for p in (_ML / "energy" / "split").glob("*.py"):
        for key in pat.findall(p.read_text(encoding="utf-8")):
            assert key in calendars, f"{_rel(p)} uses unknown calendar {key}"


def test_fit_notebooks_never_read_the_test_partition():
    pat = re.compile(r"""(==|!=)\s*['"]test['"]|isin\([^)]*['"]test['"]""")
    for sub in ("energy/fit", "commerce/fit"):
        for p in (_ML / sub).glob("*.py"):
            assert not pat.search(p.read_text(encoding="utf-8")), _rel(p)


def test_registry_tables_are_defined_and_used():
    ddl = set(load_ml_common()["REGISTRY_DDL"])
    used = set()
    for p in _NOTEBOOKS:
        used |= set(
            re.findall(r"read_ml\(\"([a-z_]+)\"", p.read_text(encoding="utf-8"))
        )
    registry = {
        "dataset_manifest",
        "split_specification",
        "partition_manifest",
        "null_class_registry",
        "imputer_parameters",
        "mask_specification",
        "gap_limits",
        "null_rate_profile",
        "feature_contract",
        "gate_results",
    }
    assert registry == ddl
    assert used & registry


def test_gate_notebooks_and_freeze_gate_exist():
    for name in (
        "01_leakage_audit.py",
        "02_null_handling_audit.py",
        "03_freeze_gate.py",
    ):
        assert (_ML / "gate" / name).exists()
