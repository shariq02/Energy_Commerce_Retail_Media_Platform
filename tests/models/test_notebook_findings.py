"""Static checks on the findings cell of the model notebooks -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Every numbered notebook that records results ends with a call that writes its own
findings section; the call must name known tables and tasks of the right ecosystem.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests._notebook_loader import load_foundation_libs

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_MODELS = Path(__file__).resolve().parents[2] / "databricks" / "models"
_SKIPPED_FOLDERS = {"lib", "gate"}
_NOTEBOOKS = sorted(
    p
    for p in _MODELS.rglob("[0-9]*.py")
    if p.parent.name not in _SKIPPED_FOLDERS
    and p.parent != _MODELS
    and p.name != "00_foundation_setup.py"
)
_CALLS = ("write_model_findings", "write_foundation_findings")
_NAMES = [str(p.relative_to(_MODELS)) for p in _NOTEBOOKS]


def _last_call(path):
    last = path.read_text(encoding="utf-8").split("# COMMAND ----------")[-1]
    for node in ast.parse(last).body:
        call = getattr(node, "value", None)
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") in _CALLS:
            return call
    return None


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_NAMES)
def test_notebook_ends_with_its_findings_call(path):
    assert _last_call(path) is not None


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_NAMES)
def test_findings_call_names_known_tables_and_tasks(path, lib):
    call = _last_call(path)
    if call.func.id == "write_foundation_findings":
        return
    tables = ast.literal_eval(call.args[3])
    known = set(lib["MODEL_DDL"]) | {"foundation_registry_entry", "scoring_requests"}
    assert set(tables) <= known
    keywords = {k.arg: k.value for k in call.keywords}
    if "task_ids" in keywords:
        wanted = {t["task_id"] for t in lib["TASKS"]}
        named = {
            e.value for e in keywords["task_ids"].elts if isinstance(e, ast.Constant)
        }
        assert named <= wanted


def test_the_findings_key_is_the_notebook_path():
    keys = set()
    for path in _NOTEBOOKS:
        call = _last_call(path)
        key = ast.literal_eval(call.args[1])
        if call.func.id == "write_model_findings":
            assert key == str(path.relative_to(_MODELS))[:-3].replace("/", "__")
        keys.add(key)
    assert len(keys) == len(_NOTEBOOKS)
