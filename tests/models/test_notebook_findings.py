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

from tests._notebook_loader import load_model_libs

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_MODELS = Path(__file__).resolve().parents[2] / "databricks" / "models"
_ALL = sorted(p for p in _MODELS.rglob("[0-9]*.py") if p.parent.name != "lib")
# a gate writes the findings files itself and ends with the merge
_GATES = [p for p in _ALL if "FINDINGS_DIR" in p.read_text(encoding="utf-8")]
_NOTEBOOKS = [p for p in _ALL if p not in _GATES and p.name != "01_model_libraries.py"]
_NAMES = [p.relative_to(_MODELS).as_posix() for p in _NOTEBOOKS]


def _findings_call(path):
    """The call of `write_model_findings` in a notebook, in any cell."""
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == (
            "write_model_findings"
        ):
            return node
    return None


@pytest.fixture(scope="module")
def lib():
    return load_model_libs()


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_NAMES)
def test_notebook_writes_its_findings(path):
    assert _findings_call(path) is not None


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_NAMES)
def test_findings_call_names_known_tables_and_tasks(path, lib):
    call = _findings_call(path)
    tables = call.args[3]
    if not isinstance(tables, ast.List):
        return
    known = set(lib["MODEL_DDL"])
    named = {e.value for e in tables.elts if isinstance(e, ast.Constant)}
    assert named <= known, named - known
    keywords = {k.arg: k.value for k in call.keywords}
    if "task_ids" in keywords:
        wanted = {t["task_id"] for t in lib["TASKS"]} | set(lib["CHECK_ONLY"])
        listed = getattr(keywords["task_ids"], "elts", [])  # a computed list is skipped
        named = {e.value for e in listed if isinstance(e, ast.Constant)}
        assert named <= wanted


def test_the_findings_key_is_the_notebook_path():
    keys = set()
    for path in _NOTEBOOKS:
        key = ast.literal_eval(_findings_call(path).args[1])
        assert key == path.relative_to(_MODELS).as_posix()[:-3].replace("/", "__")
        keys.add(key)
    assert len(keys) == len(_NOTEBOOKS)


def test_every_gate_that_writes_findings_is_listed():
    names = {p.relative_to(_MODELS).as_posix() for p in _GATES}
    assert names == {
        "train/02_candidate_gate.py",
        "evaluate/02_evaluation_gate.py",
        "approve/04_approval_gate.py",
        "register/03_registry_gate.py",
        "operate/04_operate_gate.py",
    }


@pytest.mark.parametrize("gate", _GATES, ids=lambda p: p.name)
def test_findings_gates_end_with_the_merge(gate):
    last = gate.read_text(encoding="utf-8").split("# COMMAND ----------")
    assert last[-1].strip().endswith("merge_notebook_findings()")


def test_notebook_findings_group_splits_prefix_smoke_and_key(lib):
    group = lib["notebook_findings_group"]
    assert group("energy__split__01_x.md") == ("energy", False, "energy__split__01_x")
    assert group("commerce__evaluate__01_evaluate_smoke.md") == (
        "commerce",
        True,
        "commerce__evaluate__01_evaluate",
    )


def test_merge_replaces_a_section_and_keeps_the_others(lib):
    import datetime as dt

    now = dt.datetime(2026, 10, 23, tzinfo=dt.UTC)
    one = "# H\n\n_x_\n\n## A\n\n### t\n\nrow\n"
    sections = {"energy__a": lib["section_of_findings_file"](one)}
    sections["energy__b"] = "## B\n\nold"
    text = lib["merged_findings_text"]("energy", sections, False, now)
    again = lib["read_merged_sections"](text)
    assert again == sections
    again["energy__b"] = "## B\n\nnew"
    text = lib["merged_findings_text"]("energy", again, False, now)
    result = lib["read_merged_sections"](text)
    assert result["energy__b"] == "## B\n\nnew"
    assert result["energy__a"].startswith("## A")
