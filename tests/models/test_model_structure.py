"""Static checks on the model notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Every file must parse, carry the standard header, stay within the cell limit and cite
no planning documents. Dataset notebooks must pin a registered task, read through
`read_frozen`, and never name the test partition; only the evaluation code and the
split code may. Only the shared library may convert Spark frames to pandas.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests._notebook_loader import load_model_common

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_MODELS = _ROOT / "databricks" / "models"
_FILES = sorted(_MODELS.rglob("*.py"))
_NOTEBOOKS = [p for p in _FILES if not p.name.startswith("_")]
_DATASET_NOTEBOOKS = [p for p in _NOTEBOOKS if p.parent.name in ("energy", "commerce")]
_MAX_CELLS = 30
_EVALUATION = [p for p in _NOTEBOOKS if p.parent.name == "evaluate"]
_DOC_REFERENCES = re.compile(r"docs/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")


def _rel(p: Path) -> str:
    return str(p.relative_to(_MODELS))


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


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_only_the_shared_library_converts_frames_to_pandas(path):
    if path.name == "_model_common.py":
        return
    assert ".toPandas(" not in path.read_text(encoding="utf-8"), _rel(path)


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_model_code_never_names_the_test_partition(path):
    # Splitting code assigns the partition and the evaluation code reads it; the
    # other libraries and the model notebooks only ever read train and validation.
    if path.name == "_model_common.py" or path.parent.name == "split":
        return
    if _rel(path).startswith("evaluate/") or path.name.startswith("_eval_"):
        return
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"""['"]test['"]""", text), _rel(path)


def test_every_registered_task_has_a_notebook_that_pins_it():
    tasks = load_model_common()["TASKS"]
    pinned = set()
    for p in _DATASET_NOTEBOOKS:
        pinned |= set(
            re.findall(r'TaskContext\("([^"]+)"', p.read_text(encoding="utf-8"))
        )
    assert pinned == {t["task_id"] for t in tasks}


def test_every_dataset_notebook_belongs_to_a_registered_task():
    tasks = load_model_common()["TASKS"]
    registered = {t["notebook"] for t in tasks}
    assert {_rel(p) for p in _DATASET_NOTEBOOKS} == registered


@pytest.mark.parametrize("path", _DATASET_NOTEBOOKS, ids=_rel)
def test_dataset_notebooks_read_through_the_frozen_reader(path):
    text = path.read_text(encoding="utf-8")
    assert "read_frozen(" in text
    assert "spark.table(" not in text and "spark.read" not in text
    assert 'dbutils.widgets.text("smoke"' in text


@pytest.mark.parametrize("path", _DATASET_NOTEBOOKS, ids=_rel)
def test_dataset_notebooks_call_a_paradigm_runner(path):
    text = path.read_text(encoding="utf-8")
    assert re.search(r"\brun_[a-z_]+\(ctx", text), _rel(path)


def test_every_registered_task_is_evaluated_by_exactly_one_notebook():
    tasks = load_model_common()["TASKS"]
    named = []
    for p in _EVALUATION:
        named += re.findall(r'evaluate_by_id\("([^"]+)"', p.read_text(encoding="utf-8"))
    assert sorted(named) == sorted(t["task_id"] for t in tasks)


@pytest.mark.parametrize("path", _EVALUATION, ids=_rel)
def test_evaluation_notebooks_use_the_evaluation_entry_point(path):
    text = path.read_text(encoding="utf-8")
    if path.name == "00_evaluation_setup.py":
        return
    assert "spark.table(" not in text and "spark.read" not in text
    assert 'dbutils.widgets.text("smoke"' in text
    assert "%run ../lib/_eval_common" in text and "%run ../lib/_eval_specs" in text


def test_model_schemas_are_only_the_two_agreed_names():
    for p in _FILES:
        names = set(re.findall(r"\b(\w+_ml_models)\b", p.read_text(encoding="utf-8")))
        assert names <= {"energy_ml_models", "commerce_ml_models"}, _rel(p)


def test_setup_preflight_splits_and_gates_exist():
    for rel in (
        "00_model_setup.py",
        "00_model_libraries.py",
        "00_model_preflight.py",
        "split/01_weak_supervision_split.py",
        "split/02_redispatch_matching_split.py",
        "split/03_evaluation_spec.py",
        "gate/01_candidate_guards.py",
        "gate/02_candidate_selection.py",
        "gate/03_export_findings.py",
        "evaluate/00_evaluation_setup.py",
        "evaluate/gate/01_evaluation_guards.py",
        "evaluate/gate/02_evaluation_flags.py",
        "evaluate/gate/03_export_findings.py",
        "lib/_eval_common.py",
        "lib/_eval_specs.py",
        "approve/00_approval_setup.py",
        "approve/01_recommend.py",
        "approve/02_record_decisions.py",
        "approve/03_model_cards.py",
        "approve/gate/01_approval_guards.py",
        "approve/gate/02_approval_cards.py",
        "approve/gate/03_export_findings.py",
        "lib/_approval_rules.py",
        "lib/_approval_render.py",
        "register/00_registry_setup.py",
        "register/01_register_models.py",
        "register/02_reproducibility_energy.py",
        "register/02_reproducibility_commerce.py",
        "register/gate/01_registry_guards.py",
        "register/gate/02_reproducibility_guards.py",
        "register/gate/03_export_findings.py",
        "lib/_registry_rules.py",
        "lib/_registry_check.py",
        "operate/00_operate_setup.py",
        "operate/01_reference_profiles_energy.py",
        "operate/01_reference_profiles_commerce.py",
        "operate/02_monitor_energy.py",
        "operate/02_monitor_commerce.py",
        "operate/03_performance_and_triggers.py",
        "operate/gate/01_operate_guards.py",
        "operate/gate/02_monitoring_guards.py",
        "operate/gate/03_export_findings.py",
        "lib/_operate_rules.py",
        "lib/_operate_windows.py",
    ):
        assert (_MODELS / rel).exists(), rel
    assert (
        _ROOT / "databricks" / "schema_registry" / "05_snapshot_model_schema.py"
    ).exists()


_APPROVAL = [p for p in _NOTEBOOKS if _rel(p).startswith("approve/")]


@pytest.mark.parametrize("path", _APPROVAL, ids=_rel)
def test_approval_notebooks_read_recorded_results_only(path):
    text = path.read_text(encoding="utf-8")
    assert ".toPandas(" not in text
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text


def test_approval_notebooks_load_the_approval_libraries():
    for p in _APPROVAL:
        text = p.read_text(encoding="utf-8")
        assert "lib/_model_common" in text, _rel(p)
        assert "lib/_approval_rules" in text, _rel(p)


_REGISTER = [p for p in _NOTEBOOKS if _rel(p).startswith("register/")]
_REGISTER_CHECKS = [
    p for p in _REGISTER if p.parent.name == "register" and p.name.startswith("02_")
]
_REGISTER_RECORD_ONLY = [p for p in _REGISTER if p not in _REGISTER_CHECKS]


@pytest.mark.parametrize("path", _REGISTER_RECORD_ONLY, ids=_rel)
def test_registry_record_notebooks_read_recorded_results_only(path):
    text = path.read_text(encoding="utf-8")
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text
    needs = ["lib/_model_common"]
    if path.name != "00_registry_setup.py":
        needs.append("lib/_registry_rules")
    for lib in needs:
        assert lib in text, _rel(path)


@pytest.mark.parametrize("path", _REGISTER_CHECKS, ids=_rel)
def test_reproducibility_notebooks_use_the_check_library(path):
    text = path.read_text(encoding="utf-8")
    for lib in ("_registry_rules", "_registry_check", "_eval_common", "_eval_specs"):
        assert f"%run ../lib/{lib}" in text, _rel(path)
    assert "reproduce_ecosystem(" in text and "record_checks(" in text
    assert "evaluate_by_id(" not in text


def test_registry_check_library_scores_the_earlier_partitions_only():
    text = (_MODELS / "lib" / "_registry_check.py").read_text(encoding="utf-8")
    assert "ALLOWED_PARTITIONS" in text and 'partition = "validation"' in text
    assert "evaluate_task(" not in text and "run_candidate(" not in text


_OPERATE = [p for p in _NOTEBOOKS if _rel(p).startswith("operate/")]
_OPERATE_WINDOW_RUNS = [
    p for p in _OPERATE if p.name[:2] in ("01", "02") and p.parent.name == "operate"
]
_OPERATE_RECORD_ONLY = [p for p in _OPERATE if p not in _OPERATE_WINDOW_RUNS]


@pytest.mark.parametrize("path", _OPERATE, ids=_rel)
def test_operate_notebooks_never_change_a_decision_or_a_registry_row(path):
    text = path.read_text(encoding="utf-8")
    pattern = (
        r"""(?:replace_rows|replace_model_rows)\([^)]*['"]model_(?:registry|approval)"""
    )
    assert not re.search(pattern, text), _rel(path)
    assert 'saveAsTable(model_fqn("model_registry"' not in text, _rel(path)
    assert 'saveAsTable(model_fqn("model_approval"' not in text, _rel(path)


@pytest.mark.parametrize("path", _OPERATE_RECORD_ONLY, ids=_rel)
def test_operate_record_notebooks_read_recorded_results_only(path):
    text = path.read_text(encoding="utf-8")
    assert "load_bundle(" not in text and "evaluate_by_id(" not in text
    assert "operate_ecosystem(" not in text and ".toPandas(" not in text


@pytest.mark.parametrize("path", _OPERATE_WINDOW_RUNS, ids=_rel)
def test_operate_window_notebooks_use_the_window_library(path):
    text = path.read_text(encoding="utf-8")
    for lib in ("_eval_common", "_eval_specs", "_operate_rules", "_operate_windows"):
        assert f"%run ../lib/{lib}" in text, _rel(path)
    assert "operate_ecosystem(" in text and "evaluate_by_id(" not in text


def test_operate_window_library_scores_no_target():
    text = (_MODELS / "lib" / "_operate_windows.py").read_text(encoding="utf-8")
    assert ".score(" not in text.replace("bundle.score(", "")
    assert "evaluate_task(" not in text and "record_evaluation(" not in text
    assert "window_partition(" in text and "TEST_PARTITION" in text
