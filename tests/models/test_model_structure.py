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
    ):
        assert (_MODELS / rel).exists(), rel
    assert (
        _ROOT / "databricks" / "schema_registry" / "05_snapshot_model_schema.py"
    ).exists()
