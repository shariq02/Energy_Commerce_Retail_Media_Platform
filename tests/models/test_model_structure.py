"""Static checks on the model notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

Every file must parse, carry the standard header, stay within the cell limit and cite
no planning documents. Only the shared library may convert Spark frames to pandas, and
only the evaluation code may name the test partition. The task file holds every task,
and the notebooks hold no task of their own: the notebooks of a stage run the tasks of
all ecosystems from that file.
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
_MAX_CELLS = 45
_DOC_REFERENCES = re.compile(r"docs/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")
_RETIRED_NAMES = re.compile(
    r"foundation_(?:ts|tab|anomaly|survival|rank|rl|stack)|weak_supervision|"
    r"redispatch_matching|self_supervised_other|chronos|tabpfn|tabicl|momentfm|"
    r"timesfm|granite_tsfm|task_registry|evaluation_split_manifest"
)


def _rel(p: Path) -> str:
    return p.relative_to(_MODELS).as_posix()


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_file_parses_and_has_the_databricks_header(path):
    text = path.read_text(encoding="utf-8")
    ast.parse(text, filename=str(path))
    assert text.splitlines()[0] == "# Databricks notebook source"


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_rel)
def test_notebook_stays_within_the_cell_limit(path):
    n = path.read_text(encoding="utf-8").count("# COMMAND ----------") + 1
    assert n <= _MAX_CELLS, f"{_rel(path)} has {n} cells"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_file_carries_no_document_references(path):
    hits = _DOC_REFERENCES.findall(path.read_text(encoding="utf-8"))
    assert not hits, f"{_rel(path)} cites planning documents: {hits}"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_no_code_of_a_removed_model_family_remains(path):
    hits = _RETIRED_NAMES.findall(path.read_text(encoding="utf-8"))
    assert not hits, f"{_rel(path)} names removed code: {hits}"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_only_the_shared_library_converts_frames_to_pandas(path):
    if path.name == "_model_common.py":
        return
    assert ".toPandas(" not in path.read_text(encoding="utf-8"), _rel(path)


_MAY_NAME_THE_TEST_PARTITION = {
    "lib/_model_common.py",
    "lib/_model_evaluate.py",
    "evaluate/01_evaluate.py",
    "evaluate/02_evaluation_gate.py",
}


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_model_code_never_names_the_test_partition(path):
    # The shared library assigns the partition and the evaluation code reads it; the
    # other libraries and the notebooks only ever read train and validation.
    if _rel(path) in _MAY_NAME_THE_TEST_PARTITION:
        return
    text = path.read_text(encoding="utf-8")
    assert not re.search(r"""['"]test['"]""", text), _rel(path)


def test_every_notebook_has_a_numbered_name_in_its_folder():
    for p in _NOTEBOOKS:
        assert re.match(r"\d\d_", p.name), _rel(p)
    assert {p.parent.name for p in _NOTEBOOKS} == {
        "setup",
        "train",
        "evaluate",
        "approve",
        "register",
        "operate",
    }


def test_the_task_notebooks_hold_no_task_id_of_their_own():
    task_ids = set(load_model_common()["TASK_BY_ID"])
    for p in _NOTEBOOKS:
        text = p.read_text(encoding="utf-8")
        assert not [t for t in task_ids if f'"{t}"' in text], _rel(p)


def test_training_and_evaluation_run_every_task_through_the_task_file():
    for name, entry in (
        ("train/01_train.py", "run_task("),
        ("evaluate/01_evaluate.py", "evaluate_by_id("),
    ):
        text = (_MODELS / name).read_text(encoding="utf-8")
        assert entry in text, name
        assert "spark.table(" not in text and "spark.read" not in text, name
        assert "SMOKE = False" in text, name
        assert re.search(r"^(RETRAIN|REEVALUATE) = False$", text, re.MULTILINE), name


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_no_model_file_reads_a_widget(path):
    assert "dbutils.widgets" not in path.read_text(encoding="utf-8"), _rel(path)


def test_only_the_volume_schema_keeps_an_ecosystem_name():
    for p in _FILES:
        names = set(re.findall(r"\b(\w+_ml_models)\b", p.read_text(encoding="utf-8")))
        assert names <= {"energy_ml_models"}, _rel(p)


_MODEL_TABLE_ACCESS = [
    r"saveAsTable\(model_fqn\(",
    r"CREATE TABLE IF NOT EXISTS \{model_fqn\(",
    r"spark\.table\(model_fqn\(",
]


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_model_tables_are_reached_only_through_the_shared_helpers(path):
    """The ecosystems share each model table, so every read, create and append goes
    through `read_model`, `create_model_table` and `append_model_rows`."""
    if path.name == "_model_common.py":
        return
    text = path.read_text(encoding="utf-8")
    for pattern in _MODEL_TABLE_ACCESS:
        assert not re.search(pattern, text), f"{_rel(path)}: {pattern}"


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_a_delete_on_a_model_table_names_the_ecosystem(path):
    text = path.read_text(encoding="utf-8")
    if "DELETE FROM {model_fqn(" in text:
        assert "ecosystem_predicate(" in text, _rel(path)


def test_the_expected_files_exist():
    for rel in (
        "setup/01_model_libraries.py",
        "setup/02_model_setup.py",
        "train/01_train.py",
        "train/02_candidate_gate.py",
        "evaluate/01_evaluate.py",
        "evaluate/02_evaluation_gate.py",
        "approve/01_recommend.py",
        "approve/02_record_decisions.py",
        "approve/03_model_cards.py",
        "approve/04_approval_gate.py",
        "register/01_register_models.py",
        "register/02_reproducibility.py",
        "register/03_registry_gate.py",
        "operate/01_reference_and_monitor.py",
        "operate/02_performance_and_triggers.py",
        "operate/03_score_request.py",
        "operate/04_operate_gate.py",
        "lib/_model_common.py",
        "lib/_model_fit.py",
        "lib/_model_evaluate.py",
        "lib/_model_govern.py",
        "lib/_model_operate.py",
    ):
        assert (_MODELS / rel).exists(), rel
    assert {_rel(p) for p in _FILES} == {
        "setup/01_model_libraries.py",
        "setup/02_model_setup.py",
        "train/01_train.py",
        "train/02_candidate_gate.py",
        "evaluate/01_evaluate.py",
        "evaluate/02_evaluation_gate.py",
        "approve/01_recommend.py",
        "approve/02_record_decisions.py",
        "approve/03_model_cards.py",
        "approve/04_approval_gate.py",
        "register/01_register_models.py",
        "register/02_reproducibility.py",
        "register/03_registry_gate.py",
        "operate/01_reference_and_monitor.py",
        "operate/02_performance_and_triggers.py",
        "operate/03_score_request.py",
        "operate/04_operate_gate.py",
        "lib/_model_common.py",
        "lib/_model_fit.py",
        "lib/_model_evaluate.py",
        "lib/_model_govern.py",
        "lib/_model_operate.py",
    }
    assert (_ROOT / "databricks" / "schema_registry" / "01_snapshot_schema.py").exists()


_APPROVAL = [p for p in _NOTEBOOKS if _rel(p).startswith("approve/")]
_REGISTER = [p for p in _NOTEBOOKS if _rel(p).startswith("register/")]
_OPERATE = [p for p in _NOTEBOOKS if _rel(p).startswith("operate/")]


@pytest.mark.parametrize("path", _APPROVAL, ids=_rel)
def test_approval_notebooks_read_recorded_results_only(path):
    text = path.read_text(encoding="utf-8")
    assert ".toPandas(" not in text
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text
    assert "%run ../lib/_model_common" in text
    assert "%run ../lib/_model_govern" in text


_REPRODUCIBILITY = _MODELS / "register" / "02_reproducibility.py"
_RECORD_ONLY = [p for p in _REGISTER if p != _REPRODUCIBILITY]


@pytest.mark.parametrize("path", _RECORD_ONLY, ids=_rel)
def test_registry_record_notebooks_read_recorded_results_only(path):
    text = path.read_text(encoding="utf-8")
    assert "evaluate_by_id(" not in text and "load_bundle(" not in text
    assert "%run ../lib/_model_common" in text
    assert "%run ../lib/_model_govern" in text


def test_reproducibility_notebook_uses_the_check_functions_and_scores_no_test_data():
    text = _REPRODUCIBILITY.read_text(encoding="utf-8")
    for lib in ("_model_common", "_model_fit", "_model_evaluate", "_model_govern"):
        assert f"%run ../lib/{lib}" in text
    assert "reproduce_ecosystem(" in text and "record_checks(" in text
    assert "evaluate_by_id(" not in text
    library = (_MODELS / "lib" / "_model_govern.py").read_text(encoding="utf-8")
    evaluation = (_MODELS / "lib" / "_model_evaluate.py").read_text(encoding="utf-8")
    assert "class ReproContext(EvalContext)" in evaluation
    assert (
        "ALLOWED_PARTITIONS" in evaluation
        and 'self.partition = "validation"' in evaluation
    )
    assert "class ReproContext" not in library
    assert "evaluate_task(" not in library and "run_candidate(" not in library


@pytest.mark.parametrize("path", _OPERATE, ids=_rel)
def test_operate_notebooks_never_change_a_decision_or_a_registry_row(path):
    text = path.read_text(encoding="utf-8")
    pattern = r"""(?:replace_table_rows|replace_model_rows)\([^)]*['"]model_(?:registry|approval)"""
    assert not re.search(pattern, text), _rel(path)
    assert 'saveAsTable(model_fqn("model_registry"' not in text, _rel(path)
    assert 'saveAsTable(model_fqn("model_approval"' not in text, _rel(path)


def test_the_score_request_takes_keys_and_a_window_and_no_feature_values():
    text = (_MODELS / "operate" / "03_score_request.py").read_text(encoding="utf-8")
    for setting in ("TASK_ID", "KEYS", "WINDOW_START", "WINDOW_END"):
        assert f"{setting} = " in text
    assert "evaluate_by_id(" not in text and "widgets" not in text
    library = (_MODELS / "lib" / "_model_operate.py").read_text(encoding="utf-8")
    assert "key_columns(cfg)" in library


def test_operate_window_library_scores_no_target():
    text = (_MODELS / "lib" / "_model_operate.py").read_text(encoding="utf-8")
    assert ".score(" not in text.replace("bundle.score(", "")
    assert "evaluate_task(" not in text and "record_evaluation(" not in text
    assert "window_partition(" in text and "TEST_PARTITION" in text


def test_genomics_texts_hold_no_identifier():
    """Findings and cards of the genomics ecosystem go to the repository as counts,
    rates and metrics only: no variant, gene, rsID or genomic position."""
    gate = (_MODELS / "approve" / "04_approval_gate.py").read_text(encoding="utf-8")
    assert "genomics_texts_hold_no_identifier" in gate
    assert "GENOMICS_IDENTIFIER.search(" in gate
