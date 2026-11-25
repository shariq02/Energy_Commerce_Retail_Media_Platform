"""Static checks on the ML notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

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
    # these library functions call the gates themselves
    "build_datasets",
    "write_stored_table",
}
_WRITE_CALLS = {
    "write_ml",
    "write_dataset_rows",
    "write_stored_table",
    "build_datasets",
}
# Small entity tables only (guarded by a size assertion) or tiny aggregates. The
# gate loads one train sample of at most PROXY_SAMPLE_ROWS rows per dataset for the
# label-proxy check.
_TO_PANDAS_ALLOWED = {
    "energy/02_unit_and_site_datasets.py",
    "energy/03_zone_and_weather_datasets.py",
    "gate/01_ml_gates.py",
}
_DOC_REFERENCES = re.compile(r"docs/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}")


def _rel(p: Path) -> str:
    return p.relative_to(_ML).as_posix()


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


@pytest.mark.parametrize("path", _NOTEBOOKS, ids=_rel)
def test_notebook_stays_within_the_cell_limit(path):
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


def test_the_spec_references_only_defined_calendars():
    calendars = set(load_ml_common()["SPLIT_CALENDARS"])
    pat = re.compile(r"(?:calendar_partition|rolling_fold)\([^,]+,\s*\"(\w+)\"")
    for p in _FILES:
        for key in pat.findall(p.read_text(encoding="utf-8")):
            assert key in calendars, f"{_rel(p)} uses unknown calendar {key}"


def test_build_code_never_reads_the_test_partition():
    pat = re.compile(r"""(==|!=)\s*['"]test['"]|isin\([^)]*['"]test['"]""")
    for p in (*_ML.glob("*/0*.py"), _ML / "_ml_fit.py"):
        if p.parent.name == "gate":
            continue
        assert not pat.search(p.read_text(encoding="utf-8")), _rel(p)


def test_registry_tables_are_defined_and_used():
    ddl = set(load_ml_common()["REGISTRY_DDL"])
    used = set()
    for p in _FILES:
        used |= set(
            re.findall(r"read_ml\(\"([a-z_]+)\"", p.read_text(encoding="utf-8"))
        )
    registry = {
        "dataset_manifest",
        "partition_manifest",
        "null_class_registry",
        "imputer_parameters",
        "mask_specification",
        "feature_contract",
        "gate_results",
    }
    assert registry == ddl
    assert used & registry


def test_the_ml_folder_stays_under_the_file_target():
    """Fewer than 15 files: the notebooks, the libraries and the spec file."""
    spec = _ROOT / "src" / "schemas" / "mappings" / "ml_datasets.yml"
    assert spec.exists()
    assert len(_FILES) + 1 < 15, f"{len(_FILES)} files in databricks/ml"


_SHARED_NAME_ALLOWED = {"_ml_common.py", "00_ml_setup.py", "01_ml_gates.py"}


@pytest.mark.parametrize("path", _FILES, ids=_rel)
def test_shared_registry_tables_are_reached_through_the_helpers(path):
    """Both ecosystems share these tables, so a read goes through `read_ml` and a
    write through `replace_rows`; only the setup, the freeze gate and the library
    name the full table."""
    if path.name in _SHARED_NAME_ALLOWED:
        return
    text = path.read_text(encoding="utf-8")
    for name in load_ml_common()["ML_SHARED_TABLES"]:
        assert f'ml_fqn("{name}"' not in text, f"{_rel(path)}: {name}"
        assert f"ml_fqn('{name}'" not in text, f"{_rel(path)}: {name}"


def test_the_gate_notebook_exists():
    assert (_ML / "gate" / "01_ml_gates.py").exists()


def test_write_ml_refuses_a_frozen_dataset_before_any_write():
    text = (_ML / "_ml_common.py").read_text()
    for name in ("def write_ml(", "def write_dataset_rows("):
        body = text[text.index(name) :]
        assert body.index("_frozen_guard(") < body.index(".saveAsTable(")


def test_the_partition_frame_returns_the_fold_as_an_int():
    """The family table and the manifest keep `fold_id` as int; a grouped fold is
    a bigint hash, and Delta refuses to merge the two types."""
    text = (_ML / "_ml_common.py").read_text()
    body = text[text.index("def partition_frame(") :]
    body = body[: body.index("\ndef ")]
    assert 'F.col("fold_id").cast("int")' in body.rsplit("return", 1)[1]


def test_freeze_gate_protects_and_reads_the_version_before_freezing():
    text = (_ML / "gate" / "01_ml_gates.py").read_text()
    assert "delta.deletedFileRetentionDuration" in text
    assert "delta.logRetentionDuration" in text
    assert 'format("noop")' in text
    call = text.index("error = protect_and_read(full, version)")
    assert call < text.index("SET status = 'frozen'")


def test_the_freeze_pins_a_family_only_when_every_dataset_of_it_passed():
    text = (_ML / "gate" / "01_ml_gates.py").read_text()
    assert "waiting = [d for d in built" in text
    assert "if waiting or failed:" in text
    call = text.index("error = protect_and_read(full, version)")
    assert text.index("if waiting or failed:") < call
