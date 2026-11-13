"""Unit tests for `scripts/utilities/check_run_chain_names.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "utilities"
    / "check_run_chain_names.py"
)
HEADER = "# Databricks notebook source\n"


@pytest.fixture(scope="module")
def checker():
    spec = importlib.util.spec_from_file_location("check_run_chain_names", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def chain(tmp_path, checker, monkeypatch):
    """A fake repo: databricks/silver/_base.py, _layer.py (runs _base) and a
    notebook in databricks/silver/energy/ that runs _layer."""
    silver = tmp_path / "databricks" / "silver"
    (silver / "energy").mkdir(parents=True)
    (silver / "_base.py").write_text(HEADER + "def helper():\n    return 1\n")
    (silver / "_layer.py").write_text(
        HEADER + "# MAGIC %run ./_base\n\ndef layer():\n    return SOURCE\n"
    )
    monkeypatch.setattr(checker, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(checker, "NOTEBOOK_ROOT", tmp_path / "databricks")
    return silver


def _notebook(silver: Path, body: str) -> Path:
    path = silver / "energy" / "01_notebook.py"
    path.write_text(HEADER + "# MAGIC %run ../_layer\n\nSOURCE = 'dwd'\n" + body)
    return path


def test_the_chain_is_depth_first_and_each_library_once(checker, chain):
    notebook = _notebook(chain, "")
    names = [path.name for path in checker.run_chain(notebook.resolve())]
    assert names == ["_base.py", "_layer.py"]


def test_the_offset_counts_the_library_lines(checker, chain):
    notebook = _notebook(chain, "")
    text, offset = checker.joined_source(notebook.resolve())
    assert text.splitlines()[offset] == HEADER.strip()


def test_a_changed_library_selects_the_notebooks_that_run_it(checker, chain):
    notebook = _notebook(chain, "")
    assert checker.notebooks(["databricks/silver/_base.py"]) == [notebook]
    assert checker.notebooks(["databricks/gold/other.py"]) == []
    assert checker.notebooks(None) == [notebook]


@pytest.mark.skipif(shutil.which("ruff") is None, reason="ruff is not installed")
def test_an_undefined_name_is_reported_at_the_notebook_line(checker, chain):
    notebook = _notebook(chain, "value = helper() + layer() + missing_name\n")
    found = checker.undefined_names(notebook)
    assert len(found) == 1
    assert found[0].startswith("databricks/silver/energy/01_notebook.py:5:")
    assert "missing_name" in found[0]


@pytest.mark.skipif(shutil.which("ruff") is None, reason="ruff is not installed")
def test_names_from_the_chain_and_late_bound_library_names_pass(checker, chain):
    notebook = _notebook(chain, "value = helper() + layer()\n")
    assert checker.undefined_names(notebook) == []
