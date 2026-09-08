"""Static checks on the energy_silver notebook sources -- no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

The inputs are the files under ``databricks/silver/`` and the field-class seed
``src/schemas/field_classes/energy_silver_field_classes.csv``. The tests assert
each notebook parses and carries the standard header, every table a notebook
writes is classified in the seed, the seed matches its generator, and the
shared library / notebooks introduce no ``canonical_id`` or
``monotonically_increasing_id``.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_SILVER = _ROOT / "databricks" / "silver"
_SEED = _ROOT / "src" / "schemas" / "field_classes" / "energy_silver_field_classes.csv"
_GENERATOR = _ROOT / "src" / "schemas" / "_generate_field_classes.py"

_NOTEBOOKS = sorted(
    p
    for p in _SILVER.rglob("*.py")
    if p.name != "_silver_common.py" and not p.name.startswith("_generate")
)
# forbidden as real code use (quoted column name / function call), not prose
_FORBIDDEN = ('"canonical_id"', "'canonical_id'", "F.monotonically_increasing_id")


def _seed_tables() -> set[str]:
    lines = _SEED.read_text(encoding="utf-8").splitlines()[1:]
    return {ln.split(",", 1)[0] for ln in lines if ln}


# function name -> positional index of the Silver-table-name argument
_TABLE_ARG = {"write_silver": 1, "explode_link_bridge": 3}


def _write_silver_targets(tree: ast.AST) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        idx = _TABLE_ARG.get(node.func.id)
        if idx is None or len(node.args) <= idx:
            continue
        arg = node.args[idx]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            out.add(arg.value)
    return out


def _code_lines(text: str) -> str:
    """Drop comment / MAGIC lines so prose mentions do not trip token checks."""
    return "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))


@pytest.mark.parametrize("nb", _NOTEBOOKS, ids=lambda p: str(p.relative_to(_SILVER)))
def test_notebook_parses_and_has_header(nb: Path) -> None:
    text = nb.read_text(encoding="utf-8")
    ast.parse(text)
    assert text.startswith("# Databricks notebook source")
    assert "# MAGIC **Author:** Sharique Mohammad" in text
    assert "# MAGIC **Date:**" in text
    assert "ECRMAP" in text


@pytest.mark.parametrize("nb", _NOTEBOOKS, ids=lambda p: str(p.relative_to(_SILVER)))
def test_no_forbidden_silver_boundary_tokens(nb: Path) -> None:
    code = _code_lines(nb.read_text(encoding="utf-8"))
    for token in _FORBIDDEN:
        assert token not in code, f"{nb.name} references forbidden `{token}`"


def test_shared_library_has_no_forbidden_tokens() -> None:
    code = _code_lines((_SILVER / "_silver_common.py").read_text(encoding="utf-8"))
    for token in _FORBIDDEN:
        assert token not in code


def test_every_written_silver_table_is_classified() -> None:
    classified = _seed_tables()
    missing: dict[str, set[str]] = {}
    for nb in _NOTEBOOKS:
        targets = _write_silver_targets(ast.parse(nb.read_text(encoding="utf-8")))
        gap = targets - classified
        if gap:
            missing[nb.name] = gap
    assert not missing, f"Silver tables absent from the field-class seed: {missing}"


def test_field_class_seed_is_not_stale() -> None:
    result = subprocess.run(
        [sys.executable, str(_GENERATOR), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
