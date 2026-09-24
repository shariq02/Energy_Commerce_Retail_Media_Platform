"""Static scan of notebook source for cross-layer table dependencies.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: one AST-based scanner, shared by `_generate_field_classes.py`,
`databricks/silver/00_silver_setup.py`, `databricks/gold/00_gold_setup.py`
and the test suite -- a table name literal is extracted straight from every
`write_silver(...)` / `explode_link_bridge(...)` / `write_semantic(...)` call
(Silver side) or `read_silver(...)` call (Gold side), never hand-maintained
twice.
"""

from __future__ import annotations

import ast
from pathlib import Path

# function name -> positional index of the Silver-table-name argument
TABLE_ARG = {"write_silver": 1, "explode_link_bridge": 3, "write_semantic": 1}


def silver_notebooks(silver_root: Path) -> list[Path]:
    return sorted(
        p
        for p in silver_root.rglob("*.py")
        if p.name != "_silver_common.py" and not p.name.startswith("_generate")
    )


def _module_str_constants(tree: ast.AST) -> dict[str, str]:
    """Module-level `NAME = "literal"` assignments (e.g. `TABLE = "..."`)."""
    return {
        t.id: node.value.value
        for node in getattr(tree, "body", [])
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
        for t in node.targets
        if isinstance(t, ast.Name)
    }


def write_silver_targets(tree: ast.AST) -> set[str]:
    consts = _module_str_constants(tree)
    out: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        idx = TABLE_ARG.get(node.func.id)
        if idx is None or len(node.args) <= idx:
            continue
        arg = node.args[idx]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            out.add(arg.value)
        elif isinstance(arg, ast.Name) and arg.id in consts:
            out.add(consts[arg.id])
    return out


def tables_by_notebook(silver_root: Path) -> dict[str, set[str]]:
    """notebook filename -> the table names it writes."""
    out: dict[str, set[str]] = {}
    for nb in silver_notebooks(silver_root):
        targets = write_silver_targets(ast.parse(nb.read_text(encoding="utf-8")))
        if targets:
            out[nb.name] = targets
    return out


def all_written_tables(silver_root: Path) -> set[str]:
    out: set[str] = set()
    for targets in tables_by_notebook(silver_root).values():
        out |= targets
    return out


def gold_notebooks(gold_root: Path) -> list[Path]:
    return sorted(p for p in gold_root.rglob("*.py") if not p.name.startswith("_"))


def read_silver_targets(tree: ast.AST) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        if node.func.id != "read_silver" or not node.args:
            continue
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            out.add(arg.value)
    return out


def silver_dependencies_by_gold_notebook(gold_root: Path) -> dict[str, set[str]]:
    """Gold notebook filename -> the Silver table names it reads."""
    out: dict[str, set[str]] = {}
    for nb in gold_notebooks(gold_root):
        targets = read_silver_targets(ast.parse(nb.read_text(encoding="utf-8")))
        if targets:
            out[nb.name] = targets
    return out


def all_read_silver_tables(gold_root: Path) -> set[str]:
    out: set[str] = set()
    for targets in silver_dependencies_by_gold_notebook(gold_root).values():
        out |= targets
    return out
