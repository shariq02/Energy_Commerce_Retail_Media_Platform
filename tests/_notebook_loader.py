"""Loads a Databricks "notebook source" .py file's definitions for unit
testing, without a live Spark session.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

`%run ../x` is a Databricks-notebook magic comment, not real Python -- when a
notebook is loaded as a plain file it does nothing, so a shared-library
notebook's functions are only reachable by reproducing what `%run` does:
executing the target notebook's source into the caller's namespace first.
This loader does exactly that, and stubs `pyspark` in `sys.modules` so the
module-level `from pyspark.sql import ...` imports succeed without the real
dependency installed. The stub satisfies imports only -- it has no behavior,
so any function that actually calls a Spark API is out of scope for the
tests built on this loader and must not be exercised through it.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _stub_pyspark() -> None:
    if "pyspark.sql" in sys.modules:
        return
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.Column = object
    pyspark_sql.DataFrame = object
    pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_sql_window = types.ModuleType("pyspark.sql.window")
    pyspark_sql_window.Window = object
    sys.modules.setdefault("pyspark", pyspark)
    sys.modules.setdefault("pyspark.sql", pyspark_sql)
    sys.modules.setdefault("pyspark.sql.functions", pyspark_sql_functions)
    sys.modules.setdefault("pyspark.sql.window", pyspark_sql_window)


def _exec_notebook(path: Path, namespace: dict) -> dict:
    source = path.read_text(encoding="utf-8")
    # exec is the mechanism under test here (reproducing what Databricks'
    # own %run does), not a security-relevant use -- the source is always a
    # fixed, repo-local notebook file, never external or user-controlled.
    exec(compile(source, str(path), "exec"), namespace)  # noqa: S102
    return namespace


def load_silver_common() -> dict:
    """Executes `databricks/silver/_silver_common.py` into a fresh namespace
    and returns it -- the pure (non-Spark-API) functions defined there are
    callable directly out of the returned dict."""
    _stub_pyspark()
    namespace: dict = {"__name__": "_silver_common_under_test"}
    return _exec_notebook(
        REPO_ROOT / "databricks" / "silver" / "_silver_common.py", namespace
    )


def load_gold_common() -> dict:
    """Same as `load_silver_common`, then layers `_gold_common.py` on top --
    reproducing the real notebook's own `%run ../silver/_silver_common` before
    `%run ../../_gold_common`, so `ecosystem_for` etc. are present the same
    way they are for the real Gold notebooks."""
    namespace = load_silver_common()
    return _exec_notebook(
        REPO_ROOT / "databricks" / "gold" / "_gold_common.py", namespace
    )
