"""shared_conformed Databricks build notebook -- static structure checks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

No Spark, no database. The notebook
``databricks/setup/03_create_shared_conformed.py`` creates the
``shared_conformed`` schema and its five conformed dimension tables and
populates ``dim_date`` / ``dim_time`` / ``dim_geography`` inline. These tests
assert the notebook parses, names every table, carries the header, and that
its date/time range constants describe a gapless, complete seed.
"""

from __future__ import annotations

import ast
import datetime as dt
from pathlib import Path

import pytest

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_NB = (
    Path(__file__).resolve().parents[2]
    / "databricks"
    / "setup"
    / "03_create_shared_conformed.py"
)
_TABLES = (
    "dim_date",
    "dim_time",
    "dim_geography",
    "geo_plz_gemeinde_xref",
    "dim_weather_context",
)


@pytest.fixture(scope="module")
def source() -> str:
    return _NB.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def tree(source: str) -> ast.AST:
    return ast.parse(source)


def test_notebook_parses_and_has_header(source: str, tree: ast.AST) -> None:
    assert source.startswith("# Databricks notebook source")
    assert "# MAGIC **Author:** Sharique Mohammad" in source
    assert "# MAGIC **Date:**" in source
    assert "ECRMAP" in source


def test_notebook_creates_the_schema(source: str) -> None:
    assert "CREATE SCHEMA IF NOT EXISTS {FQ}" in source
    assert 'SCHEMA = "shared_conformed"' in source


@pytest.mark.parametrize("table", _TABLES)
def test_notebook_defines_each_table(source: str, table: str) -> None:
    assert f"CREATE TABLE IF NOT EXISTS {{FQ}}.{table} (" in source


def test_structure_only_tables_are_not_populated(source: str) -> None:
    for table in ("geo_plz_gemeinde_xref", "dim_weather_context"):
        assert f'saveAsTable(f"{{FQ}}.{table}")' not in source


def _module_consts() -> dict:
    ns: dict = {}
    for node in ast.walk(ast.parse(_NB.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in {"DATE_RANGE_START", "DATE_RANGE_END", "_BUNDESLAENDER"}:
                try:
                    ns[name] = ast.literal_eval(node.value)
                except ValueError:
                    if name in {"DATE_RANGE_START", "DATE_RANGE_END"}:
                        # datetime.date(y, m, d) call
                        args = [ast.literal_eval(a) for a in node.value.args]
                        ns[name] = dt.date(*args)
    return ns


def test_dim_date_range_covers_every_wave_history_window() -> None:
    consts = _module_consts()
    start, end = consts["DATE_RANGE_START"], consts["DATE_RANGE_END"]
    # First-wave and energy/weather-wave histories both start well after 2016.
    assert start <= dt.date(2016, 1, 1)
    assert end >= dt.date(2031, 12, 31)
    assert (end - start).days + 1 > 5000


def test_dim_geography_seed_is_nation_plus_sixteen_states() -> None:
    consts = _module_consts()
    bl = consts["_BUNDESLAENDER"]
    assert len(bl) == 16
    codes = [c for c, _ in bl]
    assert codes == [f"{i:02d}" for i in range(1, 17)]
