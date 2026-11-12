"""bronze_merges.yml -- every part is a known Bronze table, once; DWD own columns.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

from src.schemas._generate_field_classes import _semantic_namespace

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_MERGES = _ROOT / "src" / "schemas" / "mappings" / "bronze_merges.yml"
_SNAPSHOT = _ROOT / "src" / "schemas" / "bronze_registry" / "bronze_schema_v005.md"
_GENOMICS_BRONZE = _ROOT / "databricks" / "bronze" / "_genomics_bronze_common.py"
_DWD_NON_VALUE = re.compile(r"^(STATIONS_ID|city|MESS_DATUM|QN_\d+|eor)$")


@pytest.fixture(scope="module")
def merges() -> list[dict]:
    return yaml.safe_load(_MERGES.read_text(encoding="utf-8"))["merges"]


@pytest.fixture(scope="module")
def snapshot() -> dict[tuple[str, str], list[str]]:
    """(schema, table) -> columns in order, from the Bronze snapshot."""
    tables: dict[tuple[str, str], list[str]] = {}
    for line in _SNAPSHOT.read_text(encoding="utf-8").splitlines():
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) == 6 and cells[0] in {"bronze", "genomics_bronze"}:
            tables.setdefault((cells[0], cells[1]), []).append(cells[3])
    return tables


def _staged_tables() -> set[str]:
    tree = ast.parse(_GENOMICS_BRONZE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "SOURCES" for t in node.targets
        ):
            sources = ast.literal_eval(node.value)
            return {f"{s}_{d}" for s, datasets in sources.items() for d in datasets}
    message = "SOURCES not found in the genomics Bronze config"
    raise AssertionError(message)


def test_every_merge_names_a_schema_a_table_and_parts(merges):
    for merge in merges:
        assert merge["schema"] in {"bronze", "genomics_bronze"}, merge
        assert merge["table"], merge
        assert "discriminator" in merge, merge
        assert merge["parts"], merge
        if merge.get("append"):
            assert merge["kept"], merge


def test_a_table_and_a_part_appear_once(merges):
    tables = [(m["schema"], m["table"]) for m in merges]
    parts = [(m["schema"], p) for m in merges for p in m["parts"].values()]
    assert len(tables) == len(set(tables))
    assert len(parts) == len(set(parts))
    assert not set(parts) & {t for t, m in zip(tables, merges) if not m.get("append")}


def test_parts_are_loaded_or_staged_tables(merges, snapshot):
    staged = _staged_tables()
    for merge in merges:
        for part in merge["parts"].values():
            if merge.get("at_load"):
                assert part in staged, part
            else:
                assert (merge["schema"], part) in snapshot, part


def test_a_merge_without_a_discriminator_is_built_at_load(merges):
    for merge in merges:
        if not merge["discriminator"]:
            assert merge.get("at_load"), merge


def test_dwd_values_are_the_own_value_columns_of_each_table(snapshot):
    tables = _semantic_namespace()["DWD_TABLES"]
    for table, meta in tables.items():
        own = [c for c in snapshot[("bronze", table)] if not _DWD_NON_VALUE.match(c)]
        assert sorted(meta["values"]) == sorted(own), table
