"""Synthetic operational data -- conformance to its contract and generated schemas.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import json

import pytest
import yaml

from src.generators import config

jsonschema = pytest.importorskip("jsonschema")
pytestmark = pytest.mark.schema

_CONTRACT = (
    config.REPO_ROOT / "src" / "schemas" / "contracts" / "synthetic_operational.yml"
)
_GENERATED = config.REPO_ROOT / "src" / "schemas" / "contracts" / "generated"


@pytest.fixture(scope="module")
def contract() -> dict:
    return yaml.safe_load(_CONTRACT.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def contract_tables(contract) -> dict[str, dict]:
    return {t["name"]: t for t in contract["tables"]}


def _cast(value: str, col: dict):
    if col.get("nullable") and value == "":
        return None
    ctype = col["type"]
    if ctype in ("integer", "int", "long"):
        return int(value)
    if ctype in ("decimal", "double", "float"):
        return float(value)
    if ctype == "boolean":
        return {"true": True, "false": False}[value]
    return value


def test_contract_covers_exactly_the_seven_tables(contract_tables):
    assert set(contract_tables) == set(config.TABLE_LOAD_ORDER)


@pytest.mark.parametrize("table", config.TABLE_LOAD_ORDER)
def test_contract_columns_match_csv_column_order(contract_tables, table):
    contract_cols = [c["name"] for c in contract_tables[table]["columns"]]
    assert contract_cols == config.TABLE_COLUMNS[table]


@pytest.mark.parametrize("table", config.TABLE_LOAD_ORDER)
def test_generated_schema_is_valid(table):
    path = _GENERATED / f"{table}.schema.json"
    assert path.exists(), f"missing generated schema for {table}"
    schema = json.loads(path.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)


@pytest.mark.parametrize("table", config.TABLE_LOAD_ORDER)
def test_built_rows_validate_against_generated_schema(
    built_tables, contract_tables, table
):
    schema = json.loads(
        (_GENERATED / f"{table}.schema.json").read_text(encoding="utf-8")
    )
    validator = jsonschema.Draft202012Validator(schema)
    cols = {c["name"]: c for c in contract_tables[table]["columns"]}
    for row in built_tables[table][:200]:
        typed = {k: _cast(v, cols[k]) for k, v in row.items() if k in cols}
        errors = sorted(validator.iter_errors(typed), key=str)
        assert not errors, f"{table}: {errors[0].message} in {typed}"


def test_enum_columns_declare_allowed_values(contract_tables, built_tables):
    for table, tdef in contract_tables.items():
        for col in tdef["columns"]:
            allowed = col.get("allowed_values")
            if not allowed:
                continue
            seen = {r[col["name"]] for r in built_tables[table]} - {""}
            assert seen <= set(allowed), f"{table}.{col['name']}: {seen - set(allowed)}"
