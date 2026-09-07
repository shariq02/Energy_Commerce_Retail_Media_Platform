"""Energy & weather source contracts -- contract / mapping / generated-schema conformance.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Covers the German energy & weather deepening sources: DWD (weather), SMARD
(electricity market), MaStR (generation register), the BNetzA power plant list,
and redispatch measures.

Structural checks only -- the Bronze data lives in Databricks, so these tests do
not validate rows. They assert that the five contracts, their mappings and their
generated JSON Schemas agree with the Bronze schema snapshot.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

jsonschema = pytest.importorskip("jsonschema")
pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "src" / "schemas"
_CONTRACTS = _SCHEMAS / "contracts"
_MAPPINGS = _SCHEMAS / "mappings"
_GENERATED = _CONTRACTS / "generated"
_REGISTRY = _SCHEMAS / "bronze_registry" / "bronze_schema_v001.md"

ENERGY_WEATHER_SOURCES = ("dwd", "smard", "power_plant_list", "redispatch", "mastr")


def _registry() -> dict[str, list[str]]:
    """table name -> ordered column list, parsed from the Bronze schema snapshot."""
    row = re.compile(r"\|\s*([a-z0-9_]+)\s*\|\s*(\d+)\s*\|\s*([^\s|]+)\s*\|")
    acc: dict[str, list[tuple[int, str]]] = {}
    for line in _REGISTRY.read_text(encoding="utf-8").splitlines():
        m = row.match(line)
        if m:
            acc.setdefault(m.group(1), []).append((int(m.group(2)), m.group(3)))
    return {t: [c for _, c in sorted(v)] for t, v in acc.items()}


def _contract(source: str) -> dict:
    return yaml.safe_load((_CONTRACTS / f"{source}.yml").read_text(encoding="utf-8"))


def _contract_tables(contract: dict) -> list[dict]:
    if "tables" in contract:
        return [t for t in contract["tables"] if "columns" in t]
    if "columns" in contract:
        name = contract.get("bronze_table", contract["source"]).split(".")[-1]
        return [{"name": name, "columns": contract["columns"]}]
    return []


@pytest.fixture(scope="module")
def registry() -> dict[str, list[str]]:
    return _registry()


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_parses_and_names_the_source(source):
    contract = _contract(source)
    assert contract["source"] == source
    assert contract["source_system"] == source
    assert contract.get("localisation", {}).get("required") is True


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_tables_exist_in_bronze_registry(source, registry):
    for table in _contract_tables(_contract(source)):
        assert table["name"] in registry, f"{table['name']} not in the Bronze snapshot"


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_columns_match_bronze_registry_order(source, registry):
    for table in _contract_tables(_contract(source)):
        got = [c["name"] for c in table["columns"]]
        assert got == registry[table["name"]], (
            f"{table['name']}: contract columns diverge from the Bronze snapshot"
        )


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_every_column_has_a_logical_type(source):
    allowed = {
        "string",
        "integer",
        "int",
        "long",
        "double",
        "float",
        "decimal",
        "boolean",
        "date",
        "timestamp",
    }
    for table in _contract_tables(_contract(source)):
        for col in table["columns"]:
            assert col["type"] in allowed, (
                f"{table['name']}.{col['name']}: {col['type']}"
            )


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_generated_schema_present_and_valid(source):
    for table in _contract_tables(_contract(source)):
        path = _GENERATED / f"{table['name']}.schema.json"
        assert path.exists(), (
            f"missing generated schema for {table['name']} -- run "
            f"src/schemas/_generate_jsonschema.py"
        )
        schema = json.loads(path.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator.check_schema(schema)
        assert set(schema["properties"]) == {c["name"] for c in table["columns"]}


def test_generator_check_is_clean():
    from src.schemas import _generate_jsonschema

    if not any(_GENERATED.glob("*.schema.json")):
        pytest.skip("generated schemas not built yet -- run _generate_jsonschema.py")
    assert _generate_jsonschema.generate(None, check=True) == 0, (
        "a generated schema is stale -- run src/schemas/_generate_jsonschema.py"
    )


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_mapping_references_only_contract_tables(source):
    mapping = yaml.safe_load((_MAPPINGS / f"{source}.yml").read_text(encoding="utf-8"))
    assert mapping["source"] == source
    assert mapping["ecosystem"] == "energy"
    contract_tables = {t["name"] for t in _contract_tables(_contract(source))}
    for target in mapping.get("targets", []):
        for table in target.get("from_tables", []):
            assert table in contract_tables, (
                f"{source} mapping references {table}, absent from the contract"
            )


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_mapping_has_business_name_layer(source):
    """The source -> business-meaning layer must be explicit in the mapping."""
    mapping = yaml.safe_load((_MAPPINGS / f"{source}.yml").read_text(encoding="utf-8"))
    assert "business_names" in mapping, f"{source} mapping lacks a business_names block"
    assert mapping.get("localisation", {}).get("required") is True


def test_source_ecosystem_map_covers_energy_weather_sources():
    doc = yaml.safe_load(
        (_SCHEMAS / "reference" / "source_ecosystem_map.yml").read_text(
            encoding="utf-8"
        )
    )
    mapped = {e["source_system"] for e in doc["mappings"]}
    for source in ENERGY_WEATHER_SOURCES:
        assert source in mapped, f"{source} missing from source_ecosystem_map.yml"
        assert (
            next(e for e in doc["mappings"] if e["source_system"] == source)[
                "ecosystem"
            ]
            == "energy"
        )
