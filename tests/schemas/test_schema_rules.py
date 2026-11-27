"""Schema, mapping and contract rules -- structure checks, no Spark, no data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

One file for the source contracts and their mappings (energy and weather), the
Bronze merge rules, the source-to-ecosystem map, the cross-ecosystem join rules and
the static structure of the shared_conformed setup notebook. The Bronze data lives
in Databricks, so no test validates rows.
"""

from __future__ import annotations

import ast
import datetime as dt
import json
import re
from pathlib import Path

import pytest
import yaml

from src.schemas._generate_field_classes import _semantic_namespace
from src.schemas.reference.cross_ecosystem_join_rules import (
    CONFORMED_DIMENSIONS,
    ENERGY_LOCAL_DIMENSIONS,
    JoinMechanism,
    JoinRequest,
    validate_cross_ecosystem_join,
)
from tests._notebook_loader import REPO_ROOT, genomics_bronze_sources

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_SCHEMAS = REPO_ROOT / "src" / "schemas"
_CONTRACTS = _SCHEMAS / "contracts"
_MAPPINGS = _SCHEMAS / "mappings"
_GENERATED = _CONTRACTS / "generated"
_REGISTRY_DIR = _SCHEMAS / "bronze_registry"
_MERGES = _MAPPINGS / "bronze_merges.yml"
_SNAPSHOT = _REGISTRY_DIR / "bronze_schema_v005.md"
_MAP_PATH = _SCHEMAS / "reference" / "source_ecosystem_map.yml"
_ENERGY_LOADER = (
    REPO_ROOT / "databricks" / "bronze" / "01_load_energy_commerce_bronze.py"
)
_SHARED_CONFORMED = REPO_ROOT / "databricks" / "setup" / "02_create_shared_conformed.py"

ENERGY_WEATHER_SOURCES = ("dwd", "smard", "power_plant_list", "redispatch", "mastr")
# Sources whose logical datasets are stored, in Bronze, as slices of a shared table.
BRONZE_LAYOUT_SOURCES = ("dwd", "honda_iot", "mastr")
# bronze: block keys of a shared table, outer pair first.
DISCRIMINATOR_KEYS = (
    ("discriminator", "value"),
    ("inner_discriminator", "inner_value"),
)
_DWD_NON_VALUE = re.compile(r"^(STATIONS_ID|city|MESS_DATUM|QN_\d+|eor)$")
_CONFORMED_TABLES = (
    "dim_date",
    "dim_time",
    "dim_geography",
    "geo_plz_gemeinde_xref",
    "dim_weather_context",
)


def _latest_registry() -> Path:
    return max(_REGISTRY_DIR.glob("bronze_schema_v*.md"))


def _registry() -> dict[str, list[str]]:
    """table name -> ordered column list of the schema `bronze`, parsed from the
    latest Bronze snapshot. Two row formats: `| table | # | column |` (to v004) and
    `| schema | table | # | column |` (from v005, `bronze` and `genomics_bronze`)."""
    with_schema = re.compile(
        r"\|\s*([a-z0-9_]+)\s*\|\s*([a-z0-9_]+)\s*\|\s*(\d+)\s*\|\s*([^\s|]+)\s*\|"
    )
    plain = re.compile(r"\|\s*([a-z0-9_]+)\s*\|\s*(\d+)\s*\|\s*([^\s|]+)\s*\|")
    acc: dict[str, list[tuple[int, str]]] = {}
    for line in _latest_registry().read_text(encoding="utf-8").splitlines():
        m = with_schema.match(line)
        if m:
            if m.group(1) != "bronze":
                continue
            table, position, column = m.group(2), m.group(3), m.group(4)
        else:
            m = plain.match(line)
            if not m:
                continue
            table, position, column = m.groups()
        acc.setdefault(table, []).append((int(position), column))
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


def _physical(table: dict) -> tuple[str, list[str], tuple]:
    """(Bronze table, its leading discriminator columns, the values of this
    dataset) a contract table is stored in."""
    bronze = table.get("bronze", {})
    pairs = [
        (bronze[column], bronze[value])
        for column, value in DISCRIMINATOR_KEYS
        if bronze.get(column)
    ]
    return (
        bronze.get("table", table["name"]),
        [column for column, _ in pairs],
        tuple(value for _, value in pairs),
    )


def _genomics_sources() -> set[str]:
    """The source folders of genomics_bronze, read from the Bronze config."""
    return set(genomics_bronze_sources())


def _staged_tables() -> set[str]:
    sources = genomics_bronze_sources()
    return {f"{s}_{d}" for s, datasets in sources.items() for d in datasets}


def _energy_loaded_tables() -> set[str]:
    """`mastr_<dataset>` for every MaStR dataset the energy loader writes."""
    tree = ast.parse(_ENERGY_LOADER.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "MASTR_DATASETS" for t in node.targets
        ):
            return {f"mastr_{d}" for d in ast.literal_eval(node.value)}
    message = "MASTR_DATASETS not found in the energy Bronze loader"
    raise AssertionError(message)


@pytest.fixture(scope="module")
def registry() -> dict[str, list[str]]:
    return _registry()


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


@pytest.fixture(scope="module")
def doc() -> dict:
    return yaml.safe_load(_MAP_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def notebook_text() -> str:
    return _SHARED_CONFORMED.read_text(encoding="utf-8")


# --- energy and weather source contracts ------------------------------------


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_parses_and_names_the_source(source):
    contract = _contract(source)
    assert contract["source"] == source
    assert contract["source_system"] == source
    assert contract.get("localisation", {}).get("required") is True


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_tables_exist_in_bronze_registry(source, registry):
    for table in _contract_tables(_contract(source)):
        physical, _, _ = _physical(table)
        assert physical in registry, f"{physical} not in the Bronze snapshot"


@pytest.mark.parametrize("source", ENERGY_WEATHER_SOURCES)
def test_contract_columns_match_bronze_registry_order(source, registry):
    """An own table matches the snapshot column for column; a dataset of a
    shared table has its columns in that table (which holds the columns of
    every dataset, ordered by the first dataset that has each)."""
    for table in _contract_tables(_contract(source)):
        physical, discriminators, _ = _physical(table)
        got = [c["name"] for c in table["columns"]]
        stored = registry[physical]
        if discriminators:
            assert stored[: len(discriminators)] == discriminators, (
                f"{physical}: {discriminators} are not the first Bronze columns"
            )
            missing = sorted(set(got) - set(stored[len(discriminators) :]))
            assert not missing, f"{table['name']}: not in {physical}: {missing}"
            continue
        assert got == stored, (
            f"{table['name']}: contract columns diverge from the Bronze snapshot"
        )


@pytest.mark.parametrize("source", BRONZE_LAYOUT_SOURCES)
def test_shared_bronze_tables_have_one_value_per_dataset(source, registry):
    """Every logical dataset stored in a shared Bronze table has its own value."""
    seen: dict[str, dict[tuple, str]] = {}
    for table in _contract_tables(_contract(source)):
        physical, discriminators, value = _physical(table)
        assert physical in registry, f"{physical} not in the Bronze snapshot"
        if not discriminators:
            continue
        values = seen.setdefault(physical, {})
        assert value not in values, f"{physical}: value {value!r} used twice"
        values[value] = table["name"]
        assert registry[physical][: len(discriminators)] == discriminators
    assert seen, f"{source} declares no shared Bronze table"


@pytest.mark.parametrize("source", BRONZE_LAYOUT_SOURCES)
def test_shared_bronze_datasets_have_identical_columns(source):
    """Datasets told apart only by the inner discriminator (one part of a merged
    table, such as the P and W data of a Honda meter) have the same columns."""
    by_part: dict[tuple, list[list[str]]] = {}
    for table in _contract_tables(_contract(source)):
        physical, discriminators, value = _physical(table)
        if len(discriminators) > 1:
            by_part.setdefault((physical, value[0]), []).append(
                [c["name"] for c in table["columns"]]
            )
    for part, column_lists in by_part.items():
        assert all(cols == column_lists[0] for cols in column_lists), (
            f"{part}: datasets stored together have different columns"
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
    jsonschema = pytest.importorskip("jsonschema")
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
    pytest.importorskip("jsonschema")
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


# --- bronze_merges.yml ------------------------------------------------------


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
    loaded = _energy_loaded_tables()
    for merge in merges:
        for part in merge["parts"].values():
            if merge.get("at_load"):
                assert part in staged, part
            else:
                # a table of the snapshot, or one that the loader of its schema writes
                written = loaded if merge["schema"] == "bronze" else staged
                assert (merge["schema"], part) in snapshot or part in written, part


def test_an_appended_merge_lists_its_kept_values_as_parts(merges):
    for merge in merges:
        if merge.get("append") and merge["table"] != "mastr_code_lookup":
            assert set(merge["kept"]) <= set(merge["parts"]), merge["table"]


def test_a_replaced_value_is_a_part_of_an_appended_merge_and_not_kept(merges):
    for merge in merges:
        replaced = set(merge.get("replace", []))
        if replaced:
            assert merge.get("append"), merge["table"]
            assert replaced <= set(merge["parts"]), merge["table"]
            assert not replaced & set(merge["kept"]), merge["table"]


def test_a_merge_without_a_discriminator_is_built_at_load(merges):
    for merge in merges:
        if not merge["discriminator"]:
            assert merge.get("at_load"), merge


def test_dwd_values_are_the_own_value_columns_of_each_table(snapshot):
    tables = _semantic_namespace()["DWD_TABLES"]
    for table, meta in tables.items():
        own = [c for c in snapshot[("bronze", table)] if not _DWD_NON_VALUE.match(c)]
        assert sorted(meta["values"]) == sorted(own), table


# --- source_ecosystem_map.yml -----------------------------------------------


def test_file_exists_and_parses(doc):
    assert doc["version"] == 1
    assert "governance" in doc


def test_allowed_ecosystems_is_closed_vocabulary(doc):
    allowed = doc["allowed_ecosystems"]
    assert allowed == ["energy", "commerce", "genomics"], (
        "allowed_ecosystems must only grow via a governed change -- if this "
        "fails because a new value was added, confirm an architecture "
        "decision backs it."
    )


def test_every_mapping_uses_an_allowed_ecosystem(doc):
    allowed = set(doc["allowed_ecosystems"])
    for entry in doc["mappings"]:
        assert entry["ecosystem"] in allowed, entry


def test_every_source_maps_to_exactly_one_ecosystem(doc):
    sources = [entry["source_system"] for entry in doc["mappings"]]
    assert len(sources) == len(set(sources)), "duplicate source_system entries"


def test_expected_current_scope_sources_present(doc):
    sources = {entry["source_system"] for entry in doc["mappings"]}
    expected = {
        "smard",
        "dwd",
        "mastr",
        "power_plant_list",
        "redispatch",
        "honda_iot",
        "energy_charts",
        "era5",
        "entsoe_observatory",
        "rees46",
        "ga4",
        "eurostat",
        "vg250",
    } | _genomics_sources()
    assert sources == expected


def test_every_genomics_bronze_source_maps_to_genomics(doc):
    mapped = {
        e["source_system"]: e["ecosystem"]
        for e in doc["mappings"]
        if e["source_system"] in _genomics_sources()
    }
    assert set(mapped) == _genomics_sources()
    assert set(mapped.values()) == {"genomics"}


def test_current_scope_sources_map_to_the_expected_ecosystem(doc):
    expected_ecosystem = {
        "smard": "energy",
        "dwd": "energy",
        "mastr": "energy",
        "power_plant_list": "energy",
        "redispatch": "energy",
        "honda_iot": "energy",
        "energy_charts": "energy",
        "era5": "energy",
        "entsoe_observatory": "energy",
        "rees46": "commerce",
        "ga4": "commerce",
        "eurostat": "commerce",
        "vg250": "energy",
        **dict.fromkeys(_genomics_sources(), "genomics"),
    }
    for entry in doc["mappings"]:
        assert entry["ecosystem"] == expected_ecosystem[entry["source_system"]], entry


def test_historical_out_of_scope_sources_are_not_in_mappings(doc):
    historical = {e["source_system"] for e in doc["historical_out_of_scope"]}
    current = {e["source_system"] for e in doc["mappings"]}
    assert historical == {"ipinyou", "criteo_attribution", "kddcup2012_track2"}
    assert historical.isdisjoint(current), (
        "a historically out-of-scope source must not also appear in the live mappings"
    )


# --- cross-ecosystem join rules ---------------------------------------------


def test_conformed_and_energy_local_sets_are_disjoint():
    assert CONFORMED_DIMENSIONS.isdisjoint(ENERGY_LOCAL_DIMENSIONS)


def test_conformed_set_is_the_expected_closed_set():
    assert CONFORMED_DIMENSIONS == {
        "dim_date",
        "dim_time",
        "dim_geography",
        "dim_weather_context",
    }


def _join(mechanism, basis, *, right="mobility", left="energy", description=""):
    return validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem=left,
            right_ecosystem=right,
            mechanism=mechanism,
            basis=basis,
            description=description,
        )
    )


def test_same_ecosystem_join_is_rejected_as_not_applicable():
    result = _join(JoinMechanism.CONFORMED_DIMENSION, "dim_date", right="energy")
    assert not result.allowed


def test_legitimate_conformed_dimension_join_is_allowed():
    assert _join(JoinMechanism.CONFORMED_DIMENSION, "dim_geography").allowed


def test_energy_local_dimension_is_not_a_legitimate_conformed_basis():
    assert not _join(JoinMechanism.CONFORMED_DIMENSION, "dim_customer").allowed


@pytest.mark.parametrize(
    "description",
    [
        "patient <-> energy customer by matching name and postcode",
        "vehicle owner <-> energy customer",
        "farm operator <-> retail customer",
        "hospital <-> advertiser",
        "charging-session user <-> commerce user",
        "assume it's the same person across both datasets",
        "same household inferred from shared address",
    ],
)
def test_named_identity_join_anti_patterns_are_rejected(description):
    result = _join(
        JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER,
        "looks-like-a-match",
        right="healthcare",
        description=description,
    )
    assert not result.allowed


def test_shared_authoritative_identifier_join_is_allowed_when_named():
    basis = "AGS (official German administrative code)"
    assert _join(JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER, basis).allowed


def test_unnamed_shared_authoritative_identifier_is_rejected():
    assert not _join(JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER, "   ").allowed


def test_real_infrastructure_relationship_join_is_allowed_when_named():
    basis = (
        "grid connection point present in both an energy-asset register and a "
        "charging-point register"
    )
    assert _join(JoinMechanism.REAL_INFRASTRUCTURE_RELATIONSHIP, basis).allowed


# --- shared_conformed setup notebook ----------------------------------------


def test_notebook_creates_the_schema(notebook_text):
    assert "CREATE SCHEMA IF NOT EXISTS {FQ}" in notebook_text
    assert 'SCHEMA = "shared_conformed"' in notebook_text


@pytest.mark.parametrize("table", _CONFORMED_TABLES)
def test_notebook_defines_each_table(notebook_text, table):
    assert f"CREATE TABLE IF NOT EXISTS {{FQ}}.{table} (" in notebook_text


def test_structure_only_tables_are_not_populated(notebook_text):
    for table in ("geo_plz_gemeinde_xref", "dim_weather_context"):
        assert f'saveAsTable(f"{{FQ}}.{table}")' not in notebook_text


def _module_consts() -> dict:
    ns: dict = {}
    for node in ast.walk(ast.parse(_SHARED_CONFORMED.read_text(encoding="utf-8"))):
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
