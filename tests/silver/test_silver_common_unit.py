"""Unit tests for the pure (non-Spark-API) functions in
`databricks/silver/_silver_common.py` -- the actual transform-plumbing logic,
not structural/contract checks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Loaded via `tests/_notebook_loader.py` (reproduces `%run`), since this file
is a Databricks notebook, not an importable module, and its module-level
`from pyspark.sql import ...` would otherwise require the real dependency.
Functions that call a live Spark API (`spark.table(...)`, anything taking or
returning a `DataFrame`) are out of scope here -- they need a real cluster or
a local Spark session, neither available to this suite.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests._notebook_loader import REPO_ROOT, load_silver_common

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def silver():
    return load_silver_common()


def test_repo_root_resolves_to_the_actual_repo(silver):
    root = silver["repo_root"]()
    assert Path(root) == REPO_ROOT


def test_load_yaml_reads_a_real_file(silver):
    doc = silver["load_yaml"]("src/schemas/reference/source_ecosystem_map.yml")
    assert doc["version"] == 1
    assert "mappings" in doc


def test_load_contract_and_load_mapping_resolve_the_conventional_path(silver):
    contract = silver["load_contract"]("dwd")
    mapping = silver["load_mapping"]("dwd")
    assert isinstance(contract, dict) and contract
    assert isinstance(mapping, dict) and mapping


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("smard", "energy"),
        ("dwd", "energy"),
        ("mastr", "energy"),
        ("honda_iot", "energy"),
        ("rees46", "commerce"),
        ("ga4", "commerce"),
        ("clinvar", "genomics"),
        ("gencode", "genomics"),
    ],
)
def test_ecosystem_for_matches_the_governed_map(silver, source, expected):
    assert silver["ecosystem_for"](source) == expected


def test_ecosystem_for_strict_raises_on_an_unknown_source(silver):
    with pytest.raises(ValueError, match="source_ecosystem_map"):
        silver["ecosystem_for"]("a_source_that_does_not_exist", strict=True)


def test_ecosystem_for_strict_accepts_a_mapped_source(silver):
    assert silver["ecosystem_for"]("clinvar", strict=True) == "genomics"


def test_ecosystem_for_unknown_source_falls_back_to_the_documented_default(silver):
    # _DEFAULT_ECOSYSTEM in _silver_common.py -- asserted by value, not by
    # re-reading the constant, so this catches an accidental default change.
    assert silver["ecosystem_for"]("a_source_that_does_not_exist") == "energy"


def test_contract_tables_accepts_the_multi_table_form(silver):
    contract = {
        "tables": [
            {"name": "a", "columns": [{"name": "x"}]},
            {"name": "b", "columns": [{"name": "y"}]},
            {"name": "c_no_columns"},
        ]
    }
    tables = silver["contract_tables"](contract)
    assert set(tables) == {"a", "b"}
    assert tables["a"]["columns"] == [{"name": "x"}]


def test_contract_tables_accepts_the_legacy_single_table_form(silver):
    contract = {
        "source": "dwd",
        "bronze_table": "bronze.dwd_hourly_air_temperature",
        "columns": [{"name": "mess_datum"}],
    }
    tables = silver["contract_tables"](contract)
    assert set(tables) == {"dwd_hourly_air_temperature"}


def test_contract_tables_returns_empty_for_neither_form(silver):
    assert silver["contract_tables"]({"unrelated": True}) == {}


def test_flatten_business_names_is_a_real_dwd_mapping(silver):
    mapping = silver["load_mapping"]("dwd")
    business_names = silver["flatten_business_names"](mapping, "dwd")
    assert isinstance(business_names, dict) and business_names
    assert all(isinstance(v, str) for v in business_names.values())


def test_coded_columns_returns_the_dwd_decode_maps(silver):
    mapping = silver["load_mapping"]("dwd")
    coded = silver["coded_columns"](mapping, "dwd")
    assert isinstance(coded, dict) and coded
    # WW is a real DWD coded column with no map (present-quantity code table
    # too large to enumerate) -- confirms the None-passthrough case, not just
    # the populated-map case
    assert "WW" in coded
    assert coded["WW"] is None


def test_coded_columns_mastr_uses_the_named_category_binding_shape(silver):
    mapping = silver["load_mapping"]("mastr")
    coded = silver["coded_columns"](mapping, "mastr")
    assert isinstance(coded, dict)


def test_untranslated_source_columns_flags_source_names_only(silver):
    # a MaStR `_nv` flag is decoded before this check, so one left over is flagged
    found = silver["untranslated_source_columns"](
        ["unit_id", "Strasse", "MastrNummer", "Fax_nv", "capacity_gross_kw"]
    )
    assert found == ["Strasse", "MastrNummer", "Fax_nv"]


def test_mastr_not_available_name_uses_the_english_field(silver):
    name = silver["mastr_not_available_name"]
    assert name("Hausnummer_nv", {"Hausnummer": "house_number"}) == (
        "house_number_not_available"
    )


def test_a_not_available_flag_is_no_mastr_date(silver):
    is_date = silver["_is_mastr_date"]
    assert not is_date("water_rights_expiry_date_not_available")
    assert is_date("water_rights_expiry_date")
    assert is_date("authorisation_deadline")


def test_biomethane_first_use_is_a_mastr_date(silver):
    assert silver["_is_mastr_date"]("biomethane_first_use")


class _Writer:
    def __init__(self, spark_stub):
        self.spark_stub = spark_stub

    def format(self, _name):
        return self

    def mode(self, _name):
        return self

    def saveAsTable(self, table):
        self.spark_stub.inserts.append((table, len(self.spark_stub.frame_rows)))


class _SparkStub:
    """Records one insert per `flush_logs` table; no Spark behaviour."""

    def __init__(self):
        self.inserts = []
        self.frame_rows = []

    def createDataFrame(self, rows, _ddl):
        self.frame_rows = list(rows)
        return type("Frame", (), {"write": _Writer(self)})()


def test_log_rows_wait_for_one_insert_per_log_table(silver, monkeypatch):
    stub = _SparkStub()
    monkeypatch.setitem(silver, "spark", stub)
    silver["_AUDIT_ROWS"].clear()
    silver["_WATERMARK_ROWS"].clear()
    started = silver["now_utc"]()
    for name in ("a", "b", "c"):
        silver["audit"]("comp", "src", name, 1.0, rid="run")
    silver["watermark"]("comp", "src", 5, "COMPLETE", "run", started)
    assert stub.inserts == []
    silver["flush_logs"]()
    tables = [table for table, _ in stub.inserts]
    assert tables == [silver["AUDIT_TABLE"], silver["WATERMARK_TABLE"]]
    assert silver["_AUDIT_ROWS"] == [] and silver["_WATERMARK_ROWS"] == []
    silver["flush_logs"]()
    assert len(stub.inserts) == 2


def test_audit_row_matches_the_log_table_columns(silver):
    silver["_AUDIT_ROWS"].clear()
    silver["audit"]("comp", "src", "rows_written", 7, status="PASS", rid="run")
    row = silver["_AUDIT_ROWS"].pop()
    assert len(row) == len(silver["_AUDIT_DDL"].split(","))
    assert (row[0], row[2], row[4], row[5], row[6], row[8]) == (
        "run",
        "src",
        "comp",
        "rows_written",
        7.0,
        "PASS",
    )


class _HistoryStub:
    def __init__(self, rows):
        self.rows = rows

    def sql(self, _statement):
        return self

    def select(self, *_columns):
        return self

    def collect(self):
        return self.rows


def test_delta_write_result_reads_rows_and_the_version_before(silver, monkeypatch):
    history = [
        {"version": 4, "operationMetrics": {"numOutputRows": "120"}},
        {"version": 3, "operationMetrics": {}},
    ]
    monkeypatch.setitem(silver, "spark", _HistoryStub(history))
    assert silver["delta_write_result"]("t") == (120, 3)


def test_delta_write_result_of_a_new_table_has_no_version_before(silver, monkeypatch):
    history = [{"version": 0, "operationMetrics": {"numOutputRows": "9"}}]
    monkeypatch.setitem(silver, "spark", _HistoryStub(history))
    assert silver["delta_write_result"]("t") == (9, None)
