"""source_ecosystem_map.yml -- structure and closed-vocabulary conformance.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_ROOT = Path(__file__).resolve().parents[2]
_MAP_PATH = _ROOT / "src" / "schemas" / "reference" / "source_ecosystem_map.yml"
_GENOMICS_BRONZE = _ROOT / "databricks" / "bronze" / "_genomics_bronze_common.py"


def _genomics_sources() -> set[str]:
    """The source folders of genomics_bronze, read from the Bronze config."""
    tree = ast.parse(_GENOMICS_BRONZE.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "SOURCES" for t in node.targets
        ):
            return set(ast.literal_eval(node.value))
    message = "SOURCES not found in the genomics Bronze config"
    raise AssertionError(message)


@pytest.fixture(scope="module")
def doc() -> dict:
    return yaml.safe_load(_MAP_PATH.read_text(encoding="utf-8"))


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
        "rees46",
        "ga4",
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
        "rees46": "commerce",
        "ga4": "commerce",
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
