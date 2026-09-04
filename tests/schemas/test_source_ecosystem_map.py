"""source_ecosystem_map.yml -- structure and closed-vocabulary conformance.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_MAP_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "schemas"
    / "reference"
    / "source_ecosystem_map.yml"
)


@pytest.fixture(scope="module")
def doc() -> dict:
    return yaml.safe_load(_MAP_PATH.read_text(encoding="utf-8"))


def test_file_exists_and_parses(doc):
    assert doc["version"] == 1
    assert "governance" in doc


def test_allowed_ecosystems_is_closed_vocabulary(doc):
    allowed = doc["allowed_ecosystems"]
    assert allowed == ["energy"], (
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
        "honda_iot",
        "rees46",
        "search_visibility_ramp_dryad",
        "synthetic_operational",
        "ga4",
    }
    assert sources == expected


def test_all_current_scope_sources_map_to_energy(doc):
    for entry in doc["mappings"]:
        assert entry["ecosystem"] == "energy"


def test_historical_out_of_scope_sources_are_not_in_mappings(doc):
    historical = {e["source_system"] for e in doc["historical_out_of_scope"]}
    current = {e["source_system"] for e in doc["mappings"]}
    assert historical == {"ipinyou", "criteo_attribution", "kddcup2012_track2"}
    assert historical.isdisjoint(current), (
        "a historically out-of-scope source must not also appear in the live mappings"
    )
