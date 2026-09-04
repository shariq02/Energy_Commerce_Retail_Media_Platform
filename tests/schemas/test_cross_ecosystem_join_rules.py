"""cross_ecosystem_join_rules.py -- the join rules enforced as real logic.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026
"""

from __future__ import annotations

import pytest

from src.schemas.reference.cross_ecosystem_join_rules import (
    CONFORMED_DIMENSIONS,
    ENERGY_LOCAL_DIMENSIONS,
    JoinMechanism,
    JoinRequest,
    validate_cross_ecosystem_join,
)

pytestmark = [pytest.mark.schema, pytest.mark.unit]


def test_conformed_and_energy_local_sets_are_disjoint():
    assert CONFORMED_DIMENSIONS.isdisjoint(ENERGY_LOCAL_DIMENSIONS)


def test_conformed_set_is_the_expected_closed_set():
    assert CONFORMED_DIMENSIONS == {
        "dim_date",
        "dim_time",
        "dim_geography",
        "dim_weather_context",
    }


def test_same_ecosystem_join_is_rejected_as_not_applicable():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="energy",
            mechanism=JoinMechanism.CONFORMED_DIMENSION,
            basis="dim_date",
        )
    )
    assert not result.allowed


def test_legitimate_conformed_dimension_join_is_allowed():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="mobility",
            mechanism=JoinMechanism.CONFORMED_DIMENSION,
            basis="dim_geography",
        )
    )
    assert result.allowed


def test_energy_local_dimension_is_not_a_legitimate_conformed_basis():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="mobility",
            mechanism=JoinMechanism.CONFORMED_DIMENSION,
            basis="dim_customer",
        )
    )
    assert not result.allowed


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
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="healthcare",
            mechanism=JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER,
            basis="looks-like-a-match",
            description=description,
        )
    )
    assert not result.allowed


def test_shared_authoritative_identifier_join_is_allowed_when_named():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="mobility",
            mechanism=JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER,
            basis="AGS (official German administrative code)",
        )
    )
    assert result.allowed


def test_unnamed_shared_authoritative_identifier_is_rejected():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="mobility",
            mechanism=JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER,
            basis="   ",
        )
    )
    assert not result.allowed


def test_real_infrastructure_relationship_join_is_allowed_when_named():
    result = validate_cross_ecosystem_join(
        JoinRequest(
            left_ecosystem="energy",
            right_ecosystem="mobility",
            mechanism=JoinMechanism.REAL_INFRASTRUCTURE_RELATIONSHIP,
            basis="grid connection point present in both an energy-asset register and a charging-point register",
        )
    )
    assert result.allowed
