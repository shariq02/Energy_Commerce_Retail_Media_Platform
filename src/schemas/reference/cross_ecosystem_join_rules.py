"""Governed reference module -- cross-ecosystem join rules.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: encode the closed conformed-dimension set and the legitimate
cross-ecosystem join rule as real, testable logic -- not just prose. This
module defines no ecosystem's entities/facts/dimensions; it is a
platform-level structural rule, usable by any future validation gate without
that gate existing yet.

This module makes no live database or network call. It is pure reference
data plus pure functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# --------------------------------------------------------------------------
# The closed conformed-dimension set.
# --------------------------------------------------------------------------

# Shared across every ecosystem, in the `shared_conformed` namespace. Adding
# a further conformed dimension is a governed change requiring an
# architecture decision proving it is a shared *context*, not a shared
# *entity*.
CONFORMED_DIMENSIONS: frozenset[str] = frozenset(
    {
        "dim_date",
        "dim_time",
        "dim_geography",
        "dim_weather_context",
    }
)

# Explicitly NOT conformed -- stay Energy-local and may not be referenced by
# another ecosystem. Listed to make the boundary checkable, not just
# statable.
ENERGY_LOCAL_DIMENSIONS: frozenset[str] = frozenset(
    {
        "dim_customer",
        "dim_device",
        "dim_advertiser",
        "dim_product",
        "dim_campaign",
        "dim_market",
        "dim_weather_location",
    }
)


# --------------------------------------------------------------------------
# Legitimate cross-ecosystem join mechanisms.
# --------------------------------------------------------------------------


class JoinMechanism(str, Enum):
    """The only three permitted mechanisms."""

    CONFORMED_DIMENSION = "conformed_dimension"
    SHARED_AUTHORITATIVE_IDENTIFIER = "shared_authoritative_identifier"
    REAL_INFRASTRUCTURE_RELATIONSHIP = "real_infrastructure_relationship"


# Named anti-patterns -- identity/entity joins across ecosystems that are
# prohibited outright, regardless of mechanism claimed. Matching is case-insensitive substring matching on a
# human-supplied description, intentionally conservative (prefers a false
# "needs review" over a false "safe").
PROHIBITED_IDENTITY_JOIN_PATTERNS: tuple[str, ...] = (
    "patient",
    "vehicle owner",
    "farm operator",
    "hospital",
    "advertiser",
    "commercial entity",
    "charging-session user",
    "charging session user",
    "same person",
    "same household",
    "same organisation",
    "same organization",
)


@dataclass(frozen=True)
class JoinRequest:
    """A proposed cross-ecosystem join, described for validation.

    This is a description of intent, not a query -- no ecosystem, table, or
    column names are assumed to exist yet.
    """

    left_ecosystem: str
    right_ecosystem: str
    mechanism: JoinMechanism
    # For CONFORMED_DIMENSION: the dimension name (must be in CONFORMED_DIMENSIONS).
    # For SHARED_AUTHORITATIVE_IDENTIFIER: the identifier's name/standard
    #   (e.g. "AGS", "NUTS-3", "official register ID").
    # For REAL_INFRASTRUCTURE_RELATIONSHIP: a short description of the
    #   real-world relationship the source data explicitly encodes.
    basis: str
    # A free-text description of what is being joined, used only to screen
    # against PROHIBITED_IDENTITY_JOIN_PATTERNS -- e.g.
    # "patient <-> energy customer by matching name and postcode".
    description: str = ""


@dataclass(frozen=True)
class JoinValidationResult:
    allowed: bool
    reason: str


def validate_cross_ecosystem_join(request: JoinRequest) -> JoinValidationResult:
    """Validate a proposed cross-ecosystem join against the join rules.

    This is a structural pre-check, not a data-level guarantee -- it cannot
    know whether a claimed shared identifier or infrastructure relationship
    is genuinely present in both sources' data (that check comes later,
    against real acquired data). It rejects what is structurally illegitimate
    regardless of the data, and flags everything else for that later,
    evidence-based check.
    """
    if request.left_ecosystem == request.right_ecosystem:
        return JoinValidationResult(
            allowed=False,
            reason="Not a cross-ecosystem join (left_ecosystem == right_ecosystem) -- this rule does not apply within one ecosystem.",
        )

    description_lower = request.description.lower()
    for pattern in PROHIBITED_IDENTITY_JOIN_PATTERNS:
        if pattern in description_lower:
            return JoinValidationResult(
                allowed=False,
                reason=(
                    f"Matches a prohibited identity-join anti-pattern: "
                    f"'{pattern}'. Identity/entity joins across ecosystems are "
                    f"prohibited outright."
                ),
            )

    if request.mechanism is JoinMechanism.CONFORMED_DIMENSION:
        if request.basis not in CONFORMED_DIMENSIONS:
            return JoinValidationResult(
                allowed=False,
                reason=(
                    f"'{request.basis}' is not in the closed conformed-dimension set "
                    f"{sorted(CONFORMED_DIMENSIONS)}. Adding a conformed dimension "
                    f"is a governed change -- it cannot be declared by a join "
                    f"request."
                ),
            )
        return JoinValidationResult(
            allowed=True,
            reason=f"Legitimate: join via the conformed dimension '{request.basis}'.",
        )

    if request.mechanism is JoinMechanism.SHARED_AUTHORITATIVE_IDENTIFIER:
        if not request.basis.strip():
            return JoinValidationResult(
                allowed=False,
                reason="No identifier/standard named -- cannot validate an unnamed 'shared authoritative identifier' claim.",
            )
        return JoinValidationResult(
            allowed=True,
            reason=(
                f"Structurally permitted via a shared, externally-governed standard "
                f"identifier ('{request.basis}') -- still requires later "
                f"evidence that both ecosystems' sources actually carry this "
                f"identifier from the same authoritative register."
            ),
        )

    if request.mechanism is JoinMechanism.REAL_INFRASTRUCTURE_RELATIONSHIP:
        if not request.basis.strip():
            return JoinValidationResult(
                allowed=False,
                reason="No infrastructure relationship described -- cannot validate an unnamed claim.",
            )
        return JoinValidationResult(
            allowed=True,
            reason=(
                f"Structurally permitted via a real infrastructure relationship "
                f"('{request.basis}') -- still requires later evidence that the "
                f"source data explicitly encodes this relationship."
            ),
        )

    return JoinValidationResult(allowed=False, reason="Unrecognised join mechanism.")
