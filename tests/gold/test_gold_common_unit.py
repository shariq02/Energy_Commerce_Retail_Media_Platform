"""Unit tests for the pure (non-Spark-API) functions in
`databricks/gold/_gold_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Loaded via `tests/_notebook_loader.py`, which reproduces the real Gold
notebooks' own `%run ../silver/_silver_common` before `%run ../../_gold_common`
-- `gold_schema_for` depends on `ecosystem_for`, defined in the Silver shared
library, exactly as it does in a live Databricks run. Functions that call a
live Spark API are out of scope -- see `test_silver_common_unit.py`'s module
docstring for why.
"""

from __future__ import annotations

import re

import pytest

from tests._notebook_loader import load_gold_common

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def gold():
    return load_gold_common()


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("smard", "energy_gold"),
        ("dwd", "energy_gold"),
        ("rees46", "commerce_gold"),
        ("ga4", "commerce_gold"),
    ],
)
def test_gold_schema_for_matches_the_governed_ecosystem_map(gold, source, expected):
    assert gold["gold_schema_for"](source) == expected


def test_gold_run_id_has_the_gold_prefix_and_is_distinct_from_silver(gold):
    rid = gold["gold_run_id"]()
    assert rid.startswith("gold-")
    assert re.match(r"^gold-\d{8}T\d{6}Z$", rid)


def test_gold_run_id_is_not_confusable_with_silver_run_id(gold):
    # gold_run_id() and the Silver run_id() layered in underneath it must
    # never collide in prefix, or a run_id alone stops telling you which
    # stage produced it (the property _gold_common.py's own docstring claims).
    assert gold["gold_run_id"]().split("-", 1)[0] != gold["run_id"]().split("-", 1)[0]
