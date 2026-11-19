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

import ast
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests._notebook_loader import load_gold_common

pytestmark = [pytest.mark.unit]

_SILVER_08 = (
    Path(__file__).resolve().parents[2]
    / "databricks"
    / "silver"
    / "genomics"
    / "08_derived_attributes.py"
)


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
        ("clinvar", "genomics_gold"),
    ],
)
def test_gold_schema_for_matches_the_governed_ecosystem_map(gold, source, expected):
    assert gold["gold_schema_for"](source) == expected


def _silver_rules() -> dict[str, set[str]]:
    """Entity type -> the rule names that genomics Silver `08` writes with a
    literal entity type and rule name (row helpers, `derived_rows` and keyword
    groups)."""
    positions = {"number_rows": 5, "text_rows": 5, "flag_rows": 5, "attribute_rows": 3}
    tree = ast.parse(_SILVER_08.read_text(encoding="utf-8"))
    rules: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        keywords = {k.arg: k.value for k in node.keywords}
        if node.func.id in positions and len(node.args) > positions[node.func.id]:
            entity, rule = node.args[1], node.args[positions[node.func.id]]
        elif node.func.id in (*positions, "derived_rows"):
            entity, rule = keywords.get("entity_type"), keywords.get("rule_name")
        elif node.func.id in ("keyword_rows", "captured_rows"):
            entity, rule = keywords.get("entity_type"), keywords.get("group")
        else:
            continue
        if isinstance(entity, ast.Constant) and isinstance(rule, ast.Constant):
            rules.setdefault(entity.value, set()).add(rule.value)
    return rules


def test_every_structural_rule_is_a_rule_that_silver_writes(gold):
    # A rule name that Silver does not write keeps nothing out of variant_profile.
    unknown = set(gold["STRUCTURAL_RULES"]) - _silver_rules()["variant"]
    assert not unknown, f"rules Silver 08 does not write: {sorted(unknown)}"


@pytest.mark.parametrize(
    ("dtypes", "expected"),
    [
        ([[("a", "int"), ("b", "string")], [("a", "int")]], set()),
        ([[("a", "int")], [("a", "string"), ("b", "int")]], {"a"}),
        ([], set()),
    ],
)
def test_mixed_columns_names_a_column_with_two_types(gold, dtypes, expected):
    assert gold["mixed_columns"](dtypes) == expected


def test_unfed_tables_lists_the_silver_tables_no_gold_table_uses(gold):
    feeds = {"gene_profile": ("entity_master",), "benchmark": ("variant",)}
    catalog = {"entity_master", "variant", "locus_score"}
    assert gold["unfed_tables"](feeds, catalog) == ["locus_score"]


def test_the_typed_table_width_is_the_owner_ceiling(gold):
    assert gold["MAX_TYPED_COLUMNS"] == 140


@pytest.mark.parametrize(
    ("columns", "attribute", "rule", "expected"),
    [
        (["gene_key", "is_pharmacogene"], "is_pharmacogene", None, "is_pharmacogene"),
        (
            ["pharmacogene__is_pharmacogene", "other_rule__is_pharmacogene"],
            "is_pharmacogene",
            "pharmacogene",
            "pharmacogene__is_pharmacogene",
        ),
        (
            ["review_quality__quality_tier"],
            "quality_tier",
            None,
            "review_quality__quality_tier",
        ),
    ],
)
def test_pick_column_finds_the_pivoted_attribute(
    gold, columns, attribute, rule, expected
):
    frame = SimpleNamespace(columns=columns)
    assert gold["pick_column"](frame, attribute, rule) == expected


@pytest.mark.parametrize("columns", [[], ["a__score", "b__score"]])
def test_pick_column_raises_on_no_or_two_matches(gold, columns):
    with pytest.raises(RuntimeError):
        gold["pick_column"](SimpleNamespace(columns=columns), "score")


def test_findings_keep_counts_and_drop_data_values(gold):
    header = "| check | metric |\n|---|---|"
    blocks = [
        ("numeric", f"{header}\n| min:price | min |\n| zero_count:price | count |"),
        ("dq", f"{header}\n| value_counts:overall_status | count |"),
    ]
    kept = gold["without_values"](blocks)
    assert kept == [("numeric", f"{header}\n| zero_count:price | count |")]


def test_stage_of_tells_gold_and_analytics_run_ids_apart(gold):
    assert gold["stage_of"](gold["analytics_run_id"]()) == "analytics"
    assert gold["stage_of"](gold["gold_run_id"]()) == "gold"


def test_gold_run_id_has_the_gold_prefix_and_is_distinct_from_silver(gold):
    rid = gold["gold_run_id"]()
    assert rid.startswith("gold-")
    assert re.match(r"^gold-\d{8}T\d{6}Z$", rid)


def test_gold_run_id_is_not_confusable_with_silver_run_id(gold):
    # gold_run_id() and the Silver run_id() layered in underneath it must
    # never collide in prefix, or a run_id alone stops telling you which
    # stage produced it (the property _gold_common.py's own docstring claims).
    assert gold["gold_run_id"]().split("-", 1)[0] != gold["run_id"]().split("-", 1)[0]
