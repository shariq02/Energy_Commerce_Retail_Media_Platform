"""Unit tests for the pure functions of the ML EDA in `databricks/eda/_eda_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

No Spark API is called: the helpers that read a frame are out of scope here.
"""

from __future__ import annotations

import datetime as dt

import pytest

from tests._notebook_loader import load_eda_common

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_eda_common()


def test_batches_cut_a_list_into_lists_of_at_most_the_size(lib):
    assert lib["batches"](range(5), 2) == [[0, 1], [2, 3], [4]]
    assert lib["batches"]([], 2) == []


def test_roles_group_the_columns_of_a_contract_in_order(lib):
    rows = [("a", "feature"), ("t", "target"), ("b", "feature")]
    assert lib["ml_roles"](rows) == {"feature": ["a", "b"], "target": ["t"]}


@pytest.mark.parametrize(
    ("dtype", "low", "high", "distinct", "kind"),
    [
        ("boolean", None, None, None, "binary"),
        ("string", None, None, None, "text"),
        ("int", 0, 1, 2, "binary"),
        ("double", 0.0, 1.0, 2, "binary"),
        ("double", 0.0, 1.0, 500, "numeric"),
        ("bigint", -3, 9, 2, "numeric"),
        ("double", None, None, 0, "numeric"),
        ("array<string>", None, None, None, "other"),
    ],
)
def test_the_target_kind_follows_the_type_and_the_values(
    lib, dtype, low, high, distinct, kind
):
    assert lib["ml_target_kind"](dtype, low, high, distinct) == kind


def _days(*offsets):
    start = dt.date(2024, 1, 1)
    return [start + dt.timedelta(days=o) for o in offsets]


def test_gaps_count_the_steps_missing_and_the_longest_gap(lib):
    found = lib["date_gaps"]({"train": _days(0, 1, 2, 5, 6)})["train"]
    assert found == {"dates": 5, "step": 1, "missing": 2, "longest": 3}


def test_a_complete_axis_has_no_gap(lib):
    found = lib["date_gaps"]({"test": _days(0, 1, 2, 3)})["test"]
    assert found["missing"] == 0
    assert found["longest"] == 1


def test_one_date_has_no_step(lib):
    found = lib["date_gaps"]({"test": _days(0)})["test"]
    assert found == {"dates": 1, "step": None, "missing": 0, "longest": 0}


def test_a_yearly_axis_uses_its_own_step(lib):
    years = [dt.date(y, 1, 1) for y in (2019, 2020, 2021, 2023)]
    found = lib["date_gaps"]({"train": years})["train"]
    assert found["missing"] == 1


def _result(**parts):
    base = {"target": "t", "kind": "binary", "features": 2, "numbers": 2}
    base.update({"dominant": {}, "pairs": [], "checked": 2})
    base.update(parts)
    return base


def test_blocks_hold_the_tables_of_the_results(lib):
    result = _result(
        partitions={
            "train": {"rows": 10, "labelled": 9, "mean": 0.5, "std": 0.1, "zero": 0.5}
        },
        shift={"a": {"test": (1.5, 0.2)}, "b": {}},
        association=[("a", 0.95)],
        above_limit=1,
        gaps={"train": {"dates": 3, "step": 1, "missing": 1, "longest": 2}},
    )
    blocks = dict(lib["ml_eda_blocks"]({"demo": result}))
    assert "| demo | binary | train | 10 | 9 |" in blocks["Target by partition"]
    shift = blocks["Feature shift from train (ten largest per dataset)"]
    assert "`a` | test | 1.5 | 0.2" in shift
    assert "demo: 1" in blocks["ML-Readiness Evidence"]
    assert "['demo']" in blocks["ML-Readiness Evidence"]


def test_a_dataset_without_a_target_is_listed(lib):
    result = _result(target=None, kind=None)
    blocks = dict(lib["ml_eda_blocks"]({"demo": result}))
    assert blocks["Datasets without a readable target"] == "demo"
