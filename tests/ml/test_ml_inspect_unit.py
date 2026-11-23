"""Unit tests for the label-proxy check of `databricks/ml/_ml_inspect.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

The check is pure pandas and scikit-learn; no Spark API is called.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_ml_inspect

pytestmark = [pytest.mark.unit]

ROWS = 6000


@pytest.fixture(scope="module")
def check():
    return load_ml_inspect()["label_proxy_columns"]


def _frame(**columns):
    return pd.DataFrame(columns)


def test_a_column_that_is_null_exactly_for_one_class_gives_the_label_away(check):
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, ROWS)
    frequency = np.where(y == 1, np.nan, rng.uniform(0.0, 1.0, ROWS))
    noise = rng.normal(size=ROWS)
    found = check(
        _frame(y=y, frequency=frequency, noise=noise), "y", ["frequency", "noise"]
    )
    assert ("frequency", "one column") in found
    assert "noise" not in {c for c, _how in found}


def test_two_columns_that_make_the_rule_are_found_by_the_tree(check):
    rng = np.random.default_rng(1)
    a = rng.uniform(0.0, 1.0, ROWS)
    b = rng.integers(0, 2, ROWS)
    y = ((a < 0.3) & (b == 1)).astype(int)
    noise = rng.normal(size=ROWS)
    found = dict(check(_frame(y=y, a=a, b=b, noise=noise), "y", ["a", "b", "noise"]))
    assert {"a", "b"} <= set(found)
    assert "noise" not in found


def test_columns_that_do_not_know_the_label_are_not_flagged(check):
    rng = np.random.default_rng(2)
    y = rng.integers(0, 2, ROWS)
    frame = _frame(y=y, x=rng.normal(size=ROWS), z=rng.integers(0, 5, ROWS))
    assert check(frame, "y", ["x", "z"]) == []


def test_a_strong_but_honest_column_is_not_flagged(check):
    rng = np.random.default_rng(3)
    x = rng.normal(size=ROWS)
    y = (x + rng.normal(scale=1.0, size=ROWS) > 0).astype(int)
    assert check(_frame(y=y, x=x), "y", ["x"]) == []


def test_a_unique_identifier_is_pure_by_size_and_is_not_flagged(check):
    rng = np.random.default_rng(4)
    y = rng.integers(0, 2, ROWS)
    frame = _frame(y=y, record_id=[f"id{i}" for i in range(ROWS)])
    assert check(frame, "y", ["record_id"]) == []


def test_a_label_that_is_not_binary_or_has_one_class_returns_nothing(check):
    rng = np.random.default_rng(5)
    x = rng.normal(size=ROWS)
    assert check(_frame(y=x, x=x), "y", ["x"]) == []
    assert check(_frame(y=np.zeros(ROWS, dtype=int), x=x), "y", ["x"]) == []


def test_a_boolean_label_and_a_text_column_work(check):
    rng = np.random.default_rng(6)
    y = rng.integers(0, 2, ROWS).astype(bool)
    origin = np.where(y, "germline", "somatic")
    found = check(_frame(y=y, origin=origin), "y", ["origin"])
    assert ("origin", "one column") in found
