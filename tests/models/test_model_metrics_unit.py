"""Known-answer tests for `databricks/models/lib/_model_metrics.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Numpy-only library, loaded through `tests/_notebook_loader.py`; every case is a
small example with a value worked out by hand.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from tests._notebook_loader import load_model_metrics

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def m():
    return load_model_metrics()


def test_regression_errors_and_skill(m):
    assert m["mae"]([1, 2, 3], [1, 2, 5]) == pytest.approx(2 / 3)
    assert m["rmse"]([1, 2, 3], [1, 2, 5]) == pytest.approx(math.sqrt(4 / 3))
    assert m["skill"](1.0, 2.0) == pytest.approx(0.5)
    assert math.isnan(m["skill"](1.0, 0.0))


def test_nan_pairs_are_dropped(m):
    assert m["mae"]([1, np.nan, 3], [2, 5, 3]) == pytest.approx(0.5)


def test_pinball_penalises_the_right_side(m):
    assert m["pinball"]([10], [8], 0.9) == pytest.approx(1.8)
    assert m["pinball"]([8], [10], 0.9) == pytest.approx(0.2)


def test_quantile_metrics_report_each_level_and_coverage(m):
    y = np.arange(100, dtype=float)
    out = m["quantile_metrics"](y, {0.1: y - 5, 0.5: y, 0.9: y + 5})
    assert out["pinball_q50"] == pytest.approx(0.0)
    assert out["interval_coverage_80"] == pytest.approx(1.0)


def test_average_precision(m):
    assert m["average_precision"]([1, 1, 0, 0], [0.9, 0.8, 0.3, 0.1]) == pytest.approx(
        1.0
    )
    assert m["average_precision"]([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(
        (1 + 2 / 3) / 2
    )


def test_constant_scores_give_the_base_rate(m):
    assert m["average_precision"]([1, 0, 0, 0], [0.5] * 4) == pytest.approx(0.25)


def test_log_loss_and_calibration(m):
    assert m["log_loss"]([1, 0], [0.9, 0.1]) == pytest.approx(-math.log(0.9))
    assert m["calibration_error"]([1, 1, 0, 0], [1, 1, 0, 0]) == pytest.approx(0.0)
    assert m["calibration_error"]([1, 1, 1, 1], [0.0] * 4) == pytest.approx(1.0)


def test_classification_metrics_bundle(m):
    out = m["classification_metrics"]([1, 0, 1, 0], [0.9, 0.2, 0.8, 0.1])
    assert out["pr_auc"] == pytest.approx(1.0)
    assert out["base_rate"] == pytest.approx(0.5)


def test_concordance_index(m):
    d, e = [1, 2, 3, 4], [1, 1, 1, 1]
    assert m["concordance_index"](d, e, [4, 3, 2, 1]) == pytest.approx(1.0)
    assert m["concordance_index"](d, e, [1, 2, 3, 4]) == pytest.approx(0.0)
    assert m["concordance_index"](d, e, [1, 1, 1, 1]) == pytest.approx(0.5)


def test_kaplan_meier_curve_and_lookup(m):
    times, surv = m["kaplan_meier"]([1, 2, 3], [1, 1, 1])
    assert list(times) == [1, 2, 3]
    assert list(surv) == pytest.approx([2 / 3, 1 / 3, 0.0])
    at = m["km_at"](times, surv, [0.5, 1, 2.5])
    assert list(at) == pytest.approx([1.0, 2 / 3, 1 / 3])


def test_kaplan_meier_handles_censoring(m):
    _, surv = m["kaplan_meier"]([1, 2, 3], [1, 0, 1])
    assert list(surv) == pytest.approx([2 / 3, 0.0])


def test_brier_and_calibration_gap(m):
    assert m["brier_at"]([1, 5], [1, 1], [0.0, 1.0], 3) == pytest.approx(0.0)
    assert m["brier_at"]([1, 5], [1, 1], [0.5, 0.5], 3) == pytest.approx(0.25)
    gap = m["survival_calibration_gap"]([1, 2, 3], [1, 1, 1], [1 / 3] * 3, 2)
    assert gap == pytest.approx(0.0)


def test_ranking_metrics(m):
    out = m["ranking_metrics"]([1, 2, math.inf], ks=(1, 2))
    assert out["recall_at_1"] == pytest.approx(1 / 3)
    assert out["recall_at_2"] == pytest.approx(2 / 3)
    assert out["ndcg_at_2"] == pytest.approx((1 + 1 / math.log2(3)) / 3)
    assert out["mrr"] == pytest.approx(0.5)


def test_masked_mae_uses_only_masked_positions(m):
    assert m["masked_mae"]([1, 2, 3], [2, 2, 5], [True, False, True]) == pytest.approx(
        1.5
    )


def test_detection_metrics(m):
    out = m["detection_metrics"]([1, 1, 0, 0], [1, 0, 1, 0], 0.02)
    assert out["precision"] == pytest.approx(0.5)
    assert out["recall"] == pytest.approx(0.5)
    assert out["flag_rate"] == pytest.approx(0.02)


def test_agreement_metrics(m):
    out = m["agreement_metrics"](["a", "b", "a"], ["a", "a", "a"], [1, 2, 3])
    assert out["action_agreement"] == pytest.approx(2 / 3)
    assert out["reward_when_agree"] == pytest.approx(2.0)
    assert out["reward_when_disagree"] == pytest.approx(2.0)
    assert out["reward_weighted_agreement"] == pytest.approx(4 / 6)


def test_label_matrix_metrics(m):
    matrix = [[0, 0], [0, 1], [-1, 1], [-1, -1]]
    out = m["label_matrix_metrics"](matrix)
    assert out["coverage"] == pytest.approx(0.75)
    assert out["conflict_rate"] == pytest.approx(0.5)
    assert out["agreement_rate"] == pytest.approx(0.5)


def test_per_class_precision_recall_marks_thin_classes(m):
    y_true = ["a"] * 12 + ["b"] * 2
    y_pred = ["a"] * 12 + ["a", "b"]
    out = m["per_class_precision_recall"](y_true, y_pred, ["a", "b"], min_rows=10)
    p, r, n, ok = out["a"]
    assert (p, r, n, ok) == (pytest.approx(12 / 13), pytest.approx(1.0), 12, True)
    assert out["b"][3] is False
    assert math.isnan(out["b"][0])


def test_bias_metrics_report_signed_error_and_spread(m):
    out = m["bias_metrics"]([1, 2, 3, 4], [2, 3, 4, 5])
    assert out["mean_error"] == pytest.approx(1.0)
    assert out["mean_error_relative"] == pytest.approx(0.4)
    assert out["pred_std_ratio"] == pytest.approx(1.0)
    assert m["bias_metrics"]([1, 2, 3, 4], [2.5] * 4)[
        "pred_std_ratio"
    ] == pytest.approx(0.0)
