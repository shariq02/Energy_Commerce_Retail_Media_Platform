"""Unit tests for the evaluation libraries under `databricks/models`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. Resampling, the check helpers, the flag
rules, the findings text and the per-paradigm scoring are checked on small
constructed examples; no Spark session and no stored model is involved.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_eval_libs

pytestmark = [pytest.mark.unit]

_MODELS = Path(__file__).resolve().parents[2] / "databricks" / "models"


@pytest.fixture(scope="module")
def lib():
    return load_eval_libs()


def _ctx(task_id="price_daily.price", higher=False, primary="mae"):
    return SimpleNamespace(
        task_id=task_id,
        higher_is_better=higher,
        primary_metric=primary,
        partition="test",
        thresholds={
            "bootstrap_resamples": 50,
            "bootstrap_seed": 7,
            "min_segment_rows": 2,
            "injection_seed": 8,
            "reconstruction_event_seed": 9,
        },
    )


def _evaluator(lib, name, ctx, spec=None, blocks=None):
    cfg = {"spec": spec or {}, "blocks": blocks or {"by": "row"}}
    return lib[name](ctx, cfg)


class _Echo:
    """A stored model whose prediction is one column of the frame."""

    def __init__(self, column):
        self.column = column

    def predict(self, pdf):
        return pdf[self.column].to_numpy(dtype="float64")


# --- resampling -------------------------------------------------------------


def test_block_sampler_draws_whole_blocks(lib):
    labels = np.array(["a", "a", "b", "c", "c", "c"], dtype=object)
    sampler = lib["BlockSampler"](labels, 1000, 1)
    rng = np.random.default_rng(0)
    sizes = {"a": 2, "b": 1, "c": 3}
    for _ in range(20):
        drawn = labels[sampler.draw(rng)]
        for block, size in sizes.items():
            assert (drawn == block).sum() % size == 0


def test_block_sampler_uses_a_subset_of_blocks_above_the_row_cap(lib):
    labels = np.arange(1000).astype(str).astype(object)
    sampler = lib["BlockSampler"](labels, 100, 1)
    assert 2 <= sampler.n_blocks < 1000
    assert sampler.rows_used == sampler.n_blocks
    assert sampler.rows_total == 1000


def test_bootstrap_interval_brackets_the_statistic(lib):
    low, high, n = lib["bootstrap_interval"](
        lambda rng: 0.5 + rng.normal(0, 0.01), 200, 3
    )
    assert low < 0.5 < high
    assert n == 200


def test_bootstrap_interval_is_not_estimable_without_valid_resamples(lib):
    low, high, n = lib["bootstrap_interval"](lambda rng: float("nan"), 50, 3)
    assert math.isnan(low) and math.isnan(high)
    assert n == 0


def test_skill_is_oriented_so_that_positive_means_better(lib):
    assert lib["orient_skill"](8.0, 10.0, False) == pytest.approx(0.2)
    assert lib["orient_skill"](0.8, 0.6, True) == pytest.approx(0.2)
    assert math.isnan(lib["orient_skill"](None, 1.0, False))
    assert math.isnan(lib["orient_skill"](1.0, 0.0, False))


def test_degradation_is_positive_when_the_heldout_score_is_worse(lib):
    assert lib["degradation"](12.0, 10.0, False) == pytest.approx(0.2)
    assert lib["degradation"](0.6, 0.8, True) == pytest.approx(0.25)
    assert lib["degradation"](8.0, 10.0, False) == pytest.approx(-0.2)
    assert math.isnan(lib["degradation"](1.0, 0.0, False))
    assert math.isnan(lib["degradation"](None, 1.0, False))


def test_interval_favours_a_model_with_smaller_errors(lib):
    ev = _evaluator(lib, "Evaluator", _ctx())
    ev.frames["eval"] = pd.DataFrame({"x": range(200)})
    err_model = np.random.default_rng(1).uniform(0, 1, 200)
    err_base = err_model + 1.0
    model = lib["Scored"]({}, 200, lambda idx: float(err_model[idx].mean()))
    base = lib["Scored"]({}, 200, lambda idx: float(err_base[idx].mean()))
    low, high, _n, blocks, rows = ev.interval(model, base)
    assert 0 < low < high < 1
    assert blocks == 200 and rows == 200


# --- prediction sanity and breakdowns ----------------------------------------


def test_prediction_sanity_counts_missing_and_out_of_range_values(lib):
    out = lib["prediction_sanity"]([1.0, 2.0, np.nan, 100.0], [0.0, 10.0])
    assert out["pred_finite_share"] == pytest.approx(0.75)
    assert out["pred_outside_range_share"] == pytest.approx(1 / 3)
    assert lib["prediction_sanity"](None, None) == {}


def test_breakdown_drops_levels_with_too_few_rows(lib):
    frame = pd.DataFrame(
        {"g": ["a", "a", "b"], "d": ["2025-01-03", "2025-01-20", "2025-02-01"]}
    )
    out = lib["breakdown"](frame, lambda idx: float(len(idx)), "mae", ["g"], "d", 2)
    assert out == {"mae__g__a": 2.0, "mae__month__2025-01": 2.0}


def test_block_labels_use_the_month_the_group_or_the_row(lib):
    frame = pd.DataFrame({"d": ["2025-01-03", "2025-02-01"], "g": ["u", "v"]})
    assert list(lib["block_labels"](frame, {"by": "month", "col": "d"})) == [
        "2025-01",
        "2025-02",
    ]
    assert list(lib["block_labels"](frame, {"by": "group", "col": "g"})) == ["u", "v"]
    assert len(set(lib["block_labels"](frame, {"by": "row"}))) == 2


# --- tabular scoring ----------------------------------------------------------


def test_tabular_regression_scores_known_answers(lib):
    ev = _evaluator(lib, "TabularEvaluator", _ctx(), {"target": "y", "price": True})
    ev.frames["eval"] = pd.DataFrame(
        {"y": [1.0, 2.0, 3.0, 4.0], "p": [1.0, 2.0, 3.0, 5.0]}
    )
    ev.base_name = "baseline"
    bundles = {
        "baseline": lib["BaselineModel"]("constant", value=2.5),
        "model": _Echo("p"),
    }
    base = ev.score(bundles["baseline"], "eval", bundles)
    model = ev.score(bundles["model"], "eval", bundles)
    assert base.metrics["mae"] == pytest.approx(1.0)
    assert "skill_mae" not in base.metrics
    assert model.metrics["mae"] == pytest.approx(0.25)
    assert model.metrics["skill_mae"] == pytest.approx(0.75)
    assert "pinball_q50" in model.metrics
    assert model.row_stat(np.array([0, 3])) == pytest.approx(0.5)


def test_tabular_classification_scores_known_answers(lib):
    ev = _evaluator(
        lib,
        "TabularEvaluator",
        _ctx("lapse_ga4.return", True, "pr_auc"),
        {"target": "y"},
    )
    ev.frames["eval"] = pd.DataFrame(
        {"y": [0.0, 0.0, 1.0, 1.0], "p": [0.1, 0.2, 0.8, 0.9]}
    )
    ev.base_name = "baseline"
    bundles = {
        "baseline": lib["BaselineModel"]("constant", value=0.5),
        "model": _Echo("p"),
    }
    base = ev.score(bundles["baseline"], "eval", bundles)
    model = ev.score(bundles["model"], "eval", bundles)
    assert model.metrics["pr_auc"] == pytest.approx(1.0)
    assert base.metrics["pr_auc"] == pytest.approx(0.5)
    assert model.row_stat(np.arange(4)) == pytest.approx(1.0)


# --- survival, ranking, matching ------------------------------------------------


def test_survival_scores_concordance_brier_and_calibration(lib):
    frame = pd.DataFrame(
        {
            "d": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "e": [1, 1, 0, 1, 0, 1],
            "g": ["a", "a", "a", "b", "b", "b"],
        }
    )
    km = lib["KaplanMeierBaseline"]("g").fit(frame, "d", "e")
    spec = {"duration_col": "d", "event_col": "e", "group_col": "g"}
    ev = _evaluator(lib, "SurvivalEvaluator", _ctx("survival.unit_lifetime"), spec)
    ev.frames["eval"] = frame
    sc = ev.score(km, "eval", {})
    assert {"concordance", "brier_1y", "calibration_gap_5y"} <= set(sc.metrics)
    assert sc.row_stat(np.arange(6)) == pytest.approx(sc.metrics["concordance"])


def test_ranking_scores_the_rank_of_the_true_item(lib):
    pop = lib["PopularityRanker"]({"a": 1, "b": 2, "c": 3}, "y")
    ev = _evaluator(lib, "RankingEvaluator", _ctx("next_item_ga4.next_item"))
    ev.frames["eval"] = pd.DataFrame({"y": ["a", "b", "c", "a"]})
    sc = ev.score(pop, "eval", {})
    assert sc.metrics["recall_at_5"] == pytest.approx(1.0)
    gain = (1.0 + 1 / math.log2(3) + 0.5 + 1.0) / 4
    assert sc.metrics["ndcg_at_10"] == pytest.approx(gain)
    assert sc.row_stat(np.arange(4)) == pytest.approx(gain)


def test_matching_marks_thin_tiers_not_estimable(lib):
    ev = _evaluator(
        lib, "MatchingEvaluator", _ctx("redispatch_matching.tier"), {"target": "t"}
    )
    ev.frames["eval"] = pd.DataFrame({"t": ["a"] * 12 + ["b"] * 2})
    major = lib["MajorityTier"]("a")
    ev.attach({"majority": major}, "majority")
    sc = ev.score(major, "eval", {"majority": major})
    assert sc.metrics["balanced_accuracy"] == pytest.approx(0.5)
    assert sc.metrics["estimable__a"] == 1.0 and sc.metrics["estimable__b"] == 0.0
    assert sc.row_stat(np.arange(14)) == pytest.approx(0.5)


# --- weak supervision, anomaly, reconstruction ---------------------------------


def test_weak_supervision_scores_coverage_and_conflict_on_the_heldout_entities(lib):
    rows = [
        ("series", "s1", "f1", "x", "train"),
        ("series", "s1", "f2", "x", "train"),
        ("series", "s2", "f1", "x", "test"),
        ("series", "s2", "f2", "y", "test"),
        ("series", "s3", "f1", "y", "test"),
    ]
    pdf = pd.DataFrame(
        rows, columns=["entity_type", "entity_id", "lf_name", "lf_label", "partition"]
    )
    pdf["group_key"] = pdf["entity_id"]
    ev = _evaluator(lib, "WeakEvaluator", _ctx("weak_supervision.labels"))
    ev.mats = lib["label_data"](pdf)
    sc = ev.score({"rule": "majority"}, "eval", {})
    assert sc.metrics["coverage"] == pytest.approx(1.0)
    assert sc.metrics["resolved_rate__series"] == pytest.approx(0.5)
    assert sc.metrics["conflict_rate__series"] == pytest.approx(1.0)
    assert list(ev.entity_groups) == ["s2", "s3"]
    assert sc.row_stat(np.array([0, 1])) == pytest.approx(1.0)


def test_anomaly_scores_flags_at_the_recorded_threshold(lib):
    frame = pd.DataFrame({"g": ["a"] * 6, "y": [0.1, 0.2, 0.1, 0.0, 0.2, 0.1]})
    injected_frame = frame.copy()
    injected_frame.loc[2, "y"] = 9.0
    ev = _evaluator(lib, "AnomalyEvaluator", _ctx("honda_anomaly.anomaly"))
    ev.frames["eval"] = frame
    ev.injected_frame = injected_frame
    ev.injected = np.array([False, False, True, False, False, False])
    ev.recorded = {"z": {"threshold": 3.0}}
    table = pd.DataFrame({"g": ["a"], "mean": [0.1], "sd": [0.1]})
    z = lib["ZScoreModel"](["g"], table, 0.1, 0.1, "y")
    sc = ev.score(z, "eval", {"z": z})
    assert sc.metrics["precision"] == pytest.approx(1.0)
    assert sc.metrics["recall"] == pytest.approx(1.0)
    assert sc.metrics["flag_rate"] == pytest.approx(0.0)
    assert sc.metrics["pr_auc"] == pytest.approx(1.0)


def test_reconstruction_error_keeps_missing_values_and_wraps_angles(lib):
    err = lib["abs_error_full"]("wind_direction", ["wind_direction"], [350.0], [10.0])
    assert err[0] == pytest.approx(20.0)
    plain = lib["abs_error_full"]("x", [], [350.0, np.nan], [10.0, 1.0])
    assert plain[0] == pytest.approx(340.0) and math.isnan(plain[1])


def test_reconstruction_interval_resamples_whole_series(lib):
    ev = _evaluator(lib, "ReconstructionEvaluator", _ctx(), {"sets": []})
    sc = lib["Scored"]({}, 4)
    sc.parts = {("w", 0): (np.ones(4), 2 * np.ones(4), np.array([0, 0, 1, 1]))}
    assert ev.can_interval(sc)
    low, high, _n, blocks, rows = ev.interval(sc, sc)
    assert low == pytest.approx(0.5) and high == pytest.approx(0.5)
    assert blocks == 2 and rows == 4


# --- offline policies -------------------------------------------------------------


def test_pumped_policy_reward_uses_clipped_actions_and_the_logged_size(lib):
    spec = {
        "state_cols": [],
        "action_col": "a",
        "reward_col": "r",
        "price_col": "p",
        "key_cols": ["ep", "step"],
        "action_bounds": (-1.0, 1.0),
    }
    ev = _evaluator(lib, "PumpedEvaluator", _ctx("rl_pumped_storage.policy"), spec)
    ev.frames["eval"] = pd.DataFrame(
        {
            "ep": [1, 1, 2, 2],
            "a": [1.0, -1.0, 1.0, -1.0],
            "p": [10.0, 2.0, 10.0, 2.0],
            "r": [10.0, -2.0, 10.0, -2.0],
        }
    )

    class Policy:
        def action(self, pdf):
            return np.array([5.0, -5.0, 5.0, -5.0])

    sc = ev.score(Policy(), "eval", {})
    assert sc.metrics["reward_timing"] == pytest.approx(4.0)
    assert sc.metrics["action_mae"] == pytest.approx(0.0)
    assert sc.row_stat(np.array([0, 1])) == pytest.approx(4.0)


def test_action_policy_scores_agreement_with_the_logged_action(lib):
    spec = {"action_col": "a", "reward_col": "r", "key_cols": ["ep", "step"]}
    ev = _evaluator(lib, "ActionEvaluator", _ctx("rl_redispatch.policy"), spec)
    ev.frames["eval"] = pd.DataFrame(
        {"a": ["up", "down", "up", "up"], "r": [1.0, 2.0, 3.0, 4.0]}
    )

    class Policy:
        def action(self, pdf):
            return np.array(["up", "up", "up", "down"])

    sc = ev.score(Policy(), "eval", {})
    assert sc.metrics["action_agreement"] == pytest.approx(0.5)
    assert sc.row_stat(np.array([0, 2])) == pytest.approx(1.0)


# --- flags and findings -----------------------------------------------------------

_THRESHOLDS = {
    "degradation_flag_relative": 0.2,
    "min_bootstrap_blocks": 5,
    "prediction_range_share_flag": 0.1,
    "reproduction_tolerance_relative": 1e-6,
}


def _result(model, stage, metric, value, status="ok", detail=None):
    return {
        "task_id": "t",
        "model_name": model,
        "stage": stage,
        "status": status,
        "detail": detail,
        "metric": metric,
        "value": value,
    }


def test_flags_cover_the_declared_rules(lib):
    metrics = {
        "primary_change_relative": 0.3,
        "skill_primary": 0.1,
        "skill_primary_ci_low": -0.05,
        "skill_primary_ci_high": 0.2,
        "bootstrap_blocks": 3.0,
        "pred_finite_share": 0.9,
        "pred_std": 0.0,
        "pred_outside_range_share": 0.5,
        "reproduction_gap_relative": 1e-3,
    }
    rows = [_result("m", "candidate", k, v) for k, v in metrics.items()]
    names = {f[3] for f in lib["evaluation_flag_rows"](rows, _THRESHOLDS)}
    assert names == {
        "validation-to-test degradation above the threshold",
        "skill interval includes zero",
        "few resampling blocks",
        "non-finite predictions",
        "constant predictions",
        "predictions outside the target range",
        "stored model does not reproduce its validation result",
    }


def test_clean_results_and_baselines_raise_no_skill_flags(lib):
    clean = {
        "primary_change_relative": 0.05,
        "skill_primary": 0.2,
        "skill_primary_ci_low": 0.1,
        "skill_primary_ci_high": 0.3,
        "bootstrap_blocks": 12.0,
        "pred_finite_share": 1.0,
        "reproduction_gap_relative": 0.0,
    }
    rows = [_result("m", "candidate", k, v) for k, v in clean.items()]
    rows += [_result("b", "baseline", "primary_change_relative", 0.0)]
    rows += [_result("b", "baseline", "pred_std", 0.0)]
    assert lib["evaluation_flag_rows"](rows, _THRESHOLDS) == []


def test_a_model_that_failed_to_evaluate_is_flagged(lib):
    rows = [_result("m", "candidate", None, None, "failed", "KeyError: x")]
    flags = lib["evaluation_flag_rows"](rows, _THRESHOLDS)
    assert [f[3] for f in flags] == ["model not evaluated"]


def test_flag_dicts_name_the_fields(lib):
    rows = [("t", "m", "candidate", "flag", "detail", 1.0, 0.5)]
    assert lib["flag_dicts"](rows) == [
        {
            "task_id": "t",
            "model_name": "m",
            "stage": "candidate",
            "flag": "flag",
            "detail": "detail",
            "value": 1.0,
            "threshold": 0.5,
        }
    ]


def test_findings_text_has_its_sections_and_scrubs_private_values(lib):
    task = lib["TASKS"][0]
    base = {
        "task_id": task["task_id"],
        "model_name": "m",
        "family": "tabular",
        "stage": "candidate",
        "partition": "test",
        "frozen_delta_version": 3,
        "smoke": False,
        "n_rows": 10,
        "detail": None,
        "status": "ok",
    }
    primary = {"metric": task["primary_metric"], "value": 1.0, "validation_value": 0.9}
    failed = {
        "model_name": "m2",
        "status": "failed",
        "detail": "failed for me@example.com",
        "metric": None,
        "value": None,
        "validation_value": None,
    }
    results = [
        {**base, **primary},
        {**base, "metric": "skill_primary", "value": 0.2, "validation_value": None},
        {**base, **failed},
    ]
    context = [
        {
            "task_id": task["task_id"],
            "run_id": "r1",
            "status": "data_ready",
            "smoke": False,
            "notebook_path": "/Workspace/Users/me@example.com/x/databricks/models/n",
            "recorded_at": "2026-10-02 10:00:00",
            "data_notes": '[{"read": {"partition": "test", "rows": 10}}]',
        }
    ]
    spec = [{"spec_key": "k", "spec_value": "v", "recorded_at": "2026-10-02"}]
    text = lib["render_evaluation_findings"](
        task["ecosystem"], results, [], context, [], spec, smoke=False, stamp="now"
    )
    for heading in ("## settings", "## summary", "## flags", f"## {task['task_id']}"):
        assert heading in text
    assert "_no flags raised_" in text
    assert "rows 10" in text
    assert "me@example.com" not in text and "/Users/me" not in text


def _integrity(lib, monkeypatch, held, earlier, cfg, smoke=False):
    calls = []

    def fake_check(component, source, name, ok, **kwargs):
        calls.append((name, ok))

    monkeypatch.setitem(lib, "check", fake_check)
    ctx = SimpleNamespace(smoke=smoke, component="c", rid="r")
    ev = lib["Evaluator"](ctx, {"spec": {}, **cfg})
    ev.frames["eval"], ev.frames["validation"] = held, earlier
    lib["check_partition_integrity"](ctx, ev)
    return calls


def test_integrity_requires_the_evaluated_period_to_follow_validation(lib, monkeypatch):
    cfg = {"blocks": {"by": "row"}, "time_col": "d"}
    held = pd.DataFrame({"d": ["2025-03-01", "2025-03-05"]})
    early = pd.DataFrame({"d": ["2024-12-30", "2025-01-02"]})
    forward = _integrity(lib, monkeypatch, held, early, cfg)
    backward = _integrity(lib, monkeypatch, early, held, cfg)
    assert forward == [("evaluated_partition_after_validation", True)]
    assert backward == [("evaluated_partition_after_validation", False)]


def test_integrity_requires_disjoint_groups_and_skips_smoke_runs(lib, monkeypatch):
    cfg = {"blocks": {"by": "group", "col": "group_key"}}
    held = pd.DataFrame({"group_key": ["u1", "u2"]})
    early = pd.DataFrame({"group_key": ["u2", "u3"]})
    found = _integrity(lib, monkeypatch, held, early, cfg)
    assert found == [("evaluated_partition_shares_no_group", False)]
    assert _integrity(lib, monkeypatch, held, early, cfg, smoke=True) == []


# --- the task specifications ------------------------------------------------------


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, list | tuple):
        for v in value:
            yield from _strings(v)


def test_every_task_has_a_specification_and_an_evaluator(lib):
    assert set(lib["EVAL_TASKS"]) == {t["task_id"] for t in lib["TASKS"]}
    for cfg in lib["EVAL_TASKS"].values():
        assert lib["EVALUATOR_NAMES"][cfg["kind"]] in lib
        assert cfg["blocks"]["by"] in ("month", "group", "row")


def test_specifications_use_only_values_the_candidate_notebooks_use(lib):
    for task in lib["TASKS"]:
        text = (_MODELS / task["notebook"]).read_text(encoding="utf-8")
        spec = lib["EVAL_TASKS"][task["task_id"]]["spec"]
        missing = [s for s in _strings(spec) if f'"{s}"' not in text]
        assert not missing, (task["task_id"], missing)


def test_row_fraction_is_read_from_the_recorded_parameters(lib):
    models = [{"params": "{'row_fraction': 0.7}"}, {"params": None}]
    assert lib["row_fraction_of"](models) == pytest.approx(0.7)
    assert lib["row_fraction_of"]([{"params": "{}"}]) == pytest.approx(1.0)


def test_recorded_settings_are_numbers_where_they_are_used_as_numbers(lib):
    d = lib["EVAL_SPEC_DEFAULTS"]
    assert int(d["bootstrap_resamples"]) == 200
    assert float(d["degradation_flag_relative"]) == pytest.approx(0.2)
    assert int(d["min_bootstrap_blocks"]) >= 2
    assert re.fullmatch(r"\d+", d["bootstrap_seed"])
