"""Unit tests for the pure parts of the model libraries under `databricks/models`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. Baselines, injection, survival curves,
session histories, rankers, mask events, label models and reward helpers are checked
on small constructed examples; nothing here trains a model.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_model_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_model_libs()


# --- tabular baselines -------------------------------------------------------


def test_group_mean_baseline_falls_back_to_the_overall_mean(lib):
    train = pd.DataFrame({"g": ["a", "a", "b"], "y": [1.0, 3.0, 10.0]})
    base = lib["fit_baseline"](train, "y", {"kind": "group_mean", "cols": ["g"]})
    pred = base.predict(pd.DataFrame({"g": ["a", "b", "c"]}))
    assert list(pred[:2]) == pytest.approx([2.0, 10.0])
    assert pred[2] == pytest.approx(14 / 3)


def test_column_baseline_fills_missing_inputs_with_the_train_median(lib):
    train = pd.DataFrame({"lag": [1.0, 2.0, 3.0], "y": [5.0, 7.0, 9.0]})
    base = lib["fit_baseline"](train, "y", {"kind": "column", "column": "lag"})
    pred = base.predict(pd.DataFrame({"lag": [4.0, np.nan]}))
    assert list(pred) == pytest.approx([4.0, 7.0])


def test_ratio_baseline_scales_with_the_denominator(lib):
    train = pd.DataFrame({"g": ["a", "a"], "cap": [10.0, 20.0], "y": [5.0, 20.0]})
    base = lib["fit_baseline"](
        train, "y", {"kind": "group_ratio", "cols": ["g"], "denominator": "cap"}
    )
    assert base.predict(pd.DataFrame({"g": ["a"], "cap": [40.0]}))[0] == pytest.approx(
        30.0
    )


def test_ratio_baseline_prefers_the_imputed_denominator(lib):
    train = pd.DataFrame(
        {"g": ["a"], "cap": [np.nan], "cap_imputed": [10.0], "y": [5.0]}
    )
    base = lib["fit_baseline"](
        train, "y", {"kind": "group_ratio", "cols": ["g"], "denominator": "cap"}
    )
    frame = pd.DataFrame({"g": ["a"], "cap": [np.nan], "cap_imputed": [20.0]})
    assert base.predict(frame)[0] == pytest.approx(10.0)


def test_constant_baselines(lib):
    train = pd.DataFrame({"y": [1.0, 3.0]})
    assert (
        lib["fit_baseline"](train, "y", {"kind": "train_mean"}).predict(train)[0] == 2.0
    )
    assert lib["fit_baseline"](train, "y", {"kind": "zero"}).predict(train)[0] == 0.0
    assert (
        lib["fit_baseline"](train, "y", {"kind": "base_rate"}).predict(train)[0] == 2.0
    )


def test_quantile_baseline_orders_the_levels(lib):
    train = pd.DataFrame({"y": np.arange(1.0, 101.0)})
    base = lib["fit_quantile_baseline"](train, "y", (0.1, 0.5, 0.9), [])
    pred = base.predict(train.head(3))
    assert pred[0.1][0] < pred[0.5][0] < pred[0.9][0]


# --- anomaly injection -------------------------------------------------------


def test_injection_changes_only_the_injected_windows(lib):
    values = np.zeros(200)
    groups = np.zeros(200, dtype=int)
    out, injected = lib["inject_anomalies"](
        values, groups, np.array([1.0]), [3], [1.0], ["spike"], [3.0, 3.0], 0.1, 1
    )
    assert injected.sum() > 0
    assert injected.sum() % 3 == 0
    assert set(out[injected]) == {3.0}
    assert set(out[~injected]) == {0.0}


def test_flatline_injection_holds_the_window_start_value(lib):
    values = np.arange(300, dtype=float)
    groups = np.zeros(300, dtype=int)
    out, injected = lib["inject_anomalies"](
        values, groups, np.array([1.0]), [5], [1.0], ["flatline"], [3.0, 3.0], 0.1, 2
    )
    starts = np.flatnonzero(injected & ~np.roll(injected, 1))
    assert len(starts) > 0
    for s in starts:
        assert len(set(out[s : s + 5])) == 1


def test_injection_is_repeatable(lib):
    args = (
        np.zeros(100),
        np.zeros(100, dtype=int),
        np.array([1.0]),
        [2],
        [1.0],
        ["drop"],
        [3, 5],
        0.1,
        7,
    )
    a, ia = lib["inject_anomalies"](*args)
    b, ib = lib["inject_anomalies"](*args)
    assert np.array_equal(a, b)
    assert np.array_equal(ia, ib)


# --- survival ---------------------------------------------------------------


def test_breslow_survival_matches_the_hand_computed_hazard(lib):
    times, cum = lib["breslow_baseline"]([1, 2, 3], [1, 1, 1], [1, 1, 1])
    assert list(cum) == pytest.approx([1 / 3, 1 / 3 + 1 / 2, 1 / 3 + 1 / 2 + 1])
    surv = lib["survival_at"](times, cum, np.array([1.0]), [2])
    assert surv[0, 0] == pytest.approx(math.exp(-(1 / 3 + 1 / 2)))


def test_higher_risk_lowers_survival(lib):
    times, cum = lib["breslow_baseline"]([1, 2, 3], [1, 1, 1], [1, 1, 1])
    surv = lib["survival_at"](times, cum, np.array([0.5, 2.0]), [2])
    assert surv[0, 0] > surv[1, 0]


def test_kaplan_meier_baseline_returns_valid_curves_per_group(lib):
    frame = pd.DataFrame({"g": [0, 0, 1, 1], "d": [1, 2, 1, 2], "e": [1, 1, 1, 0]})
    km = lib["KaplanMeierBaseline"]("g").fit(frame, "d", "e")
    surv = km.survival(frame)
    assert surv.shape == (4, len(lib["SURVIVAL_HORIZONS_YEARS"]))
    assert ((surv >= 0) & (surv <= 1)).all()
    assert km.risk(frame).shape == (4,)


# --- ranking ----------------------------------------------------------------


def test_session_histories_stop_at_the_session_and_pad_with_zeros(lib):
    frame = pd.DataFrame({"s": ["s1", "s1", "s1", "s2"], "item": ["a", "b", "c", "a"]})
    hist = lib["build_histories"](frame, {"a": 1, "b": 2, "c": 3}, 3, "s", "item")
    assert hist.tolist() == [[0, 0, 1], [0, 1, 2], [1, 2, 3], [0, 0, 1]]


def test_popularity_ranker_ranks_by_train_frequency(lib):
    pop = lib["fit_popularity"](pd.DataFrame({"t": ["x", "x", "y"]}), "t")
    ranks = pop.ranks(pd.DataFrame({"t": ["x", "y", "z"]}))
    assert ranks[0] == 1 and ranks[1] == 2 and math.isinf(ranks[2])


def test_cooccurrence_ranker_lists_followers_then_falls_back(lib):
    train = pd.DataFrame({"src": ["a", "a", "a", "b"], "t": ["x", "x", "y", "z"]})
    pop = lib["fit_popularity"](train, "t")
    cooc = lib["fit_cooccurrence"](train, "src", "t", pop, top=2)
    ranks = cooc.ranks(pd.DataFrame({"src": ["a", "a", "a"], "t": ["x", "y", "z"]}))
    assert ranks[0] == 1 and ranks[1] == 2
    assert ranks[2] >= 3


# --- reconstruction ---------------------------------------------------------


def test_events_stay_inside_their_series(lib):
    rng = np.random.default_rng(0)
    events = lib["sample_events"](rng, [2, 5], [1, 1], [50, 60], np.arange(2), 30)
    assert len(events) == 30
    for i, start, length in events:
        assert start >= 1
        assert start + length <= [50, 60][i]


def _series_set(lib, n=100):
    arr = np.arange(n, dtype=float).reshape(n, 1)
    cal = (np.zeros(n, dtype=int), np.zeros(n, dtype=int), np.ones(n, dtype=int))
    return lib["SeriesSet"]("s", ["x"], [arr], [cal], ["v"], 24)


def test_event_features_use_only_data_before_the_block(lib):
    sset = _series_set(lib)
    x, y = lib["event_features"](sset, 0, 0, 30, 5)
    assert x[:, 0].tolist() == [29.0] * 5
    assert x[:, 1].tolist() == [6.0, 7.0, 8.0, 9.0, 10.0]
    assert x[:, 2].tolist() == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert y.tolist() == [30.0, 31.0, 32.0, 33.0, 34.0]


def test_circular_error_wraps_around_north(lib):
    err = lib["masked_abs_error"]("wind_direction", {"wind_direction"}, [350.0], [10.0])
    assert err[0] == pytest.approx(20.0)
    plain = lib["masked_abs_error"]("wind_speed", {"wind_direction"}, [350.0], [10.0])
    assert plain[0] == pytest.approx(340.0)


def test_causal_baseline_picks_the_seasonal_value_when_asked(lib):
    x = np.array([[5.0, 7.0], [5.0, np.nan]])
    carry = lib["BaselineReconstructor"]("carry_forward", {0: 0.0}).predict(0, x)
    seasonal = lib["BaselineReconstructor"]("seasonal_carry", {0: 0.0}).predict(0, x)
    assert carry.tolist() == [5.0, 5.0]
    assert seasonal.tolist() == [7.0, 5.0]


# --- weak supervision and matching ------------------------------------------


def test_majority_vote_leaves_ties_and_empty_rows_unresolved(lib):
    matrix = np.array([[0, 0], [0, 1], [-1, 1], [-1, -1]])
    assert lib["majority_vote"](matrix).tolist() == [0, -1, 1, -1]


def test_label_model_recovers_function_accuracies(lib):
    rng = np.random.default_rng(3)
    truth = rng.integers(0, 2, 6000)
    accuracy = [0.9, 0.8, 0.7]
    cols = [np.where(rng.random(6000) < a, truth, 1 - truth) for a in accuracy]
    model = lib["fit_label_model"](np.column_stack(cols))
    assert list(model.accuracy) == pytest.approx(accuracy, abs=0.06)


def test_label_matrices_split_by_entity_type(lib):
    frame = pd.DataFrame(
        {
            "entity_type": ["series", "series", "unit"],
            "entity_id": ["p|m", "p|m", "u1"],
            "lf_name": ["a", "b", "era"],
            "lf_label": ["gap", "no_gap", "pre"],
            "partition": ["train", "train", "validation"],
        }
    )
    mats = lib["label_matrices"](frame)
    votes, lfs, _labels, part = mats["series"]
    assert votes.shape == (1, 2) and lfs == ["a", "b"] and part.tolist() == ["train"]
    assert mats["unit"][0].shape == (1, 1)


def test_tier_metrics_keep_thin_tiers_in_the_balanced_score(lib):
    y_true = np.array(["a"] * 12 + ["b"] * 2)
    y_pred = np.array(["a"] * 12 + ["a", "b"])
    out = lib["tier_metrics"](y_true, y_pred, ["a", "b"])
    assert out["estimable__b"] == 0.0
    assert out["estimable_tiers"] == 1.0
    assert out["balanced_accuracy"] == pytest.approx(0.75)


def test_majority_tier_does_not_score_perfectly_when_a_thin_tier_exists(lib):
    y_true = np.array(["a"] * 12 + ["b"] * 2)
    out = lib["tier_metrics"](y_true, np.array(["a"] * 14), ["a", "b"])
    assert out["balanced_accuracy"] == pytest.approx(0.5)


# --- policies ---------------------------------------------------------------


def test_reward_weights_are_positive_and_grow_with_reward(lib):
    w = lib["reward_weights"](np.array([-5.0, 0.0, 5.0]))
    assert (w > 0).all()
    assert w[0] < w[1] < w[2]


def test_action_bins_cover_the_range(lib):
    actions = np.linspace(-1, 1, 200)
    edges, centres = lib["action_bins"](actions)
    assert len(centres) == lib["ACTION_BINS"]
    assert list(centres) == sorted(centres)
    idx = lib["bin_index"](actions, edges)
    assert idx.min() == 0 and idx.max() == lib["ACTION_BINS"] - 1


def test_rule_policy_discharges_at_high_prices(lib):
    policy = lib["RulePolicy"]("price", 50.0, 10.0)
    out = policy.action(pd.DataFrame({"price": [40.0, 50.0, 60.0]}))
    assert out.tolist() == [-10.0, 10.0, 10.0]


def test_event_histories_exclude_the_current_event(lib):
    frame = pd.DataFrame(
        {"s": ["a", "a", "a"], "seq_event_type": ["view", "cart", "purchase"]}
    )
    hist = lib["event_histories"](frame, {"view": 1, "cart": 2, "purchase": 3}, 2, "s")
    assert hist.tolist() == [[0, 0], [0, 1], [1, 2]]


def test_pumped_metrics_clip_actions_and_score_timing_at_the_logged_size(lib):
    valid = pd.DataFrame(
        {
            "episode_id": ["d1", "d1", "d1", "d1"],
            "action_net_mwh": [10.0, -10.0, 10.0, -10.0],
            "state_price_eur_per_mwh": [100.0, 20.0, 80.0, 40.0],
            "reward_eur": [1000.0, -200.0, 800.0, -400.0],
        }
    )
    spec = {
        "action_col": "action_net_mwh",
        "price_col": "state_price_eur_per_mwh",
        "reward_col": "reward_eur",
        "key_cols": ["episode_id", "step"],
        "action_bounds": (-10.0, 10.0),
    }
    out = lib["pumped_metrics"](valid, np.array([500.0, -500.0, 500.0, -500.0]), spec)
    assert out["action_mae"] == pytest.approx(0.0)
    assert out["reward_policy"] == pytest.approx(out["reward_logged"])
    greedy = lib["pumped_metrics"](valid, np.array([1.0, 1.0, 1.0, -1.0]), spec)
    assert greedy["reward_timing"] == pytest.approx((1000 + 200 + 800 - 400) / 4)
    assert greedy["reward_timing_gain"] == pytest.approx(greedy["reward_timing"] - 300)


def test_group_diagnostic_reports_skill_over_the_group_mean_and_error_per_group(lib):
    frame = pd.DataFrame({"scope": ["a", "a", "b", "b"]})
    diag = {"name": "scope_mean", "group_col": "scope"}
    y = np.array([1.0, 1.0, 5.0, 5.0])
    out = lib["_diagnostic_metrics"](y, y + 1.0, frame, diag, y + 2.0)
    assert out["skill_mae_vs_scope_mean"] == pytest.approx(0.5)
    assert out["mae__scope__a"] == pytest.approx(1.0)
    assert "skill_mae_vs_scope_mean" not in lib["_diagnostic_metrics"](
        y, y, frame, diag, y, skill=False
    )


def test_item_diagnostics_count_targets_already_seen_in_the_session(lib):
    valid = pd.DataFrame(
        {
            "s": ["a", "a", "a"],
            "step": [0, 1, 2],
            "item": ["x", "y", "x"],
            "next": ["y", "x", "z"],
        }
    )
    train = pd.DataFrame({"item": ["x", "y"], "next": ["y", "x"]})
    out = lib["item_diagnostics"](train, valid, "s", "step", "item", "next")
    assert out["item_vocabulary_train"] == 2
    assert out["target_seen_earlier_in_session"] == pytest.approx(1 / 3)
    assert out["target_in_train_vocabulary"] == pytest.approx(2 / 3)
