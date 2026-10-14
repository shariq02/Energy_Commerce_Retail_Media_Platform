"""Unit tests for the operate libraries under `databricks/models/lib`.

The reference profile, the comparison of a window, the performance check, the
triggers and the findings text are plain Python and pandas; each test builds its
rows by hand. The window functions are tested with small stand-ins for the
evaluator and the stored model."""

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_operate_libs

pytestmark = [pytest.mark.schema, pytest.mark.unit]

NOW = dt.datetime(2026, 10, 5, 12, 0, tzinfo=dt.UTC)
SPEC = {
    "psi_warning": 0.10,
    "psi_flag": 0.25,
    "null_rate_increase_pp": 5.0,
    "reference_bins": 10,
    "max_window_rows": 1000,
    "max_categories": 3,
}


@pytest.fixture(scope="module")
def lib():
    return load_operate_libs()


def _columns(ddl: str) -> list:
    return [part.split()[0] for part in ddl.split(", ")]


def _normal(n=5000, shift=0.0, seed=1):
    return pd.Series(np.random.default_rng(seed).normal(shift, 1.0, n))


def test_psi_is_zero_for_equal_shares_and_grows_with_the_shift(lib):
    same = [0.25, 0.25, 0.25, 0.25]
    assert lib["psi"](same, same) == pytest.approx(0.0)
    assert lib["psi"](same, [0.4, 0.3, 0.2, 0.1]) > 0.1


def test_psi_does_not_fail_on_an_empty_bin(lib):
    assert np.isfinite(lib["psi"]([0.5, 0.5, 0.0], [0.4, 0.4, 0.2]))


@pytest.mark.parametrize(
    ("value", "level"),
    [
        (0.0999, "ok"),
        (0.10, "warning"),
        (0.2499, "warning"),
        (0.25, "flag"),
        (None, "not_applicable"),
        (float("nan"), "not_applicable"),
    ],
)
def test_psi_level_at_the_boundaries(lib, value, level):
    assert lib["psi_level"](value, SPEC) == level


@pytest.mark.parametrize(
    ("rise", "level"),
    [(4.99, "ok"), (5.0, "flag"), (-3.0, "ok"), (None, "not_applicable")],
)
def test_null_level_at_the_boundary(lib, rise, level):
    assert lib["null_level"](rise, SPEC) == level


def test_missing_settings_stop_the_run(lib):
    rows = [{"parameter": "psi_warning", "parameter_value": "0.1"}]
    with pytest.raises(RuntimeError, match="not recorded"):
        lib["operate_spec_from_rows"](rows)


def test_the_defaults_load_as_typed_settings(lib):
    rows = [
        {"parameter": p, "parameter_value": v} for p, v, _d in lib["OPERATE_DEFAULTS"]
    ]
    spec = lib["operate_spec_from_rows"](rows)
    assert spec["psi_flag"] == 0.25 and spec["max_window_rows"] == 100_000


def test_a_numeric_profile_has_shares_that_sum_to_one(lib):
    row = lib["profile_series"]("x", pd.Series(range(1, 101)), SPEC)
    shares = json.loads(row["bin_shares"])
    assert row["value_kind"] == "numeric" and row["n_rows"] == 100
    assert sum(shares) == pytest.approx(1.0) and len(shares) == 10
    assert (row["min_value"], row["max_value"]) == (1.0, 100.0)


def test_a_constant_column_has_one_edge(lib):
    row = lib["profile_series"]("x", pd.Series([1.0, 1.0, 1.0]), SPEC)
    assert json.loads(row["bin_edges"]) == [1.0]
    assert json.loads(row["bin_shares"]) == [1.0, 0.0]


def test_a_numeric_profile_counts_nulls(lib):
    row = lib["profile_series"]("x", pd.Series([1.0, None, 3.0, None]), SPEC)
    assert row["null_rate"] == pytest.approx(0.5)


def test_a_categorical_profile_keeps_the_top_levels_and_the_rest(lib):
    series = pd.Series(["a"] * 50 + ["b"] * 30 + ["c"] * 15 + ["d"] * 5)
    row = lib["profile_series"]("x", series, SPEC)
    assert row["value_kind"] == "categorical"
    assert json.loads(row["bin_edges"]) == ["a", "b", "c"]
    assert json.loads(row["bin_shares"]) == pytest.approx([0.5, 0.3, 0.15, 0.05])


def test_booleans_and_dates_are_numeric(lib):
    flags = lib["profile_series"]("f", pd.Series([True, False, True]), SPEC)
    days = lib["profile_series"](
        "d", pd.Series([dt.date(2020, 1, 1), dt.date(2020, 1, 2)]), SPEC
    )
    assert flags["value_kind"] == days["value_kind"] == "numeric"
    assert days["min_value"] == 18262.0


def test_the_same_distribution_stays_ok(lib):
    ref = lib["profile_series"]("x", _normal(seed=1), SPEC)
    out = {
        r["check_kind"]: r
        for r in lib["compare_series"](ref, "x", _normal(seed=2), SPEC)
    }
    assert out["feature_drift"]["level"] == "ok"
    assert out["null_rate"]["level"] == "ok"
    assert set(out) == {"feature_drift", "null_rate", "out_of_range"}


def test_a_shifted_distribution_is_flagged_with_the_recorded_thresholds(lib):
    ref = lib["profile_series"]("x", _normal(seed=1), SPEC)
    out = lib["compare_series"](ref, "x", _normal(shift=2.0, seed=2), SPEC)
    drift = next(r for r in out if r["check_kind"] == "feature_drift")
    assert drift["level"] == "flag" and drift["value"] >= 0.25
    assert (drift["warning_threshold"], drift["flag_threshold"]) == (0.10, 0.25)
    assert drift["n_reference"] == 5000 and drift["n_window"] == 5000


@pytest.mark.parametrize(
    ("subject", "level"),
    [
        ("year", "info"),
        ("month", "info"),
        ("trend_years", "info"),
        ("day_of_year", "info"),
        ("iso_week", "info"),
        ("price", "flag"),
    ],
)
def test_calendar_feature_drift_is_informational_only(lib, subject, level):
    ref = lib["profile_series"](subject, _normal(seed=1), SPEC)
    out = lib["compare_series"](ref, subject, _normal(shift=2.0, seed=2), SPEC)
    drift = next(r for r in out if r["check_kind"] == "feature_drift")
    assert drift["level"] == level and drift["value"] >= 0.25


def test_a_calendar_name_is_not_informational_for_the_prediction(lib):
    ref = lib["profile_series"]("year", _normal(seed=1), SPEC)
    out = lib["compare_series"](
        ref, "year", _normal(shift=2.0, seed=2), SPEC, "prediction_"
    )
    drift = next(r for r in out if r["check_kind"] == "prediction_drift")
    assert drift["level"] == "flag"


def test_a_rise_of_the_null_rate_is_flagged(lib):
    ref = lib["profile_series"]("x", _normal(seed=1), SPEC)
    window = _normal(seed=2)
    window[:600] = np.nan
    out = lib["compare_series"](ref, "x", window, SPEC)
    nulls = next(r for r in out if r["check_kind"] == "null_rate")
    assert nulls["level"] == "flag" and nulls["value"] == pytest.approx(12.0)
    assert nulls["flag_threshold"] == 5.0


def test_values_outside_the_reference_range_are_reported_not_flagged(lib):
    ref = lib["profile_series"]("x", pd.Series(range(100)), SPEC)
    out = lib["compare_series"](ref, "x", pd.Series([5, 50, 500, 600]), SPEC)
    extra = next(r for r in out if r["check_kind"] == "out_of_range")
    assert extra["value"] == 0.5 and extra["level"] == "info"


def test_unseen_levels_are_reported_for_a_categorical_column(lib):
    ref = lib["profile_series"]("x", pd.Series(["a"] * 6 + ["b"] * 4), SPEC)
    out = lib["compare_series"](ref, "x", pd.Series(["a", "b", "z", "z"]), SPEC)
    extra = next(r for r in out if r["check_kind"] == "unseen_category")
    assert extra["value"] == 0.5 and extra["level"] == "info"


def test_prediction_checks_carry_the_prediction_kinds(lib):
    ref = lib["profile_series"]("__prediction__", _normal(seed=1), SPEC)
    out = lib["compare_series"](
        ref, "__prediction__", _normal(seed=2), SPEC, "prediction_"
    )
    assert {r["check_kind"] for r in out} == {
        "prediction_drift",
        "prediction_null_rate",
        "prediction_out_of_range",
    }


def test_a_window_without_feature_columns_says_so(lib):
    ref = {"__prediction__": lib["profile_series"]("p", _normal(seed=1), SPEC)}
    out = lib["compare_window"](ref, pd.DataFrame(), _normal(seed=2), SPEC)
    none = next(r for r in out if r["subject"] == "__none__")
    assert none["level"] == "not_applicable"
    assert any(r["check_kind"] == "prediction_drift" for r in out)


def test_a_column_without_a_reference_is_not_applicable(lib):
    features = pd.DataFrame({"new_column": [1.0, 2.0]})
    out = lib["compare_window"]({}, features, pd.Series([1.0, 2.0]), SPEC)
    assert [(r["subject"], r["level"]) for r in out] == [
        ("new_column", "not_applicable")
    ]


@pytest.mark.parametrize(
    ("held", "valid", "higher", "expected"),
    [
        (125.0, 100.0, False, 0.25),
        (80.0, 100.0, False, -0.2),
        (0.4, 0.5, True, 0.2),
        (None, 1.0, True, None),
        (1.0, 0.0, False, None),
        (float("nan"), 1.0, False, None),
    ],
)
def test_relative_degradation(lib, held, valid, higher, expected):
    got = lib["relative_degradation"](held, valid, higher)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def _approval(lib, higher, valid, held, rules=None):
    tid = next(t["task_id"] for t in lib["TASKS"] if t["higher_is_better"] == higher)
    return {
        "task_id": tid,
        "model_name": "m",
        "primary_metric": "metric",
        "validation_value": valid,
        "evaluated_value": held,
        "frozen_delta_version": 3,
        "rules": json.dumps(rules or []),
    }


def test_the_performance_limit_is_exclusive(lib):
    at = lib["performance_result"](_approval(lib, False, 100.0, 120.0), 0.2)
    over = lib["performance_result"](_approval(lib, False, 100.0, 121.0), 0.2)
    assert at["level"] == "ok" and over["level"] == "flag"
    assert over["flag_threshold"] == 0.2 and over["check_kind"] == "performance"


def test_a_missing_recorded_value_is_not_applicable(lib):
    out = lib["performance_result"](_approval(lib, True, None, 0.4), 0.2)
    assert out["level"] == "not_applicable" and out["value"] is None


def _flagged(kind, value=0.4, level="flag", window="held_out"):
    return {
        "task_id": "t",
        "model_name": "m",
        "window_id": window,
        "check_kind": kind,
        "subject": "s",
        "value": value,
        "flag_threshold": 0.25,
        "level": level,
        "detail": "detail text",
    }


def test_a_performance_flag_makes_a_trigger_with_the_recorded_condition(lib):
    rules = [{"rule": "R04", "outcome": "conditional", "detail": "revalidate"}]
    approval = _approval(lib, False, 100.0, 130.0, rules)
    perf = lib["performance_result"](approval, 0.2) | {"window_id": "held_out"}
    out = lib["build_triggers"](approval, [perf], 3, 0.2)
    assert [t["trigger_kind"] for t in out] == ["performance_degradation"]
    assert out[0]["condition"] == "revalidate" and out[0]["source"] == "R04"


def test_a_drift_flag_makes_a_trigger_and_a_warning_does_not(lib):
    approval = _approval(lib, False, 100.0, 100.0)
    flag = [_flagged("feature_drift", 0.5)]
    warn = [_flagged("feature_drift", 0.15, "warning")]
    assert [
        t["trigger_kind"] for t in lib["build_triggers"](approval, flag, 3, 0.2)
    ] == ["drift_flag"]
    assert lib["build_triggers"](approval, warn, 3, 0.2) == []


def test_a_new_frozen_version_makes_a_trigger(lib):
    rules = [{"rule": "R12", "outcome": "conditional", "detail": "new version"}]
    approval = _approval(lib, False, 100.0, 100.0, rules)
    out = lib["build_triggers"](approval, [], 4, 0.2)
    assert [t["trigger_kind"] for t in out] == ["new_frozen_dataset_version"]
    assert out[0]["condition"] == "new version"
    assert lib["build_triggers"](approval, [], 3, 0.2) == []


def _entry():
    return {
        "task_id": "t",
        "model_name": "m",
        "version": 2,
        "dataset_id": "d",
        "frozen_delta_version": 3,
    }


def test_result_rows_have_exactly_the_result_columns(lib):
    found = lib["compare_series"](
        lib["profile_series"]("x", _normal(), SPEC), "x", _normal(seed=2), SPEC
    )
    rows = lib["result_rows"](found, _entry(), "validation", "rid", NOW)
    assert set(rows[0]) == set(_columns(lib["MODEL_DDL"]["monitoring_results"]))
    assert {r["registry_version"] for r in rows} == {2}


def test_reference_rows_have_exactly_the_reference_columns(lib):
    profile = [lib["profile_series"]("x", _normal(), SPEC)]
    rows = lib["reference_rows"](profile, _entry(), "rid", NOW)
    assert set(rows[0]) == set(_columns(lib["MODEL_DDL"]["reference_profile"]))
    assert rows[0]["frozen_delta_version"] == 3 and rows[0]["dataset_id"] == "d"


def test_flag_rows_have_exactly_the_flag_columns_and_only_levels_above_ok(lib):
    results = [
        {
            **_flagged("feature_drift", 0.5),
            "registry_version": 2,
            "warning_threshold": 0.1,
        },
        {
            **_flagged("feature_drift", 0.15, "warning"),
            "registry_version": 2,
            "warning_threshold": 0.1,
        },
        {
            **_flagged("feature_drift", 0.01, "ok"),
            "registry_version": 2,
            "warning_threshold": 0.1,
        },
    ]
    rows = lib["flag_rows"](results, "rid", NOW)
    assert [r["level"] for r in rows] == ["flag", "warning"]
    assert [r["threshold"] for r in rows] == [0.25, 0.1]
    assert set(rows[0]) == set(_columns(lib["MODEL_DDL"]["monitoring_flags"]))


def test_trigger_rows_have_exactly_the_trigger_columns(lib):
    trigger = lib["build_triggers"](_approval(lib, False, 100.0, 100.0), [], 4, 0.2)
    rows = lib["trigger_rows"](trigger, _entry(), "rid", NOW)
    assert set(rows[0]) == set(_columns(lib["MODEL_DDL"]["monitoring_triggers"]))


def test_table_tuples_follow_the_table_order(lib):
    ddl = "a string, b int, c double"
    assert lib["table_tuples"]([{"c": 1.5, "a": "x"}], ddl) == [("x", None, 1.5)]


def _findings_data(lib):
    ref = lib["profile_series"]("x", _normal(), SPEC)
    found = lib["compare_series"](ref, "x", _normal(shift=2.0, seed=2), SPEC)
    results = lib["result_rows"](found, _entry(), "held_out", "rid", NOW)
    return {
        "spec": [
            {
                "parameter": "psi_flag",
                "parameter_value": "0.25",
                "description": "flag",
                "recorded_at": NOW,
            }
        ],
        "reference": [
            {
                "task_id": "t",
                "model_name": "m",
                "registry_version": 2,
                "frozen_delta_version": 3,
                "subject": "x",
                "n_rows": 5000,
            }
        ],
        "predictions": [
            {"task_id": "t", "model_name": "m", "window_id": "held_out", "rows": 10}
        ],
        "results": results,
        "triggers": [
            {
                "task_id": "t",
                "model_name": "m",
                "registry_version": 2,
                "trigger_kind": "drift_flag",
                "source": "monitoring",
                "condition": "a drift check reached the flag level",
                "detail": "/Users/someone@example.com/notebook",
                "value": 0.5,
                "threshold": 0.25,
            }
        ],
        "checks": [
            {
                "component": "models/operate/gate/01_operate_guards",
                "metric_name": "every_registered_model_scored_in_every_window",
                "status": "PASS",
                "error_detail": "",
                "recorded_at": NOW,
            }
        ],
    }


def test_the_findings_carry_every_section_and_no_private_text(lib):
    text = lib["render_monitoring_findings"]("energy", _findings_data(lib), "stamp")
    for part in (
        "## summary",
        "## settings",
        "## reference profiles",
        "## stored predictions",
        "## monitoring by model and window",
        "## warnings and flags",
        "## performance against the recorded values",
        "## retraining triggers",
        "## check results",
        "no held-out target is scored",
    ):
        assert part in text
    assert "someone@example.com" not in text and "/Users/someone" not in text


def test_the_flag_table_is_capped(lib):
    data = _findings_data(lib)
    flag = {**data["results"][0], "level": "flag", "value": 1.0}
    data["results"] = [flag] * (lib["MAX_FINDING_ROWS"] + 5)
    text = lib["render_monitoring_findings"]("commerce", data, "stamp")
    assert f"{lib['MAX_FINDING_ROWS'] + 5} row(s); the first" in text


def test_window_ids_map_to_partitions(lib):
    assert lib["window_partition"]("train") == "train"
    assert lib["window_partition"]("validation") == "validation"
    assert lib["window_partition"]("held_out") == lib["TEST_PARTITION"]
    with pytest.raises(KeyError):
        lib["window_partition"]("unknown")


def test_sampling_is_stable_and_within_the_cap(lib):
    assert list(lib["sample_positions"](5, 10)) == [0, 1, 2, 3, 4]
    first = lib["sample_positions"](1000, 100)
    assert len(first) == 100 and list(first) == sorted(first)
    assert list(first) == list(lib["sample_positions"](1000, 100))


def test_row_keys_join_the_key_columns_or_fall_back_to_the_row_number(lib):
    frame = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    cfg = {"spec": {"key_cols": ["a", "b"]}}
    assert lib["row_keys"](cfg, frame) == ["1|x", "2|y"]
    assert lib["row_keys"]({"spec": {"key_cols": ["zz"]}}, frame) == ["0", "1"]
    cfg = {"spec": {"session_col": "a", "step_col": "b"}}
    assert lib["row_keys"](cfg, frame) == ["1|x", "2|y"]


class _Evaluator:
    def __init__(self, frame, spec=None, classification=False):
        self.frames = {"eval": frame}
        self.spec = spec or {}
        self.classification = classification


class _Bundle:
    def __init__(self, **calls):
        for name, fn in calls.items():
            setattr(self, name, fn)


def test_tabular_predictions_use_the_median_of_a_quantile_bundle(lib):
    frame = pd.DataFrame({"x": [1.0, 2.0]})
    quantile = _Bundle(
        task="quantile", predict=lambda f: {0.1: f.x - 1, 0.5: f.x, 0.9: f.x + 1}
    )
    point = _Bundle(predict=lambda f: f.x * 2)
    ev = _Evaluator(frame)
    values, labels, keys = lib["predict_window"]("tabular", ev, quantile)
    assert list(values) == [1.0, 2.0] and labels is None and keys is None
    assert list(lib["predict_window"]("tabular", ev, point)[0]) == [2.0, 4.0]


def test_survival_anomaly_and_numeric_policy_predictions(lib):
    ev = _Evaluator(pd.DataFrame({"x": [1.0, 2.0]}))
    for kind, name in (
        ("survival", "risk"),
        ("anomaly", "score"),
        ("pumped", "action"),
    ):
        bundle = _Bundle(**{name: lambda f: f.x + 1})
        values, labels, _keys = lib["predict_window"](kind, ev, bundle)
        assert list(values) == [2.0, 3.0] and labels is None


def test_a_categorical_policy_gives_text_labels(lib):
    ev = _Evaluator(pd.DataFrame({"x": [1, 2]}))
    values, labels, keys = lib["predict_window"](
        "action", ev, _Bundle(action=lambda f: np.array([3, 4]))
    )
    assert values is None and list(labels) == ["3", "4"] and keys is None


def test_an_unknown_kind_is_refused(lib):
    with pytest.raises(ValueError, match="no monitoring prediction"):
        lib["predict_window"]("matching", _Evaluator(pd.DataFrame()), _Bundle())


def test_monitored_columns_by_kind(lib):
    frame = pd.DataFrame({"a": [1], "b": [2], "item": [3]})
    pumped = _Evaluator(frame, {"state_cols": ["a", "zz"]})
    ranking = _Evaluator(frame, {"src_col": "item"})
    assert lib["monitored_columns"]("pumped", None, pumped, frame) == ["a"]
    assert lib["monitored_columns"]("ranking", None, ranking, frame) == ["item"]
    assert lib["monitored_columns"]("weak", None, _Evaluator(frame), frame) == []
    assert (
        lib["monitored_columns"]("reconstruction", None, _Evaluator(frame), frame) == []
    )


def test_the_prediction_series_holds_numbers_or_labels(lib):
    nums = lib["prediction_series"]({"values": np.array([1.0, 2.0]), "labels": None})
    text = lib["prediction_series"](
        {"values": None, "labels": np.array(["a", "b"], dtype=object)}
    )
    assert nums.dtype == "float64" and text.dtype == object
