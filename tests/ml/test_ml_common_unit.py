"""Unit tests for the pure (non-Spark-API) parts of `databricks/ml/_ml_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Loaded through `tests/_notebook_loader.py`, which reproduces the notebooks' own
`%run` chain. Only functions that do not call a live Spark API are exercised.
"""

from __future__ import annotations

import datetime as dt

import pytest

from tests._notebook_loader import load_ml_common

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def ml():
    return load_ml_common()


def _d(s):
    return dt.date.fromisoformat(s)


@pytest.mark.parametrize("key", ["energy_daily", "quarter_hour", "redispatch", "honda"])
def test_split_calendars_are_ordered_and_do_not_overlap(ml, key):
    cal = ml["SPLIT_CALENDARS"][key]
    assert _d(cal["train"][0]) < _d(cal["train"][1]) < _d(cal["validation"][0])
    assert _d(cal["validation"][1]) < _d(cal["test"][0])


@pytest.mark.parametrize("key", ["energy_daily", "quarter_hour", "redispatch", "honda"])
def test_every_calendar_has_an_embargo_and_rolling_blocks_inside_train(ml, key):
    cal = ml["SPLIT_CALENDARS"][key]
    assert ml["EMBARGO_DAYS"][key] >= 1
    for a, b in ml["ROLLING_BLOCKS"][key]:
        assert _d(cal["train"][0]) <= _d(a) <= _d(b) <= _d(cal["train"][1])


@pytest.mark.parametrize(
    ("date", "expected"),
    [
        ("2018-09-30", "excluded"),
        ("2018-10-01", "train"),
        ("2023-12-29", "train"),
        ("2023-12-30", "embargo"),
        ("2023-12-31", "embargo"),
        ("2024-01-01", "validation"),
        ("2024-12-29", "validation"),
        ("2024-12-30", "embargo"),
        ("2025-01-01", "test"),
        ("2026-09-01", "test"),
    ],
)
def test_energy_daily_partition_boundaries(ml, date, expected):
    assert ml["partition_for_date_py"](date, "energy_daily") == expected


def test_redispatch_test_partition_closes_with_the_old_regime(ml):
    f = ml["partition_for_date_py"]
    assert f("2020-12-31", "redispatch") == "test"
    assert f("2021-01-01", "redispatch") == "excluded"


def test_partition_dates_never_reappear_after_the_embargo(ml):
    # walking the calendar day by day must move train -> embargo -> validation
    # -> embargo -> test and never go backwards
    order = {"excluded": 0, "train": 1, "embargo": 2, "validation": 3, "test": 4}
    day = _d("2018-09-25")
    last = "excluded"
    seen_test = False
    while day < _d("2026-01-01"):
        p = ml["partition_for_date_py"](day.isoformat(), "energy_daily")
        if p == "embargo":
            day += dt.timedelta(days=1)
            continue
        assert order[p] >= order[last]
        last = p
        seen_test = seen_test or p == "test"
        day += dt.timedelta(days=1)
    assert seen_test


@pytest.mark.parametrize(
    ("year", "easter"),
    [(2024, "2024-03-31"), (2025, "2025-04-20"), (2026, "2026-04-05")],
)
def test_easter_sunday(ml, year, easter):
    assert ml["easter_sunday"](year).isoformat() == easter


def test_german_holidays_include_the_movable_and_fixed_dates(ml):
    days = {d.isoformat() for d in ml["holiday_dates"]("DE", 2025)}
    assert {
        "2025-04-18",
        "2025-04-21",
        "2025-05-29",
        "2025-06-09",
        "2025-10-03",
    } <= days


def test_every_market_area_country_has_holiday_rules(ml):
    assert set(ml["MARKET_AREA_COUNTRY"].values()) <= set(ml["HOLIDAY_RULES"])


def test_gap_caps_and_never_filled_are_disjoint_and_non_negative(ml):
    caps = ml["GAP_LIMIT_CAPS_HOURS"]
    assert all(v >= 0 for v in caps.values())
    assert not set(caps) & set(ml["NEVER_FILLED"])
    assert caps["wind_direction"] == 1


def test_realised_lags_below_two_days_are_refused_before_any_spark_call(ml):
    with pytest.raises(ValueError):
        ml["add_date_lags"](None, keys=[], date_col="d", cols=["x"], lags=[1])
    with pytest.raises(ValueError):
        ml["add_timestamp_lags"](None, keys=[], ts_col="t", cols=["x"], lag_days=[0])


def test_derive_contract_classifies_columns_by_clock(ml):
    contract, classes, defaulted = ml["derive_contract"](
        [
            "market_area_code",
            "local_date",
            "target_price",
            "price_lag2",
            "forecast_total_mwh",
            "day_of_week",
            "capacity_net_mw",
            "mystery",
            "hub_height_is_missing",
            "prefix_view_count",
            "user_hist_session_count",
        ],
        keys=["market_area_code"],
        time_col="local_date",
        registry=("capacity_net_mw",),
    )
    by = {c[0]: c for c in contract}
    assert by["market_area_code"][1] == "key"
    assert by["local_date"][1] == "time"
    assert by["target_price"][1] == "target"
    assert by["price_lag2"][1:4] == ("feature", "observation_realised", 2)
    assert by["forecast_total_mwh"][2] == "publication_before_day"
    assert by["day_of_week"][2] == "calendar"
    assert by["capacity_net_mw"][2] == "registry_effective"
    assert by["hub_height_is_missing"][1] == "flag"
    assert by["prefix_view_count"][2] == "session_prefix"
    assert by["user_hist_session_count"][2] == "user_history_before_session"
    assert defaulted == ["mystery"]
    assert {c[0]: c[1] for c in classes}["target_price"] == "F"


def test_derive_contract_skips_provenance_columns(ml):
    contract, _, _ = ml["derive_contract"](
        ["dataset_id", "_ml_run_id", "x"], keys=[], time_col=None
    )
    assert [c[0] for c in contract] == ["x"]


def test_assembled_select_drops_provenance_and_label_inputs(ml):
    sql = ml["assembled_select"](
        "demo",
        "target_demo",
        "energy",
        [{"table": "features_demo", "keys": ["a", "b"], "drop": ["extra"]}],
        target_drop=["realised_mwh"],
    )
    assert "realised_mwh" in sql.split("FROM")[0]
    assert "EXCEPT" in sql
    assert "t.a = f0.a AND t.b = f0.b" in sql
    assert "energy_ml_datasets.target_demo" in sql


def test_columns_to_drop_uses_the_threshold_and_the_exemptions(ml):
    rates = {"a": 0.5, "b": 0.95, "c": 1.0, "d": 0.99, "e": 0.0}
    assert ml["columns_to_drop"](rates, exempt={"d"}) == ["b", "c"]


def test_static_imputation_scores_three_methods_on_masked_values(ml):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(0)
    n = 400
    cap = rng.uniform(500, 6000, n)
    df = pd.DataFrame(
        {
            "capacity": cap,
            "year": rng.integers(2005, 2024, n),
            "hub": 60 + cap / 60 + rng.normal(0, 2, n),
        }
    )
    scores = ml["static_impute_scores"](df, "hub", ["capacity", "year"], ["year"])
    assert set(scores) == {"group_median", "knn", "regression"}
    assert scores["regression"] < scores["group_median"]
    pred = ml["static_impute_predict"](
        df.iloc[:300],
        df.iloc[300:],
        "hub",
        ["capacity", "year"],
        "regression",
        ["year"],
    )
    assert len(pred) == 100
