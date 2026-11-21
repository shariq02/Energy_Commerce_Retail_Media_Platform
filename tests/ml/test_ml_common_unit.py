"""Unit tests for the pure (non-Spark-API) parts of `databricks/ml/_ml_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

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


def test_derive_contract_marks_a_declared_label_derived_column(ml):
    contract, classes, _ = ml["derive_contract"](
        ["variant_key", "target_flag", "leaky_score", "plain_score"],
        keys=["variant_key"],
        time_col=None,
        label_derived=("leaky_score",),
        default_clock="static_annotation",
    )
    by = {c[0]: c for c in contract}
    assert by["leaky_score"][1] == "label_derived"
    assert by["plain_score"][1:3] == ("feature", "static_annotation")
    assert "leaky_score" not in {c[0] for c in classes}


def test_the_registry_tables_that_every_ecosystem_shares_live_once(ml):
    for name in ml["ML_SHARED_TABLES"]:
        energy, commerce = ml["ml_fqn"](name, "energy"), ml["ml_fqn"](name, "commerce")
        assert energy == commerce
        assert energy.endswith(f".ml_datasets_shared.{name}")
    assert ".energy_ml_datasets." in ml["ml_fqn"]("dataset_unit", "energy")
    assert ".commerce_ml_datasets." in ml["ml_fqn"]("dataset_lapse", "commerce")
    assert ".genomics_ml_datasets." in ml["ml_fqn"]("dataset_variant", "genomics")


def test_only_registry_tables_are_shared_and_there_are_seven(ml):
    assert set(ml["ML_SHARED_TABLES"]) <= set(ml["REGISTRY_DDL"])
    assert len(ml["ML_SHARED_TABLES"]) == len(ml["REGISTRY_DDL"]) == 7
    for merged in ("split_specification", "null_rate_profile", "gap_limits"):
        assert merged not in ml["REGISTRY_DDL"]


def test_the_merged_registry_tables_carry_the_columns_they_took_over(ml):
    manifest = ml["REGISTRY_DDL"]["dataset_manifest"]
    for column in ("family string", "split_rule string", "embargo_days int"):
        assert column in manifest
    classes = ml["REGISTRY_DDL"]["null_class_registry"]
    assert "null_rate_train double" in classes
    assert "decision string" in classes


def test_every_dataset_belongs_to_a_family_of_its_ecosystem(ml):
    spec = ml["ML_SPEC"]
    for name, dataset in spec["datasets"].items():
        if dataset.get("registered_only"):
            continue
        family = spec["families"][dataset["family"]]
        assert family["ecosystem"] == dataset["ecosystem"], name
        assert ml["dataset_table"](name) == f"dataset_{dataset['family']}"
        assert ml["dataset_table"](name) in ml["family_tables"](dataset["ecosystem"])


def test_every_built_dataset_declares_a_split_a_fit_and_a_known_calendar(ml):
    spec = ml["ML_SPEC"]
    for ecosystem in ("energy", "commerce", "genomics"):
        for name in ml["built_datasets"](ecosystem):
            dataset = spec["datasets"][name]
            assert {"grain", "split", "fit", "target_name"} <= set(dataset), name
            split = dataset["split"]
            for key in ("calendar", "default_calendar", "honda_calendar"):
                if key in split:
                    assert split[key] in ml["SPLIT_CALENDARS"], name
            assert dataset["fit"]["fold_mode"] in ("rolling", "grouped", "none")


def test_a_label_derived_column_is_never_the_target_or_a_key(ml):
    for name, dataset in ml["ML_SPEC"]["datasets"].items():
        derived = set(dataset.get("label_derived", []))
        assert not derived & set(dataset.get("grain", [])), name
        assert dataset.get("target_name") not in derived, name


def test_the_manifest_row_carries_the_split_specification(ml):
    columns = ml["split_columns"]("price_daily")
    assert columns[0] == ml["SPLIT_VERSION"]
    assert columns[1] == "calendar"
    assert columns[2] == "calendar:energy_daily"
    assert columns[3:9] == (
        *ml["SPLIT_CALENDARS"]["energy_daily"]["train"],
        *ml["SPLIT_CALENDARS"]["energy_daily"]["validation"],
        *ml["SPLIT_CALENDARS"]["energy_daily"]["test"],
    )
    assert columns[9] == ml["EMBARGO_DAYS"]["energy_daily"]
    assert len(ml["split_columns"]("survival")) == 13


def test_zone_codes_in_the_spec_are_four_distinct_market_areas(ml):
    rows = ml["ML_SPEC"]["zone_code_map"]
    assert len({r[0] for r in rows}) == len({r[1] for r in rows}) == 4


def test_ecosystem_helpers_scope_a_shared_table(ml):
    assert ml["ddl_with_ecosystem"]("a string") == "a string, ecosystem string"
    assert ml["ddl_with_ecosystem"]("a string, ecosystem string") == (
        "a string, ecosystem string"
    )
    assert ml["ecosystem_predicate"]("energy", "dataset_id = 'x'") == (
        "ecosystem = 'energy' AND (dataset_id = 'x')"
    )


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
