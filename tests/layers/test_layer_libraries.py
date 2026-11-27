"""Unit tests for the pure (non-Spark-API) parts of the shared notebook libraries.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

The ML libraries (`_ml_common.py`, the label-proxy check of `_ml_inspect.py`), the
Silver libraries with their genomics part, the Gold library, the EDA library, and the
registry rows that the generator builds from the Silver structures. The libraries are
loaded through `tests/_notebook_loader.py`, which reproduces the notebooks' own `%run`
chain. Only functions that do not call a live Spark API are exercised: those need a
real cluster, which this suite does not have.
"""

from __future__ import annotations

import ast
import copy
import datetime as dt
import re
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

from src.schemas._generate_field_classes import (
    _genomics_namespace,
    _semantic_namespace,
    assert_registry_complete,
    build_rows,
)
from src.schemas._silver_notebook_scan import (
    all_read_silver_tables,
    silver_notebooks,
    write_silver_targets,
)
from tests._notebook_loader import (
    REPO_ROOT,
    genomics_bronze_sources,
    load_eda_common,
    load_genomics_common,
    load_gold_common,
    load_ml_common,
    load_ml_inspect,
    load_silver_common,
)

pytestmark = [pytest.mark.unit]

_SILVER = REPO_ROOT / "databricks" / "silver"
_GOLD = REPO_ROOT / "databricks" / "gold"


CALENDARS = [
    "energy_daily",
    "quarter_hour",
    "redispatch",
    "honda",
    "clinvar_snapshot",
    "trial_start",
    "faers_quarter",
    "cross_border",
    "energy_daily_week",
    "buildout_month",
    "unit_year",
    "retail_month",
]


@pytest.fixture(scope="module")
def ml():
    return load_ml_common()


def _d(s):
    return dt.date.fromisoformat(s)


@pytest.mark.parametrize("key", CALENDARS)
def test_split_calendars_are_ordered_and_do_not_overlap(ml, key):
    cal = ml["SPLIT_CALENDARS"][key]
    assert _d(cal["train"][0]) < _d(cal["train"][1]) < _d(cal["validation"][0])
    assert _d(cal["validation"][1]) < _d(cal["test"][0])


@pytest.mark.parametrize("key", CALENDARS)
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


def test_derive_contract_marks_the_columns_named_by_a_part_of_their_name(ml):
    contract, _, _ = ml["derive_contract"](
        [
            "variant_key",
            "target_is_rare",
            "gnomad_eur_allele_frequency",
            "is_rare_variant",
            "plain_score",
        ],
        keys=["variant_key"],
        time_col=None,
        label_derived_like=("allele_frequency", "is_rare"),
        default_clock="static_annotation",
    )
    by = {c[0]: c[1] for c in contract}
    assert by["gnomad_eur_allele_frequency"] == "label_derived"
    assert by["is_rare_variant"] == "label_derived"
    assert by["plain_score"] == "feature"
    # a target is a target even when a part of its name is declared
    assert by["target_is_rare"] == "target"


def test_a_biological_target_class_column_is_no_target(ml):
    contract, classes, _ = ml["derive_contract"](
        ["gene_key", "target_class_count", "target_class_enzyme", "target_is_rare"],
        keys=["gene_key"],
        time_col=None,
        default_clock="static_annotation",
    )
    by = {c[0]: c[1] for c in contract}
    assert by["target_class_count"] == "feature"
    assert by["target_class_enzyme"] == "feature"
    assert by["target_is_rare"] == "target"
    assert [c[0] for c in classes if c[1] == "F"] == ["target_is_rare"]


def test_is_label_derived_reads_names_and_parts_of_names(ml):
    f = ml["is_label_derived"]
    assert f("a", ("a",), ())
    assert f("x_allele_count", (), ("allele_count",))
    assert not f("x_score", ("a",), ("allele_count",))


def test_the_benchmark_datasets_do_not_read_the_frequency_or_the_coordinates(ml):
    frequency = {
        "allele_count",
        "allele_number",
        "allele_frequency",
        "minor_allele_frequency",
        "minor_allele_count",
    }
    for name, dataset in ml["ML_SPEC"]["datasets"].items():
        if dataset.get("benchmark_set") is None:
            continue
        assert frequency <= set(dataset["label_derived"]), name
        overrides = dataset["contract"]["overrides"]
        for column in ("variant_key", "contig", "position", "reference_allele"):
            assert overrides[column][0] == "key", name


def test_every_label_derived_like_entry_is_a_non_empty_text(ml):
    for name, dataset in ml["ML_SPEC"]["datasets"].items():
        for part in dataset.get("label_derived_like", []):
            assert isinstance(part, str) and part, name


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


def test_every_built_dataset_declares_every_fit_key(ml):
    for ecosystem in ("energy", "commerce", "genomics"):
        for name in ml["built_datasets"](ecosystem):
            fit = ml["ML_SPEC"]["datasets"][name]["fit"]
            assert set(ml["FIT_KEYS"]) <= set(fit), name


def test_spec_problems_is_empty_for_a_dataset_whose_frame_has_its_columns(ml):
    datasets = ml["ML_SPEC"]["datasets"]
    columns = {*datasets["price_daily"]["grain"], "local_date", "market_area_code"}
    assert ml["spec_problems"]("price_daily", columns) == []


def _probe_problems(ml, base, columns, **changes):
    """spec_problems of a copy of the dataset `base` with `changes`, named `_probe`."""
    datasets = ml["ML_SPEC"]["datasets"]
    datasets["_probe"] = {**datasets[base], **changes}
    try:
        return ml["spec_problems"]("_probe", columns)
    finally:
        del datasets["_probe"]


def _price_daily_columns(ml):
    base = ml["ML_SPEC"]["datasets"]["price_daily"]
    return {*base["grain"], "local_date", "market_area_code"}


def test_spec_problems_names_a_missing_fit_key_and_a_missing_column(ml):
    base = ml["ML_SPEC"]["datasets"]["price_daily"]
    fit = {k: v for k, v in base["fit"].items() if k != "date_col"}
    problems = _probe_problems(ml, "price_daily", {"market_area_code"}, fit=fit)
    assert "_probe: no fit key date_col" in problems
    assert "_probe: split column local_date is not in the frame" in problems
    assert "_probe: grain column local_date is not in the frame" in problems


@pytest.mark.parametrize("key", ["max_rows", "min_labelled_rows"])
@pytest.mark.parametrize("bad", [0, -5, 1.5, "10", True])
def test_spec_problems_rejects_a_bad_row_limit(ml, key, bad):
    problems = _probe_problems(
        ml, "price_daily", _price_daily_columns(ml), **{key: bad}
    )
    assert f"_probe: {key} must be a positive whole number" in problems


@pytest.mark.parametrize(
    ("key", "good"), [("max_rows", 1_000_000), ("min_labelled_rows", 10)]
)
def test_spec_problems_accepts_a_positive_row_limit(ml, key, good):
    problems = _probe_problems(
        ml, "price_daily", _price_daily_columns(ml), **{key: good}
    )
    assert problems == []


def test_the_monthly_retail_dataset_declares_its_own_label_floor(ml):
    spec = ml["ML_SPEC"]["datasets"]
    assert spec["retail_demand_monthly"]["min_labelled_rows"] == 10
    others = [k for k, v in spec.items() if "min_labelled_rows" in v]
    assert others == ["retail_demand_monthly"]


def test_spec_problems_reports_a_dataset_that_is_not_in_the_spec(ml):
    assert ml["spec_problems"]("_unknown", set()) == ["_unknown: not in the spec file"]


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


def test_a_source_split_with_train_rows_also_gives_a_validation_partition(ml):
    """Training and evaluation need train and validation rows; a source split gives
    only train and test, so every trained source-split dataset declares a share."""
    root = REPO_ROOT / "src" / "schemas" / "mappings"
    tasks = yaml.safe_load((root / "model_tasks.yml").read_text(encoding="utf-8"))
    trained = {t["dataset_id"] for t in tasks["tasks"].values()}
    for name in trained:
        split = ml["ML_SPEC"]["datasets"][name]["split"]
        if split["rule"] == "source_defined":
            assert split.get("validation_percent"), name
            assert split.get("group_cols"), name


def test_spec_problems_names_a_validation_group_column_the_frame_lacks(ml):
    datasets = ml["ML_SPEC"]["datasets"]
    name = "benchmark_maps"
    columns = {*datasets[name]["grain"], "split", "protein_id"}
    assert ml["spec_problems"](name, columns) == []
    split = {**datasets[name]["split"], "group_cols": ["missing_column"]}
    problems = _probe_problems(ml, name, columns, split=split)
    assert "_probe: split column missing_column is not in the frame" in problems


def test_the_new_calendars_close_each_partition_on_their_own_dates(ml):
    f = ml["partition_for_date_py"]
    assert f("2022-01-01", "clinvar_snapshot") == "train"
    assert f("2023-01-01", "clinvar_snapshot") == "validation"
    assert f("2026-10-26", "clinvar_snapshot") == "test"
    assert f("2021-12-31", "unit_year") == "embargo"
    assert f("2022-01-01", "unit_year") == "validation"
    assert f("2025-12-31", "unit_year") == "test"
    assert f("2026-01-01", "unit_year") == "excluded"
    assert f("2018-09-30", "cross_border") == "excluded"
    assert f("2018-10-01", "cross_border") == "train"
    assert f("2024-06-01", "cross_border") == "validation"
    assert f("2023-12-01", "buildout_month") == "embargo"
    assert f("2024-01-01", "buildout_month") == "validation"


def test_a_source_split_carves_its_validation_by_gene_or_protein_group(ml):
    datasets = ml["ML_SPEC"]["datasets"]
    expected = {
        "benchmark_causal_eqtl": ["split_group"],
        "benchmark_maps": ["protein_id"],
        "benchmark_pathogenic_coding": ["split_group"],
    }
    for name, columns in expected.items():
        assert datasets[name]["split"]["validation_group_cols"] == columns, name


def test_spec_problems_names_a_validation_group_column_by_its_name(ml):
    datasets = ml["ML_SPEC"]["datasets"]
    name = "benchmark_maps"
    problems = ml["spec_problems"](name, {*datasets[name]["grain"], "split"})
    assert f"{name}: split column protein_id is not in the frame" in problems


def test_reclassification_reads_the_label_derived_list_of_the_variant_label(ml):
    datasets = ml["ML_SPEC"]["datasets"]
    assert (
        datasets["variant_reclassification"]["label_derived"]
        == datasets["variant_pathogenicity"]["label_derived"]
    )


def test_the_second_wave_datasets_are_declared_in_the_families_of_their_tables(ml):
    spec = ml["ML_SPEC"]
    second_wave = {
        "variant_reclassification": "reclassification",
        "trial_termination": "trial",
        "faers_outcome": "faers",
        "depmap_essentiality": "dependency",
        "gtex_expression": "expression",
        "gene_disease_progression": "association",
        "clingen_validity": "association",
        "dms_effect": "assay",
        "mhc_binding": "assay",
        "allele_frequency": "frequency",
        "cancer_type": "tumor",
        "haploinsufficiency": "gene",
        "cross_border_spread": "cross_border",
        "price_volatility": "market_daily",
        "price_spike": "market_daily",
        "solar_storage_buildout": "buildout",
        "unit_decommissioning": "unit",
        "cart_abandonment": "session_purchase",
        "session_revenue": "session_purchase",
        "retail_demand_monthly": "demand_monthly",
    }
    for name, family in second_wave.items():
        assert spec["datasets"][name]["family"] == family, name
        assert spec["datasets"][name]["target_name"].startswith("target_"), name


# --- the label-proxy check of the ML inspect library -------------------------

ROWS = 6000


@pytest.fixture(scope="module")
def proxy_check():
    return load_ml_inspect()["label_proxy_columns"]


def _frame(**columns):
    return pd.DataFrame(columns)


def test_a_column_that_is_null_exactly_for_one_class_gives_the_label_away(proxy_check):
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, ROWS)
    frequency = np.where(y == 1, np.nan, rng.uniform(0.0, 1.0, ROWS))
    noise = rng.normal(size=ROWS)
    found = proxy_check(
        _frame(y=y, frequency=frequency, noise=noise), "y", ["frequency", "noise"]
    )
    assert ("frequency", "one column") in found
    assert "noise" not in {c for c, _how in found}


def test_two_columns_that_make_the_rule_are_found_by_the_tree(proxy_check):
    rng = np.random.default_rng(1)
    a = rng.uniform(0.0, 1.0, ROWS)
    b = rng.integers(0, 2, ROWS)
    y = ((a < 0.3) & (b == 1)).astype(int)
    noise = rng.normal(size=ROWS)
    frame = _frame(y=y, a=a, b=b, noise=noise)
    found = dict(proxy_check(frame, "y", ["a", "b", "noise"]))
    assert {"a", "b"} <= set(found)
    assert "noise" not in found


def test_columns_that_do_not_know_the_label_are_not_flagged(proxy_check):
    rng = np.random.default_rng(2)
    y = rng.integers(0, 2, ROWS)
    frame = _frame(y=y, x=rng.normal(size=ROWS), z=rng.integers(0, 5, ROWS))
    assert proxy_check(frame, "y", ["x", "z"]) == []


def test_a_strong_but_honest_column_is_not_flagged(proxy_check):
    rng = np.random.default_rng(3)
    x = rng.normal(size=ROWS)
    y = (x + rng.normal(scale=1.0, size=ROWS) > 0).astype(int)
    assert proxy_check(_frame(y=y, x=x), "y", ["x"]) == []


def test_a_unique_identifier_is_pure_by_size_and_is_not_flagged(proxy_check):
    rng = np.random.default_rng(4)
    y = rng.integers(0, 2, ROWS)
    frame = _frame(y=y, record_id=[f"id{i}" for i in range(ROWS)])
    assert proxy_check(frame, "y", ["record_id"]) == []


def test_a_label_that_is_not_binary_or_has_one_class_returns_nothing(proxy_check):
    rng = np.random.default_rng(5)
    x = rng.normal(size=ROWS)
    assert proxy_check(_frame(y=x, x=x), "y", ["x"]) == []
    assert proxy_check(_frame(y=np.zeros(ROWS, dtype=int), x=x), "y", ["x"]) == []


def test_a_boolean_label_and_a_text_column_work(proxy_check):
    rng = np.random.default_rng(6)
    y = rng.integers(0, 2, ROWS).astype(bool)
    origin = np.where(y, "germline", "somatic")
    found = proxy_check(_frame(y=y, origin=origin), "y", ["origin"])
    assert ("origin", "one column") in found


# --- the Silver library -------------------------------------------------------


@pytest.fixture(scope="module")
def silver():
    return load_silver_common()


def test_repo_root_resolves_to_the_actual_repo(silver):
    root = silver["repo_root"]()
    assert Path(root) == REPO_ROOT


def test_load_yaml_reads_a_real_file(silver):
    doc = silver["load_yaml"]("src/schemas/reference/source_ecosystem_map.yml")
    assert doc["version"] == 1
    assert "mappings" in doc


def test_load_contract_and_load_mapping_resolve_the_conventional_path(silver):
    contract = silver["load_contract"]("dwd")
    mapping = silver["load_mapping"]("dwd")
    assert isinstance(contract, dict) and contract
    assert isinstance(mapping, dict) and mapping


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("smard", "energy"),
        ("dwd", "energy"),
        ("mastr", "energy"),
        ("honda_iot", "energy"),
        ("rees46", "commerce"),
        ("ga4", "commerce"),
        ("clinvar", "genomics"),
        ("gencode", "genomics"),
    ],
)
def test_ecosystem_for_matches_the_governed_map(silver, source, expected):
    assert silver["ecosystem_for"](source) == expected


def test_ecosystem_for_strict_raises_on_an_unknown_source(silver):
    with pytest.raises(ValueError, match="source_ecosystem_map"):
        silver["ecosystem_for"]("a_source_that_does_not_exist", strict=True)


def test_ecosystem_for_strict_accepts_a_mapped_source(silver):
    assert silver["ecosystem_for"]("clinvar", strict=True) == "genomics"


def test_ecosystem_for_unknown_source_falls_back_to_the_documented_default(silver):
    # _DEFAULT_ECOSYSTEM in _silver_common.py -- asserted by value, not by
    # re-reading the constant, so this catches an accidental default change.
    assert silver["ecosystem_for"]("a_source_that_does_not_exist") == "energy"


def test_contract_tables_accepts_the_multi_table_form(silver):
    contract = {
        "tables": [
            {"name": "a", "columns": [{"name": "x"}]},
            {"name": "b", "columns": [{"name": "y"}]},
            {"name": "c_no_columns"},
        ]
    }
    tables = silver["contract_tables"](contract)
    assert set(tables) == {"a", "b"}
    assert tables["a"]["columns"] == [{"name": "x"}]


def test_contract_tables_accepts_the_legacy_single_table_form(silver):
    contract = {
        "source": "dwd",
        "bronze_table": "bronze.dwd_hourly_air_temperature",
        "columns": [{"name": "mess_datum"}],
    }
    tables = silver["contract_tables"](contract)
    assert set(tables) == {"dwd_hourly_air_temperature"}


def test_contract_tables_returns_empty_for_neither_form(silver):
    assert silver["contract_tables"]({"unrelated": True}) == {}


def test_flatten_business_names_is_a_real_dwd_mapping(silver):
    mapping = silver["load_mapping"]("dwd")
    business_names = silver["flatten_business_names"](mapping, "dwd")
    assert isinstance(business_names, dict) and business_names
    assert all(isinstance(v, str) for v in business_names.values())


def test_coded_columns_returns_the_dwd_decode_maps(silver):
    mapping = silver["load_mapping"]("dwd")
    coded = silver["coded_columns"](mapping, "dwd")
    assert isinstance(coded, dict) and coded
    # WW is a real DWD coded column with no map (present-quantity code table
    # too large to enumerate) -- confirms the None-passthrough case, not just
    # the populated-map case
    assert "WW" in coded
    assert coded["WW"] is None


def test_coded_columns_mastr_uses_the_named_category_binding_shape(silver):
    mapping = silver["load_mapping"]("mastr")
    coded = silver["coded_columns"](mapping, "mastr")
    assert isinstance(coded, dict)


def test_untranslated_source_columns_flags_source_names_only(silver):
    # a MaStR `_nv` flag is decoded before this check, so one left over is flagged
    found = silver["untranslated_source_columns"](
        ["unit_id", "Strasse", "MastrNummer", "Fax_nv", "capacity_gross_kw"]
    )
    assert found == ["Strasse", "MastrNummer", "Fax_nv"]


def test_mastr_not_available_name_uses_the_english_field(silver):
    name = silver["mastr_not_available_name"]
    assert name("Hausnummer_nv", {"Hausnummer": "house_number"}) == (
        "house_number_not_available"
    )


def test_a_not_available_flag_is_no_mastr_date(silver):
    is_date = silver["_is_mastr_date"]
    assert not is_date("water_rights_expiry_date_not_available")
    assert is_date("water_rights_expiry_date")
    assert is_date("authorisation_deadline")


def test_biomethane_first_use_is_a_mastr_date(silver):
    assert silver["_is_mastr_date"]("biomethane_first_use")


class _Writer:
    def __init__(self, spark_stub):
        self.spark_stub = spark_stub

    def format(self, _name):
        return self

    def mode(self, _name):
        return self

    def saveAsTable(self, table):
        self.spark_stub.inserts.append((table, len(self.spark_stub.frame_rows)))


class _SparkStub:
    """Records one insert per `flush_logs` table; no Spark behaviour."""

    def __init__(self):
        self.inserts = []
        self.frame_rows = []

    def createDataFrame(self, rows, _ddl):
        self.frame_rows = list(rows)
        return type("Frame", (), {"write": _Writer(self)})()


def test_log_rows_wait_for_one_insert_per_log_table(silver, monkeypatch):
    stub = _SparkStub()
    monkeypatch.setitem(silver, "spark", stub)
    silver["_AUDIT_ROWS"].clear()
    silver["_WATERMARK_ROWS"].clear()
    started = silver["now_utc"]()
    for name in ("a", "b", "c"):
        silver["audit"]("comp", "src", name, 1.0, rid="run")
    silver["watermark"]("comp", "src", 5, "COMPLETE", "run", started)
    assert stub.inserts == []
    silver["flush_logs"]()
    tables = [table for table, _ in stub.inserts]
    assert tables == [silver["AUDIT_TABLE"], silver["WATERMARK_TABLE"]]
    assert silver["_AUDIT_ROWS"] == [] and silver["_WATERMARK_ROWS"] == []
    silver["flush_logs"]()
    assert len(stub.inserts) == 2


def test_audit_row_matches_the_log_table_columns(silver):
    silver["_AUDIT_ROWS"].clear()
    silver["audit"]("comp", "src", "rows_written", 7, status="PASS", rid="run")
    row = silver["_AUDIT_ROWS"].pop()
    assert len(row) == len(silver["_AUDIT_DDL"].split(","))
    assert (row[0], row[2], row[4], row[5], row[6], row[8]) == (
        "run",
        "src",
        "comp",
        "rows_written",
        7.0,
        "PASS",
    )


class _HistoryStub:
    def __init__(self, rows):
        self.rows = rows

    def sql(self, _statement):
        return self

    def select(self, *_columns):
        return self

    def collect(self):
        return self.rows


def test_delta_write_result_reads_rows_and_the_version_before(silver, monkeypatch):
    history = [
        {"version": 4, "operationMetrics": {"numOutputRows": "120"}},
        {"version": 3, "operationMetrics": {}},
    ]
    monkeypatch.setitem(silver, "spark", _HistoryStub(history))
    assert silver["delta_write_result"]("t") == (120, 3)


def test_delta_write_result_of_a_new_table_has_no_version_before(silver, monkeypatch):
    history = [{"version": 0, "operationMetrics": {"numOutputRows": "9"}}]
    monkeypatch.setitem(silver, "spark", _HistoryStub(history))
    assert silver["delta_write_result"]("t") == (9, None)


# --- the genomics Silver library ----------------------------------------------

_GOVERNANCE = {
    "source_system",
    "source_record_id",
    "ecosystem",
    "_silver_loaded_at",
    "_silver_run_id",
}
# Silver is flat: no array, struct or map type.
_SPARK_TYPES = re.compile(r"^(string|int|bigint|double|boolean|date|timestamp)$")


@pytest.fixture(scope="module")
def genomics():
    return load_genomics_common()


@pytest.fixture(scope="module")
def rules(genomics):
    return genomics["load_genomics_rules"]()


def test_the_two_schemas_hold_twelve_and_twenty_eight_tables(genomics):
    reference = genomics["GENOMICS_REFERENCE_TABLES"]
    main = genomics["GENOMICS_MAIN_TABLES"]
    assert len(reference) == 12
    assert len(main) == 28
    assert reference.isdisjoint(main)


def test_the_batch_four_tables_are_declared_in_the_main_schema(genomics):
    for table in (
        "clinical_trial",
        "protein_residue_score",
        "gene_dependency",
        "regulatory_element_gene_link",
        "epitope",
        "epitope_mhc_assay",
        "gene_disease_score_year",
    ):
        assert table in genomics["GENOMICS_STRUCTURES"], table
        assert genomics["genomics_target_schema"](table) == "genomics_silver"


def test_clinvar_tables_carry_the_snapshot_run(genomics):
    for table in (
        "entity_claim",
        "variant_hgvs",
        "variant_condition",
        "variant_classification",
    ):
        columns = dict(genomics["GENOMICS_STRUCTURES"][table])
        assert columns["first_source_snapshot"] == "string", table
        assert columns["last_source_snapshot"] == "string", table
        assert "source_snapshot" not in columns, table


def test_cell_lines_carry_the_depmap_model_columns(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["cell_line"]}
    assert {"in_tahoe", "in_depmap", "oncotree_lineage", "cosmic_id"} <= columns


def test_tumor_samples_carry_the_cohort_of_their_site(genomics):
    columns = dict(genomics["GENOMICS_STRUCTURES"]["tumor_sample"])
    assert columns["cohort"] == "string"
    assert "tissue_source_site" in columns


def test_every_table_has_a_grain_and_no_stray_table_has_one(genomics):
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    assert set(genomics["GENOMICS_TABLE_GRAIN"]) == tables


def test_target_schema_follows_the_table_sets(genomics):
    assert (
        genomics["genomics_target_schema"]("entity_master")
        == "genomics_silver_reference"
    )
    assert genomics["genomics_target_schema"]("variant") == "genomics_silver"
    with pytest.raises(KeyError):
        genomics["genomics_target_schema"]("not_a_genomics_table")


def test_declared_columns_are_lower_snake_case_with_known_types(genomics):
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    for table, columns in genomics["GENOMICS_STRUCTURES"].items():
        assert table in tables, table
        names = [name for name, _ in columns]
        assert len(names) == len(set(names)), f"duplicate column in {table}"
        assert not _GOVERNANCE & set(names), f"{table} repeats a governance column"
        for name, spark_type in columns:
            assert re.fullmatch(r"[a-z][a-z0-9_]*", name), (table, name)
            assert _SPARK_TYPES.match(spark_type), (table, name, spark_type)


def test_a_window_reads_the_bases_around_a_variant(genomics):
    # the chunk starts at position 11 and the flank is 2: the sequence starts at 9
    out = genomics["window_features"]("TTACGTAAC", 11, 2, 11, "A", "G")
    assert out["sequence_window"] == "TTACG"
    assert out["reference_base"] == "A"
    assert out["reference_matches_genome"] is True
    assert out["is_transition"] is True


def test_a_window_flags_a_cpg_site_and_a_homopolymer(genomics):
    out = genomics["window_features"]("AACGAA", 3, 1, 4, "C", "T")
    assert out["is_cpg_site"] is True
    run = genomics["window_features"]("AAAAAAA", 4, 3, 4, "A", "T")
    assert run["homopolymer_length"] == 7
    assert run["is_transition"] is False


def test_a_window_beyond_the_chunk_is_none(genomics):
    assert genomics["window_features"]("ACG", 3, 5, 3, "A", "G") is None


def _substitutions(genomics, rules):
    claim = rules["claim_attributes"]
    acids = claim["amino_acids"]
    blosum = {(a, b): 0 for a in acids for b in acids}
    rows = genomics["substitution_rows"](acids, claim["grantham"], blosum)
    return {(r["from_amino_acid"], r["to_amino_acid"]): r for r in rows}


def test_the_substitution_table_has_every_ordered_pair(genomics, rules):
    rows = _substitutions(genomics, rules)
    assert len(rows) == 400
    assert rows[("L", "L")]["grantham_distance"] == 0


@pytest.mark.parametrize(
    ("pair", "distance"),
    [(("L", "I"), 5), (("C", "W"), 215), (("S", "R"), 110), (("A", "G"), 60)],
)
def test_grantham_distances_match_the_published_values(genomics, rules, pair, distance):
    rows = _substitutions(genomics, rules)
    assert abs(rows[pair]["grantham_distance"] - distance) <= 1
    reverse = rows[pair[::-1]]["grantham_distance"]
    assert reverse == pytest.approx(rows[pair]["grantham_distance"])


def test_the_claim_attribute_rules_are_declared(rules):
    claim = rules["claim_attributes"]
    for key in (
        "therapeutic_area_minimum_diseases",
        "drug_stage_order",
        "locus_block_bases",
        "sequence_window_flank",
        "allele_fraction_low_below",
        "tumor_mutational_burden_high_from",
    ):
        assert key in claim, key
    assert len(claim["amino_acids"]) == 20


def test_the_protein_master_carries_the_amino_acid_sequence(genomics):
    assert "amino_acid_sequence" in genomics["PROTEIN_FIELDS"]


def test_the_real_rules_file_is_sound(genomics, rules):
    assert genomics["validate_genomics_rules"](rules, genomics_bronze_sources()) == []


def _problems_of(genomics, rules, change):
    """validate_genomics_rules of a copy of the rules that `change` has altered."""
    broken = copy.deepcopy(rules)
    change(broken)
    return genomics["validate_genomics_rules"](broken, genomics_bronze_sources())


def test_a_missing_section_is_reported(genomics, rules):
    problems = _problems_of(genomics, rules, lambda r: r.pop("precedence"))
    assert problems == ["missing section 'precedence'"]


def test_an_unknown_source_in_a_precedence_list_is_reported(genomics, rules):
    problems = _problems_of(
        genomics,
        rules,
        lambda r: r["precedence"]["gene"]["symbol"].append("nosuchsource.table.column"),
    )
    assert any("nosuchsource.table.column" in p for p in problems)


def test_review_stars_outside_the_scale_are_reported(genomics, rules):
    problems = _problems_of(
        genomics,
        rules,
        lambda r: r["clinvar_review_stars"].update({"practice guideline": 9}),
    )
    assert any("practice guideline" in p for p in problems)


def test_a_repeated_crosswalk_method_is_reported(genomics, rules):
    problems = _problems_of(
        genomics, rules, lambda r: r["crosswalk_method_precedence"].append("exact_id")
    )
    assert any("crosswalk_method_precedence" in p for p in problems)


def test_a_rising_feature_band_is_reported(genomics, rules):
    problems = _problems_of(
        genomics, rules, lambda r: r["clinical_features"]["mutation_burden"].reverse()
    )
    assert any("clinical_features.mutation_burden" in p for p in problems)


def test_unordered_band_limits_are_reported(genomics, rules):
    bands = [{"label": "a", "below": 0.05}, {"label": "b", "below": 0.01}]
    problems = _problems_of(
        genomics, rules, lambda r: r.update({"frequency_bands": bands})
    )
    assert any("frequency_bands" in problem for problem in problems)


def test_a_rising_from_list_is_reported(genomics, rules):
    tiers = [{"from": 1, "label": "Low Quality"}, {"from": 3, "label": "High Quality"}]
    problems = _problems_of(
        genomics, rules, lambda r: r.update({"review_quality_tiers": tiers})
    )
    assert any("review_quality_tiers" in problem for problem in problems)


def test_an_unknown_flag_in_the_gene_category_rules_is_reported(genomics, rules):
    problems = _problems_of(
        genomics,
        rules,
        lambda r: r["primary_gene_category"][0]["any"].append(
            {"flag": "has_no_such_flag"}
        ),
    )
    assert any("has_no_such_flag" in problem for problem in problems)


def test_the_crosswalk_precedence_starts_with_the_exact_id(rules):
    assert rules["crosswalk_method_precedence"][0] == "exact_id"


def test_the_clinvar_snapshot_rule_names_a_snapshot(rules):
    assert rules["clinvar_current_snapshot"] == "current"
    assert "current" in rules["clinvar_snapshot_dates"]


def test_every_dbvar_study_has_an_assembly(rules):
    assert set(rules["dbvar_assembly_by_study"].values()) == {"GRCh38"}


def test_orthologue_species_name_every_depth_level(rules):
    depth = rules["orthologue_depth"]
    named = {entry["level"] for entry in depth["species"].values()}
    assert named == set(depth["levels"])
    assert len(depth["consensus_classes"]) == 8


def test_plddt_bands_follow_alphafold(rules):
    bands = rules["plddt_bands"]
    assert [b["from"] for b in bands["bands"]] == [90, 70, 50]
    assert bands["band_default"] == "very_low"


def test_the_dose_record_number_is_declared(genomics):
    columns = dict(genomics["GENOMICS_STRUCTURES"]["faers_case_drug"])
    assert columns["dose_record_number"] == "int"


def test_conflicting_classifications_get_one_star(rules):
    stars = rules["clinvar_review_stars"]
    assert stars["criteria provided, conflicting classifications"] == 1
    assert stars["criteria provided, multiple submitters, no conflicts"] == 2


def test_precedence_rank_is_the_one_based_place_in_the_list(genomics, rules):
    rank = genomics["precedence_rank"]
    assert rank(rules, "gene", "symbol", "ncbi_gene.gene_info", "Symbol") == 2
    assert rank(rules, "gene", "hgnc_id", "gencode.annotation", "attributes") == 3
    assert rank(rules, "gene", "symbol", "ncbi_gene.gene_info", "dbXrefs") is None
    assert rank(rules, "no_such_entity", "symbol", "x.y", "z") is None
    assert rank(rules, "coordinate", "symbol", "x.y", "z") is None


def test_the_gene_coordinate_order_lists_gencode_first(rules):
    assert rules["precedence"]["coordinate"][0] == "gencode.annotation"
    assert "tcga.mc3_somatic_mutations" not in rules["precedence"]["coordinate"]


def test_tcga_is_not_a_source_of_the_variant_coordinate(rules):
    order = rules["precedence"]["variant"]["coordinate"]
    assert not any(entry.startswith("tcga.") for entry in order)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("chr1", "1"),
        ("chrX", "X"),
        ("chrx", "X"),
        ("M", "MT"),
        ("chrM", "MT"),
        ("MT", "MT"),
        ("22", "22"),
        ("KI270706.1", "KI270706.1"),
        ("chrUn_GL000220v1", "Un_GL000220v1"),
        (" 7 ", "7"),
        ("", None),
        (None, None),
    ],
)
def test_normalise_chromosome(genomics, value, expected):
    assert genomics["normalise_chromosome"](value) == expected


def test_variant_key_joins_the_five_parts(genomics):
    assert genomics["variant_key"]("GRCh38", "chr1", "976215", "a", "g") == (
        "GRCh38:1:976215:A:G"
    )
    assert genomics["variant_key"]("GRCh38", "chrM", 5, "A", "G") == "GRCh38:MT:5:A:G"


@pytest.mark.parametrize(
    "args",
    [
        ("GRCh38", "1", None, "A", "G"),
        ("GRCh38", "1", "12x", "A", "G"),
        ("GRCh38", "1", "100", "", "G"),
        ("GRCh38", None, "100", "A", "G"),
        (None, "1", "100", "A", "G"),
    ],
)
def test_variant_key_is_none_when_a_part_is_missing(genomics, args):
    assert genomics["variant_key"](*args) is None


def test_bed_start_becomes_one_based(genomics):
    assert genomics["bed_to_closed"](0, 10) == (1, 10)


@pytest.mark.parametrize(
    ("accession", "expected"),
    [
        ("ENSG00000135596.20", ("ENSG00000135596", "20")),
        ("NC_000019.10", ("NC_000019", "10")),
        ("ENSG00000135596", ("ENSG00000135596", None)),
        (None, (None, None)),
        ("", (None, None)),
    ],
)
def test_strip_version(genomics, accession, expected):
    assert genomics["strip_version"](accession) == expected


def test_split_values_can_keep_the_empty_positions(genomics):
    text = "a|b||d|"
    assert genomics["split_values"](text) == ["a", "b", "d"]
    assert genomics["split_values"](text, keep_empty=True) == ["a", "b", "", "d", ""]
    assert genomics["split_values"](None) == []


def test_gtf_attributes_keep_a_repeated_key_in_order(genomics):
    text = (
        'gene_id "ENSG00000135596.20"; transcript_id "ENST00000856847.2"; '
        'gene_type "protein_coding"; gene_name "MICAL1"; level 2; '
        'protein_id "ENSP00000526906.1"; hgnc_id "HGNC:20619"; tag "basic"; '
        'tag "TAGENE"; havana_gene "OTTHUMG00000015350.9";'
    )
    found = genomics["parse_gtf_attributes"](text)
    assert found["gene_id"] == ["ENSG00000135596.20"]
    assert found["hgnc_id"] == ["HGNC:20619"]
    assert found["level"] == ["2"]
    assert found["tag"] == ["basic", "TAGENE"]
    assert genomics["parse_gtf_attributes"](None) == {}


def test_gtf_attributes_of_a_refseq_row(genomics):
    text = 'gene_id "TRNP"; transcript_id "rna-TRNP";  gene_name "TRNP";'
    assert genomics["parse_gtf_attributes"](text) == {
        "gene_id": ["TRNP"],
        "transcript_id": ["rna-TRNP"],
        "gene_name": ["TRNP"],
    }


def test_assembly_label_uses_the_rules(genomics, rules):
    label = genomics["assembly_label"]
    assert label("Reference GRCh38.p14 Primary Assembly", rules) == "GRCh38"
    assert label("Alternate T2T-CHM13v2.0", rules) == "T2T-CHM13v2.0"
    assert label("GRCh38.p14 PATCHES", rules) is None
    assert label(None, rules) is None


def test_clinvar_conditions_keep_the_empty_group_and_pair_by_position(genomics):
    rows = genomics["parse_clinvar_conditions"](
        "MONDO:MONDO:0013342,MedGen:C3150901,OMIM:613647,Orphanet:306511"
        "||MedGen:C3661900",
        "Hereditary spastic paraplegia 48|Macular dystrophy with or without "
        "extraocular features|not provided",
        "RCV000000012|RCV005255549|RCV004998069",
    )
    assert [r["position"] for r in rows] == [1, 2, 3]
    assert rows[0]["ids"] == [
        "MONDO:MONDO:0013342",
        "MedGen:C3150901",
        "OMIM:613647",
        "Orphanet:306511",
    ]
    assert rows[1]["ids"] == []
    assert rows[1]["name"] == "Macular dystrophy with or without extraocular features"
    assert rows[2]["accession"] == "RCV004998069"
    assert {r["pairing_status"] for r in rows} == {"paired"}


def test_clinvar_conditions_with_a_trailing_empty_group(genomics):
    rows = genomics["parse_clinvar_conditions"](
        "MedGen:C1|MedGen:C2|MedGen:C3|",
        "a|b|c|FOXRED1-related disorder",
        "RCV1|RCV2|RCV3|RCV4",
    )
    assert len(rows) == 4
    assert rows[3]["ids"] == []
    assert rows[3]["name"] == "FOXRED1-related disorder"


def test_clinvar_conditions_with_unequal_lists_are_marked(genomics):
    rows = genomics["parse_clinvar_conditions"]("MedGen:C1|MedGen:C2", "a", "RCV1|RCV2")
    assert len(rows) == 2
    assert {r["pairing_status"] for r in rows} == {"count_mismatch"}
    assert rows[1]["name"] is None


def test_hgvs_name_delins(genomics):
    out = genomics["parse_hgvs_name"](
        "NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA "
        "(p.Arg27_Ile28delinsLeuLeuTer)"
    )
    assert out["parse_status"] == "ok"
    assert (out["transcript_accession"], out["transcript_version"]) == (
        "NM_014855",
        3,
    )
    assert out["gene_symbol"] == "AP5Z1"
    assert (out["c_start"], out["c_end"], out["c_edit_type"]) == (80, 83, "delins")
    assert out["c_inserted"] == "TGCTGTAAACTGTAACTGTAAA"
    assert (out["p_reference"], out["p_position"]) == ("R", 27)
    assert (out["p_end_reference"], out["p_end_position"]) == ("I", 28)
    assert (out["p_edit_type"], out["p_inserted"]) == ("delins", "LeuLeuTer")


def test_hgvs_name_frameshift_and_deletion(genomics):
    out = genomics["parse_hgvs_name"]("NM_014855.3(AP5Z1):c.1413_1426del (p.Leu473fs)")
    assert out["c_edit_type"] == "deletion"
    assert (out["p_edit_type"], out["p_position"], out["p_alternate"]) == (
        "frameshift",
        473,
        None,
    )


def test_hgvs_name_missense_and_nonsense(genomics):
    missense = genomics["parse_hgvs_name"](
        "NM_014630.3(ZNF592):c.3136G>A (p.Gly1046Arg)"
    )
    assert missense["c_edit_type"] == "substitution"
    assert (missense["c_reference"], missense["c_alternate"]) == ("G", "A")
    assert (missense["p_edit_type"], missense["p_alternate"]) == ("missense", "R")
    nonsense = genomics["parse_hgvs_name"](
        "NM_017547.4(FOXRED1):c.694C>T (p.Gln232Ter)"
    )
    assert (nonsense["p_edit_type"], nonsense["p_alternate"]) == ("nonsense", "*")


def test_hgvs_name_without_a_protein_part_is_ok(genomics):
    out = genomics["parse_hgvs_name"]("NM_000546.6(TP53):c.-30+1G>A")
    assert out["parse_status"] == "ok"
    assert out["p_notation"] is None
    assert out["c_start"] == -30
    assert out["c_start_offset"] == 1
    assert out["c_region"] == "intronic"


def test_hgvs_name_with_an_unreadable_protein_part_is_partial(genomics):
    out = genomics["parse_hgvs_name"]("NM_000001.1(GENE1):c.5A>G (p.?)")
    assert out["parse_status"] == "partial"
    assert out["c_edit_type"] == "substitution"


def test_hgvs_name_of_a_copy_number_variant_is_unparsed(genomics):
    out = genomics["parse_hgvs_name"]("GRCh38/hg38 1p36.33(chr1:1-100)x3")
    assert out["parse_status"] == "unparsed"
    assert out["c_notation"] is None


@pytest.mark.parametrize(
    ("name", "edit"),
    [
        ("NM_000145.4(FSHR):c.1A>G (p.Met1Val)", "missense"),  # a symbol with fs
        ("NM_000207.3(INS):c.188G>A (p.Gly63Asp)", "missense"),  # a symbol with ins
    ],
)
def test_a_gene_symbol_that_looks_like_an_edit_does_not_change_the_edit(
    genomics, name, edit
):
    assert genomics["parse_hgvs_name"](name)["p_edit_type"] == edit


@pytest.mark.parametrize(
    ("text", "region", "start", "offset"),
    [
        ("c.-71C>A", "five_prime_utr", -71, None),
        ("c.*45G>A", "three_prime_utr", 45, None),
        ("c.123+5G>A", "intronic", 123, 5),
        ("c.123-2A>G", "intronic", 123, -2),
        ("c.100C>T", "coding", 100, None),
    ],
)
def test_hgvs_coding_regions(genomics, text, region, start, offset):
    out = genomics["parse_hgvs_coding"](text)
    assert (out["c_region"], out["c_start"], out["c_start_offset"]) == (
        region,
        start,
        offset,
    )


def test_hgvs_coding_of_a_dot_or_empty_text_is_all_none(genomics):
    assert set(genomics["parse_hgvs_coding"](".").values()) == {None}
    assert set(genomics["parse_hgvs_coding"](None).values()) == {None}


@pytest.mark.parametrize(
    ("text", "edit", "reference", "position", "alternate"),
    [
        ("p.R835W", "missense", "R", 835, "W"),
        ("p.R100*", "nonsense", "R", 100, "*"),
        ("p.Leu10=", "synonymous", "L", 10, "L"),
        ("p.Met1?", "unknown", "M", 1, None),
        ("p.Lys2del", "deletion", "K", 2, None),
        ("p.Lys2_Gly4dup", "duplication", "K", 2, None),
        ("p.Arg123Profs*45", "frameshift", "R", 123, "P"),
        ("p.Ter100Glnext*?", "extension", "*", 100, None),
        ("p.(Arg27Gly)", "missense", "R", 27, "G"),
    ],
)
def test_hgvs_protein_edit_types(genomics, text, edit, reference, position, alternate):
    out = genomics["parse_hgvs_protein"](text)
    assert (
        out["p_edit_type"],
        out["p_reference"],
        out["p_position"],
        out["p_alternate"],
    ) == (edit, reference, position, alternate)


def test_hgvs_protein_that_is_not_notation_is_all_none(genomics):
    assert set(genomics["parse_hgvs_protein"](".").values()) == {None}


def test_one_letter_amino_acid(genomics):
    code = genomics["one_letter_amino_acid"]
    assert (code("Arg"), code("Ter"), code("R"), code("*")) == ("R", "*", "R", "*")
    assert code(None) is None
    assert code("Xyz") is None


def test_tcga_barcode(genomics):
    out = genomics["parse_tcga_barcode"]("TCGA-NH-A5IV-01A-42D-A36X-10")
    assert out["parse_status"] == "ok"
    assert out["participant_barcode"] == "TCGA-NH-A5IV"
    assert out["sample_barcode"] == "TCGA-NH-A5IV-01A"
    assert (out["sample_type_code"], out["vial"]) == ("01", "A")
    assert (out["portion"], out["analyte"]) == ("42", "D")
    assert (out["plate"], out["center"]) == ("A36X", "10")


def test_a_short_tcga_barcode_is_partial_and_a_foreign_one_is_unparsed(genomics):
    short = genomics["parse_tcga_barcode"]("TCGA-NH-A5IV-01A")
    assert short["parse_status"] == "partial"
    assert short["portion"] is None
    assert genomics["parse_tcga_barcode"]("XYZ-1-2-3")["parse_status"] == "unparsed"
    assert genomics["parse_tcga_barcode"](None)["parse_status"] == "unparsed"


def test_bronze_table_name(genomics):
    assert genomics["bronze_table_name"]("ncbi_gene.gene_info") == "ncbi_gene_gene_info"


def test_xref_pairs_keep_later_colons_in_the_value(genomics):
    pairs = genomics["parse_xref_pairs"](
        "MIM:138670|HGNC:HGNC:5|Ensembl:ENSG00000121410|AllianceGenome:HGNC:5"
    )
    assert pairs == [
        ("MIM", "138670"),
        ("HGNC", "HGNC:5"),
        ("Ensembl", "ENSG00000121410"),
        ("AllianceGenome", "HGNC:5"),
    ]


def test_xref_pairs_of_a_comma_list_and_of_bad_input(genomics):
    pairs = genomics["parse_xref_pairs"](
        "Comparative Toxicogenomics Database:1, NCBI Gene:1, no colon, :x, y:",
        ",",
    )
    assert pairs == [("Comparative Toxicogenomics Database", "1"), ("NCBI Gene", "1")]
    assert genomics["parse_xref_pairs"](None) == []


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("NCBI Gene", "ncbi_gene"),
        ("MIM", "omim"),
        ("OMIM", "omim"),
        ("RefSeq RNA", "refseq_rna"),
        ("UniProtKB", "uniprot"),
        ("Alliance-Genome", "alliance_genome"),
        ("", None),
        (None, None),
    ],
)
def test_normalise_id_type_uses_the_rule_aliases(genomics, rules, label, expected):
    assert genomics["normalise_id_type"](label, rules["id_type_aliases"]) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("5", "HGNC:5"),
        ("HGNC:5", "HGNC:5"),
        ("HGNC:HGNC:5", "HGNC:5"),
        (" HGNC:20619 ", "HGNC:20619"),
        ("not an id", "not an id"),
        (None, None),
    ],
)
def test_normalise_hgnc(genomics, value, expected):
    assert genomics["normalise_hgnc"](value) == expected


def test_a_json_empty_list_is_a_placeholder(rules):
    assert "[]" in rules["placeholders"]["general"]


def test_the_entity_master_declares_each_field_of_all_five_types(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["entity_master"]}
    for fields in genomics["MASTER_FIELDS"].values():
        for field in fields:
            assert {field, f"{field}_source", f"{field}_method"} <= columns
    assert set(genomics["MASTER_FIELDS"]) == {
        "gene",
        "transcript",
        "protein",
        "disease",
        "drug",
    }
    assert {
        "entity_type",
        "entity_key",
        "key_source",
        "grch38_contig",
        "cytoband_arm",
    } <= columns


def test_the_flat_relation_table_names_its_kinds_and_publications(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["entity_relation"]}
    assert {"relation_kind", "relation_subkind", "pubmed_id"} <= columns
    assert {f"channel_{c}_score" for c in genomics["STRING_CHANNELS"]} <= columns
    assert {"subject_id", "member_id", "member_key", "variation_interpreted"} <= columns


def test_every_variant_source_has_a_flag_column(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["variant"]}
    assert set(genomics["VARIANT_SOURCE_FLAGS"].values()) <= columns


def test_every_gene_field_has_a_precedence_list_or_is_unranked(genomics, rules):
    ranked = set(rules["precedence"]["gene"])
    unranked = {"chromosome", "map_location"}
    assert set(genomics["GENE_FIELDS"]) == ranked | unranked


def test_all_thirty_one_tables_have_a_declared_structure(genomics):
    declared = set(genomics["GENOMICS_STRUCTURES"])
    tables = genomics["GENOMICS_REFERENCE_TABLES"] | genomics["GENOMICS_MAIN_TABLES"]
    assert declared == tables


def test_the_variant_master_declares_each_field_with_source_and_method(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["variant"]}
    for field in genomics["VARIANT_FIELDS"]:
        assert {field, f"{field}_source", f"{field}_method"} <= columns
    assert {"variant_key", "key_source", "assembly", "position"} <= columns


def test_every_ranked_variant_field_is_a_declared_variant_field(genomics, rules):
    ranked = set(rules["precedence"]["variant"]) - {"coordinate"}
    assert ranked <= set(genomics["VARIANT_FIELDS"])


def test_the_frequency_bands_rise_and_end_in_common(rules):
    limits = [band["below"] for band in rules["frequency_bands"]]
    assert limits == sorted(limits)
    assert [band["label"] for band in rules["frequency_bands"]] == [
        "ultra_rare",
        "rare",
        "low_frequency",
    ]
    assert rules["frequency_band_default"] == "common"


def test_clinvar_writes_minus_one_for_no_dbsnp_id_and_no_gene(rules):
    by_column = rules["placeholders"]["by_column"]
    assert by_column["clinvar.variant_summary.RS_dbSNP"] == ["-1"]
    assert by_column["clinvar.variant_summary.GeneID"] == ["-1"]


@pytest.mark.parametrize(
    "column",
    [
        "dbvar.variants.QUAL",
        "dbvar.variants.SVLEN",
        "thousand_genomes.variants.QUAL",
        "gencode.annotation.score",
        "refseq.annotation.score",
    ],
)
def test_the_vcf_and_gff_missing_value_is_a_placeholder(rules, column):
    assert rules["placeholders"]["by_column"][column] == ["."]


def test_the_gene_ages_none_text_is_a_placeholder(rules):
    by_column = rules["placeholders"]["by_column"]
    assert by_column["gene_ages.consensus.NodeError"] == ["None"]


def test_proteingym_clinical_labels_map_to_one_and_zero(rules):
    labels = rules["proteingym_clinical_label_numbers"]
    assert labels == {"Pathogenic": 1, "Benign": 0}


@pytest.mark.parametrize(
    ("text", "is_number"),
    [
        ("12", True),
        ("-1.5", True),
        (".5", True),
        ("5.", True),
        ("1e-05", True),
        ("1,013.39", False),
        (".", False),
        ("NaN", False),
    ],
)
def test_the_number_text_pattern(genomics, text, is_number):
    assert bool(re.fullmatch(genomics["NUMBER_TEXT"], text)) is is_number


def test_alphamissense_genome_names_map_to_assembly_labels(rules):
    assert rules["ucsc_assembly_names"] == {"hg38": "GRCh38", "hg19": "GRCh37"}


def test_the_frequency_bands_keep_the_old_cutoffs(rules):
    assert [(band["label"], band["below"]) for band in rules["frequency_bands"]] == [
        ("ultra_rare", 0.001),
        ("rare", 0.01),
        ("low_frequency", 0.05),
    ]


def test_the_conservation_cutoffs_are_the_old_ones(rules):
    assert rules["conservation"] == {
        "phylop_conserved_above": 2.7,
        "gerp_constrained_above": 4.0,
        "cadd_deleterious_above": 20.0,
        "allele_frequency_common_above": 0.01,
        "allele_frequency_rare_below": 0.001,
    }


def test_the_old_gene_importance_weights_are_in_the_rules(rules):
    importance = rules["gene_importance"]
    assert [(e["from"], e["points"]) for e in importance["disease_count_points"]] == [
        (50, 10),
        (20, 8),
        (10, 5),
        (5, 3),
        (1, 2),
    ]
    assert importance["function_keyword_points"] == 2
    assert importance["cancer_points"] == 3
    assert [e["label"] for e in rules["gene_annotation_quality"]] == [
        "Excellent",
        "Good",
        "Moderate",
    ]


def test_relations_carry_the_source_detail_columns(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["entity_relation"]}
    assert {
        "standard_operating_procedure",
        "novelty_score",
        "clinpgx_level",
        "phenotype_name",
        "official_symbol_a",
        "refseq_accessions_b",
    } <= columns


def test_ontology_relations_carry_the_medgen_ids(genomics):
    columns = {name for name, _ in genomics["GENOMICS_STRUCTURES"]["ontology_relation"]}
    assert {"relation_id", "parent_atom_id", "child_atom_id"} <= columns


# --- the registry rows that the generator builds from the Silver structures ----

_SHORTHAND = {"ags", "eeg", "kwk", "chp", "tso", "pv", "sme", "nuts2", "ts", "alt"}
_SHORTHAND |= {"src", "ord", "elec", "hg"}


@pytest.fixture(scope="module")
def registry_rows():
    return build_rows()


def test_every_semantic_structure_is_classified_column_for_column(
    registry_rows,
) -> None:
    """SEMANTIC_STRUCTURES is the declared inventory; the registry must carry
    exactly its columns, in the schema allocated to each structure."""
    ns = _semantic_namespace()
    for table, columns in ns["SEMANTIC_STRUCTURES"].items():
        mine = [r for r in registry_rows if r["table_name"] == table]
        assert {r["column_name"] for r in mine} == {c for c, _ in columns}, table
        expected = ns["semantic_target_schema"](table)
        assert {r["target_schema"] for r in mine} == {expected}, table


@pytest.mark.parametrize("layer", ["semantic", "genomics"])
def test_no_declared_column_is_an_array_struct_or_map(layer, genomics) -> None:
    """Silver is flat: a closed list of values is columns, a many-side is rows."""
    structures = (
        _semantic_namespace()["SEMANTIC_STRUCTURES"]
        if layer == "semantic"
        else genomics["GENOMICS_STRUCTURES"]
    )
    nested = {
        f"{table}.{column}": kind
        for table, columns in structures.items()
        for column, kind in columns
        if kind.startswith(("array", "struct", "map"))
    }
    assert not nested, nested


def test_the_energy_and_commerce_tables_number_36() -> None:
    ns = _semantic_namespace()
    tables = set(ns["SEMANTIC_STRUCTURES"]) | set(ns["SEMANTIC_MEMBER_STRUCTURES"])
    assert len(tables) == 36


def test_every_member_structure_carries_its_members_and_discriminators(
    registry_rows,
) -> None:
    ns = _semantic_namespace()
    by_table: dict[str, set[str]] = {}
    for r in registry_rows:
        by_table.setdefault(r["table_name"], set()).add(r["column_name"])
    for table, (members, discriminators) in ns["SEMANTIC_MEMBER_STRUCTURES"].items():
        member_cols = set().union(*(by_table[m] for m in members)) - {"ecosystem"}
        assert by_table[table] == member_cols | {*discriminators, "source_dataset"}


def test_generator_build_rows_covers_every_written_table(registry_rows) -> None:
    """The live mechanism 00_silver_setup.py actually runs. This is what makes
    a newly added Silver notebook self-registering: no seed file exists."""
    produced = {r["table_name"] for r in registry_rows}
    missing: dict[str, set[str]] = {}
    for nb in silver_notebooks(_SILVER):
        targets = write_silver_targets(ast.parse(nb.read_text(encoding="utf-8")))
        gap = targets - produced
        if gap:
            missing[nb.name] = gap
    assert not missing, f"Silver tables build_rows() does not produce: {missing}"


def test_assert_registry_complete_fails_on_a_missing_table(registry_rows) -> None:
    """Proves the fail-early mechanism itself, not just today's repo state --
    an incomplete registry must raise, never be silently accepted."""
    incomplete_rows = [
        r for r in registry_rows if r["table_name"] != "reference_code_list"
    ]
    with pytest.raises(RuntimeError, match="reference_code_list"):
        assert_registry_complete(incomplete_rows)


def test_assert_registry_complete_passes_on_the_real_repo(registry_rows) -> None:
    assert_registry_complete(registry_rows)


def test_silver_columns_are_english_and_spelled_out(registry_rows) -> None:
    """Source-style (uppercase) names and abbreviations must not reach a Silver
    structure; a MaStR `_nv` flag arrives as `<English field>_not_available`."""
    ns = _semantic_namespace()
    tables = set(ns["SEMANTIC_STRUCTURES"]) | set(ns["SEMANTIC_MEMBER_STRUCTURES"])
    untranslated: dict[str, list[str]] = {}
    for r in registry_rows:
        col = r["column_name"]
        if r["table_name"] not in tables:
            continue
        german = col != col.lower()
        if german or _SHORTHAND & set(col.split("_")):
            untranslated.setdefault(r["table_name"], []).append(col)
    assert not untranslated, untranslated


def test_mastr_not_available_flags_get_english_names(registry_rows) -> None:
    columns = {r["column_name"] for r in registry_rows}
    assert "house_number_not_available" in columns
    assert not any(c.endswith("_nv") for c in columns)


def test_every_genomics_table_is_classified_in_its_own_schema(registry_rows):
    ns = _genomics_namespace()
    tables = ns["GENOMICS_REFERENCE_TABLES"] | ns["GENOMICS_MAIN_TABLES"]
    for table in tables:
        mine = [r for r in registry_rows if r["table_name"] == table]
        columns = {r["column_name"] for r in mine}
        declared = {c for c, _ in ns["GENOMICS_STRUCTURES"].get(table, [])}
        assert columns == declared | _GOVERNANCE, table
        expected = ns["genomics_target_schema"](table)
        assert {r["target_schema"] for r in mine} == {expected}, table


def test_the_registry_places_twelve_and_twenty_eight_genomics_tables(registry_rows):
    per_schema: dict[str, set[str]] = {}
    for r in registry_rows:
        per_schema.setdefault(r["target_schema"], set()).add(r["table_name"])
    assert len(per_schema["genomics_silver_reference"]) == 12
    assert len(per_schema["genomics_silver"]) == 28


def test_every_gold_read_silver_target_is_registered(registry_rows) -> None:
    """Static emulation of 00_gold_setup.py's preflight -- every table a Gold
    notebook's read_silver() names must be produced by the field-class
    generator, or the real preflight would (correctly) hard-fail before any
    Gold processing starts."""
    produced = {r["table_name"] for r in registry_rows}
    required = all_read_silver_tables(_GOLD)
    missing = sorted(required - produced)
    assert not missing, f"Gold reads unregistered Silver table(s): {missing}"


def test_gold_preflight_would_reject_an_unregistered_dependency(registry_rows) -> None:
    """Proves the preflight's own comparison logic, not just today's repo
    state -- an unregistered Gold dependency must be detectable as a gap."""
    produced = {r["table_name"] for r in registry_rows}
    required = all_read_silver_tables(_GOLD) | {"a_table_nobody_registered"}
    missing = sorted(required - produced)
    assert missing == ["a_table_nobody_registered"]


# --- the Gold library -----------------------------------------------------------

_SILVER_08 = _SILVER / "genomics" / "08_derived_attributes.py"


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


def test_gold_run_ids_tell_the_stage_apart_and_never_look_like_a_silver_run_id(gold):
    # gold_run_id() and the Silver run_id() layered in underneath it must never
    # collide in prefix, or a run_id alone stops telling you which stage made it.
    rid = gold["gold_run_id"]()
    assert re.match(r"^gold-\d{8}T\d{6}Z$", rid)
    assert gold["stage_of"](rid) == "gold"
    assert gold["stage_of"](gold["analytics_run_id"]()) == "analytics"
    assert rid.split("-", 1)[0] != gold["run_id"]().split("-", 1)[0]


# --- the EDA library -------------------------------------------------------------


@pytest.fixture(scope="module")
def eda():
    return load_eda_common()


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
def test_the_ml_target_kind_follows_the_type_and_the_values(
    eda, dtype, low, high, distinct, kind
):
    assert eda["ml_target_kind"](dtype, low, high, distinct) == kind


def _days(*offsets):
    return [dt.date(2024, 1, 1) + dt.timedelta(days=o) for o in offsets]


def test_date_gaps_count_the_missing_steps_the_longest_gap_and_the_own_step(eda):
    found = eda["date_gaps"](
        {
            "train": _days(0, 1, 2, 5, 6),
            "test": _days(0, 1, 2, 3),
            "single": _days(0),
            "yearly": [dt.date(y, 1, 1) for y in (2019, 2020, 2021, 2023)],
        }
    )
    assert found["train"] == {"dates": 5, "step": 1, "missing": 2, "longest": 3}
    assert (found["test"]["missing"], found["test"]["longest"]) == (0, 1)
    assert found["single"] == {"dates": 1, "step": None, "missing": 0, "longest": 0}
    assert found["yearly"]["missing"] == 1


def test_the_ml_eda_blocks_hold_the_tables_of_the_results(eda):
    assert eda["batches"](range(5), 2) == [[0, 1], [2, 3], [4]]
    rows = [("a", "feature"), ("t", "target"), ("b", "feature")]
    assert eda["ml_roles"](rows) == {"feature": ["a", "b"], "target": ["t"]}
    base = {"target": "t", "kind": "binary", "features": 2, "numbers": 2}
    base.update({"dominant": {}, "pairs": [], "checked": 2})
    result = {
        **base,
        "partitions": {
            "train": {"rows": 10, "labelled": 9, "mean": 0.5, "std": 0.1, "zero": 0.5}
        },
        "shift": {"a": {"test": (1.5, 0.2)}, "b": {}},
        "association": [("a", 0.95)],
        "above_limit": 1,
        "gaps": {"train": {"dates": 3, "step": 1, "missing": 1, "longest": 2}},
    }
    blocks = dict(
        eda["ml_eda_blocks"](
            {"demo": result, "bare": {**base, "target": None, "kind": None}}
        )
    )
    assert "| demo | binary | train | 10 | 9 |" in blocks["Target by partition"]
    shift = blocks["Feature shift from train (ten largest per dataset)"]
    assert "`a` | test | 1.5 | 0.2" in shift
    assert "demo: 1" in blocks["ML-Readiness Evidence"]
    assert "['demo']" in blocks["ML-Readiness Evidence"]
    assert blocks["Datasets without a readable target"] == "bare"
