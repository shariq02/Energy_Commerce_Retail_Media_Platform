"""Unit tests for the approval libraries under `databricks/models/lib`.

The rule engine and the texts are plain Python; each test builds the recorded
results of one task by hand and checks the recommendation."""

import datetime as dt
import json
import re

import pytest

from tests._notebook_loader import load_approval_libs

pytestmark = [pytest.mark.schema, pytest.mark.unit]

SPEC = {
    "degradation_relative": 0.2,
    "min_rows": 30,
    "min_blocks": 5,
    "coverage_target": 0.8,
    "coverage_tolerance": 0.1,
}
GOOD = {
    "skill_primary_ci_low": 0.1,
    "skill_primary_ci_high": 0.3,
    "primary_change_relative": 0.05,
    "bootstrap_blocks": 20.0,
}


@pytest.fixture(scope="module")
def lib():
    return load_approval_libs()


def _rows(task, model, stage="candidate", n_rows=1000, status="ok", **metrics):
    return [
        {
            "task_id": task,
            "model_name": model,
            "stage": stage,
            "status": status,
            "detail": "failed to load" if status != "ok" else None,
            "metric": name,
            "value": value,
            "validation_value": None,
            "n_rows": n_rows,
            "frozen_delta_version": 0,
            "mlflow_run_id": "run-1",
        }
        for name, value in (metrics or {None: None}).items()
    ]


def _sel(task, model, rank, baseline=False):
    return {
        "task_id": task,
        "model_name": model,
        "rank": rank,
        "is_baseline": baseline,
        "mlflow_run_id": f"run-{rank}",
        "frozen_delta_version": 0,
    }


def _decide(lib, task_id, cand=None, base=None, reads=1, **cand_metrics):
    task = lib["TASK_BY_ID"][task_id]
    metrics = {**GOOD, **cand_metrics}
    rows = _rows(task_id, "cand", **metrics, **(cand or {}))
    base_rows = _rows(task_id, task["baseline"], "baseline", **(base or {"mae": 1.0}))
    return lib["decide_selected"](task, rows, base_rows, SPEC, reads)


def _rule_ids(decision):
    return {r["rule"] for r in decision["rules"]}


def test_a_clean_candidate_is_approved(lib):
    out = _decide(lib, "load.load")
    assert out["decision"] == "approved"
    assert out["restriction"] is None and out["conditions"] == []


def test_skill_interval_that_includes_zero_is_conditional(lib):
    out = _decide(lib, "load.load", skill_primary_ci_low=-0.02)
    assert out["decision"] == "approved_with_conditions"
    assert "R02" in _rule_ids(out)


def test_skill_interval_wholly_below_zero_is_not_approved(lib):
    out = _decide(
        lib, "load.load", skill_primary_ci_low=-0.5, skill_primary_ci_high=-0.1
    )
    assert out["decision"] == "not_approved"
    assert "R03" in _rule_ids(out)


def test_missing_skill_interval_is_conditional(lib):
    task = lib["TASK_BY_ID"]["load.load"]
    rows = _rows(
        "load.load", "cand", bootstrap_blocks=20.0, primary_change_relative=0.0
    )
    out = lib["decide_selected"](task, rows, [], SPEC, 1)
    assert out["decision"] == "approved_with_conditions"


def test_degradation_above_the_limit_is_conditional_and_the_limit_itself_is_not(lib):
    over = _decide(lib, "load.load", primary_change_relative=0.35)
    at = _decide(lib, "load.load", primary_change_relative=0.2)
    assert over["decision"] == "approved_with_conditions" and "R04" in _rule_ids(over)
    assert at["decision"] == "approved"


def test_interval_coverage_outside_the_tolerance_is_conditional(lib):
    low = _decide(lib, "price_daily.price", interval_coverage_80=0.57)
    inside = _decide(lib, "price_daily.price", interval_coverage_80=0.72)
    assert "R08" in _rule_ids(low) and low["decision"] == "approved_with_conditions"
    assert "R08" not in _rule_ids(inside) and inside["decision"] == "approved"


@pytest.mark.parametrize(
    ("cand", "metrics"),
    [
        ({"n_rows": 10}, {}),
        ({}, {"bootstrap_blocks": 3.0}),
    ],
)
def test_evidence_below_the_floor_is_deferred(lib, cand, metrics):
    out = _decide(lib, "load.load", cand=cand, **metrics)
    assert out["decision"] == "deferred"
    assert "R05" in _rule_ids(out)


def test_a_thin_metric_tier_defers_the_task(lib):
    out = _decide(
        lib,
        "redispatch_matching.tier",
        n_true__exact_normalised=4.0,
        n_true__unmatched=139.0,
    )
    assert out["decision"] == "deferred"
    assert "exact_normalised" in out["rules"][0]["detail"]


def test_a_task_whose_tiers_all_pass_is_not_deferred(lib):
    out = _decide(
        lib,
        "redispatch_matching.tier",
        n_true__exact_normalised=60.0,
        n_true__unmatched=139.0,
    )
    assert out["decision"] != "deferred"


def test_too_few_positive_rows_defer_a_classification_task(lib):
    out = _decide(lib, "price_quarter_hour.negative_price", base_rate=0.001)
    assert out["decision"] == "deferred"
    assert "positive" in out["rules"][0]["detail"]


def test_a_model_that_was_not_evaluated_is_deferred(lib):
    task = lib["TASK_BY_ID"]["load.load"]
    rows = _rows("load.load", "cand", status="failed")
    out = lib["decide_selected"](task, rows, [], SPEC, 1)
    assert out["decision"] == "deferred"
    out = lib["decide_selected"](task, [], [], SPEC, 1)
    assert out["decision"] == "deferred"


def test_no_ground_truth_gives_diagnostic_use_only(lib):
    out = _decide(lib, "weak_supervision.labels")
    assert out["decision"] == "approved_with_conditions"
    assert out["restriction"] == "diagnostic_only" and "R06" in _rule_ids(out)


def test_offline_reinforcement_learning_is_offline_only_with_the_energy_figures(lib):
    out = _decide(
        lib,
        "rl_pumped_storage.policy",
        episode_net_abs_policy=4.4e4,
        episode_net_abs_logged=1.26e4,
    )
    assert out["restriction"] == "offline_only" and "R09" in _rule_ids(out)
    assert any("net energy per episode" in c for c in out["conditions"])


def test_injected_anomalies_are_offline_only(lib):
    out = _decide(lib, "honda_anomaly.anomaly")
    assert out["restriction"] == "offline_only"


def test_a_segment_with_negative_skill_is_excluded_not_the_task(lib):
    out = _decide(
        lib,
        "bias.bias",
        mae__forecast_scope__offshore_wind=9339.0,
        mae__forecast_scope__other=1.2e4,
        base={
            "mae": 1.0,
            "mae__forecast_scope__offshore_wind": 8449.0,
            "mae__forecast_scope__other": 4.4e5,
        },
    )
    by = {s["segment"]: s for s in out["segments"]}
    assert by["offshore_wind"]["decision"] == "not_approved"
    assert by["offshore_wind"]["skill"] == pytest.approx(1 - 9339 / 8449)
    assert by["other"]["decision"] == "approved_with_conditions"
    assert out["decision"] == "approved_with_conditions"
    assert "R07" in _rule_ids(out)


def test_every_segment_negative_is_not_approved(lib):
    out = _decide(
        lib,
        "honda_forecast.increment",
        mae__channel__total=40.0,
        base={"mae": 1.0, "mae__channel__total": 30.0},
    )
    assert out["decision"] == "not_approved"


def test_recorded_segment_skill_is_read_for_weather_variables(lib):
    out = _decide(
        lib,
        "weather_imputation.reconstruction",
        skill_mae__weather__air_temperature=0.93,
        skill_mae__weather__wind_direction=-0.078,
    )
    by = {s["segment"]: s["decision"] for s in out["segments"]}
    assert by == {
        "air_temperature": "approved_with_conditions",
        "wind_direction": "not_approved",
    }


def test_repeated_reads_are_disclosed_and_change_no_decision(lib):
    out = _decide(lib, "load.load", reads=7)
    assert out["decision"] == "approved"
    assert "R10" in _rule_ids(out)
    assert any(
        "at least 7 evaluation runs" in n and "not recoverable" in n
        for n in out["notes"]
    )


def test_task_disclosures_are_attached(lib):
    out = _decide(lib, "next_item_ga4.next_item")
    assert any("only baseline" in n for n in out["notes"])


def test_survival_is_conditional_with_the_gap_the_event_count_and_a_revalidation(lib):
    out = _decide(
        lib,
        "survival.unit_lifetime",
        calibration_gap_5y=0.0036,
        base={"concordance": 0.5, "calibration_gap_5y": 0.00004},
    )
    assert out["decision"] == "approved_with_conditions"
    text = " | ".join(out["conditions"])
    assert "5-year calibration gap 0.0036 against 4e-05" in text
    assert "number of events in the held-out partition was not recorded" in text
    assert "revalidate on a new frozen dataset version" in text


def test_survival_without_a_calibration_gap_keeps_the_other_conditions(lib):
    out = _decide(lib, "survival.unit_lifetime")
    assert out["decision"] == "approved_with_conditions"
    assert len(out["conditions"]) == 2


def test_positive_segment_skill_without_an_interval_is_conditional(lib):
    out = _decide(
        lib,
        "weather_imputation.reconstruction",
        skill_mae__weather__air_temperature=0.93,
    )
    assert out["decision"] == "approved_with_conditions"
    assert any(
        "no uncertainty interval" in c and "air_temperature (0.93)" in c
        for c in out["conditions"]
    )


def test_the_selected_model_follows_the_validation_rank(lib):
    task_id = "load.load"
    task = lib["TASK_BY_ID"][task_id]
    selection = [
        _sel(task_id, task["baseline"], 0, True),
        _sel(task_id, "second", 2),
        _sel(task_id, "first", 1),
    ]
    results = (
        _rows(task_id, "first", **{**GOOD, "skill_primary_ci_low": -0.1})
        + _rows(task_id, "second", **GOOD)
        + _rows(task_id, task["baseline"], "baseline", mae=1.0)
    )
    rows = lib["recommend_task"](task, selection, results, SPEC, 1)
    by_role = {r["role"]: r for r in rows}
    assert by_role["selected"]["model_name"] == "first"
    assert by_role["selected"]["decision"] == "approved_with_conditions"
    assert by_role["runner_up"]["model_name"] == "second"
    assert by_role["runner_up"]["decision"] == "not_approved"
    assert by_role["runner_up"]["rules"][0]["rule"] == "R01"
    assert by_role["baseline_fallback"]["model_name"] == task["baseline"]
    assert by_role["baseline_fallback"]["decision"] == "approved"


def test_a_baseline_recorded_under_another_name_is_found(lib):
    task_id = "weather_imputation.reconstruction"
    task = lib["TASK_BY_ID"][task_id]
    selection = [_sel(task_id, "causal_baseline", 0, True), _sel(task_id, "cand", 1)]
    results = _rows(task_id, "cand", **GOOD) + _rows(
        task_id, "causal_baseline", "baseline", skill_mae_mean=0.0
    )
    rows = lib["recommend_task"](task, selection, results, SPEC, 1)
    fallback = next(r for r in rows if r["role"] == "baseline_fallback")
    assert fallback["model_name"] == "causal_baseline"


def test_several_baselines_without_the_registered_name_raise(lib):
    task_id = "weather_imputation.reconstruction"
    task = lib["TASK_BY_ID"][task_id]
    selection = [
        _sel(task_id, "one", 0, True),
        _sel(task_id, "two", 0, True),
        _sel(task_id, "cand", 1),
    ]
    with pytest.raises(ValueError, match="none named"):
        lib["recommend_task"](task, selection, [], SPEC, 1)


def test_the_registered_baseline_wins_among_several(lib):
    task_id = "price_daily.price"
    task = lib["TASK_BY_ID"][task_id]
    selection = [
        _sel(task_id, "empirical_quantiles", 0, True),
        _sel(task_id, task["baseline"], 0, True),
        _sel(task_id, "cand", 1),
    ]
    results = _rows(task_id, "cand", **GOOD)
    rows = lib["recommend_task"](task, selection, results, SPEC, 1)
    fallback = next(r for r in rows if r["role"] == "baseline_fallback")
    assert fallback["model_name"] == task["baseline"]


def test_a_task_without_a_candidate_raises(lib):
    task = lib["TASK_BY_ID"]["load.load"]
    only_base = [_sel("load.load", task["baseline"], 0, True)]
    with pytest.raises(ValueError, match="no forwarded candidate"):
        lib["recommend_task"](task, only_base, [], SPEC, 1)


def test_the_approval_settings_are_typed_and_must_be_recorded(lib):
    rows = [
        {"parameter": p, "parameter_value": v}
        for p, v in lib["APPROVAL_DEFAULTS"].items()
    ]
    spec = lib["spec_from_rows"](rows)
    assert spec == SPEC
    with pytest.raises(RuntimeError, match="not recorded"):
        lib["spec_from_rows"](rows[:-1])


def test_the_rule_table_has_every_rule_once_with_its_class(lib):
    ids = {r[0] for r in lib["APPROVAL_RULES"]}
    assert ids == {f"R{i:02d}" for i in range(1, 11)}
    classes = {r[0]: r[1] for r in lib["APPROVAL_RULES"]}
    assert classes["R03"] == classes["R05"] == "blocking"


def _table_rows(lib, recommended):
    columns = [c.split()[0] for c in lib["MODEL_DDL"]["model_approval"].split(", ")]
    now = dt.datetime(2026, 10, 4, tzinfo=dt.UTC)
    tuples = lib["approval_tuples"](recommended, "run-x", now)
    assert all(len(t) == len(columns) for t in tuples)
    return [dict(zip(columns, t, strict=True)) for t in tuples]


def _recommended(lib, task_id="load.load", model="cand", **cand):
    task = lib["TASK_BY_ID"][task_id]
    selection = [_sel(task_id, task["baseline"], 0, True), _sel(task_id, model, 1)]
    results = _rows(task_id, model, **{**GOOD, **cand}) + _rows(
        task_id, task["baseline"], "baseline", mae=1.0, mfinite=1.0
    )
    return lib["recommend_task"](task, selection, results, SPEC, 1), results


def test_table_rows_follow_the_table_columns(lib):
    recommended, _results = _recommended(lib)
    rows = _table_rows(lib, recommended)
    assert {r["role"] for r in rows} == {"selected", "baseline_fallback"}
    assert all(r["decided_by"] == "rule_engine" and not r["overridden"] for r in rows)
    assert json.loads(rows[0]["rules"]) == recommended[0]["rules"]


def test_findings_text_lists_rules_decisions_and_overrides(lib):
    recommended, _results = _recommended(lib, primary_change_relative=0.5)
    rows = _table_rows(lib, recommended)
    rows[0]["overridden"] = True
    rows[0]["override_reason"] = "owner accepts the drift"
    spec = [
        {
            "rule_id": r,
            "rule_class": c,
            "description": d,
            "parameter": p,
            "parameter_value": v,
        }
        for r, c, d, p, v in lib["APPROVAL_RULES"]
    ]
    checks = [
        {
            "component": "models/approve/gate/01_approval_guards",
            "metric_name": "every_task_has_a_decision",
            "status": "PASS",
            "error_detail": "",
            "recorded_at": "2026-10-04",
        }
    ]
    text = lib["render_approval_findings"]("energy", rows, spec, checks, "stamp")
    assert text.startswith("# ENERGY APPROVAL FINDINGS")
    for part in ("## rules", "## summary", "## decisions", "## overrides"):
        assert part in text
    assert "owner accepts the drift" in text
    assert "## load.load" in text and "R04" in text
    assert "every_task_has_a_decision" in text


def _card(lib, text_extra=None, model="gbt_lightgbm", **cand):
    recommended, results = _recommended(lib, model=model, **cand)
    rows = _table_rows(lib, recommended)
    selected, baseline = rows[0], rows[-1]
    selected["recommended_decision"] = selected["decision"]
    notes = [
        {"read": {"partition": "held-out", "rows": 3666}},
        {"rows": 5000, "columns": 20, "rows_by_partition": {"train": 3000}},
    ]
    facts = {
        "results": results,
        "flags": [{"flag": "skill interval includes zero", "detail": "-0.02 to 0.3"}],
        "eval_context": [
            {
                "status": "data_ready",
                "data_notes": json.dumps(notes),
                "recorded_at": "2026-10-03",
            }
        ],
        "fit_context": [
            {
                "library_versions": json.dumps(
                    {"lightgbm": "4.7.0", "torch": "missing"}
                ),
                "recorded_at": "2026-10-02",
            }
        ],
        "params": "{'learning_rate': 0.1} /Users/someone@example.com/run",
        "extra": text_extra or [],
    }
    task = lib["TASK_BY_ID"]["load.load"]
    return lib["render_model_card"](task, selected, baseline, facts, SPEC, "stamp")


def test_the_model_card_has_every_section(lib):
    text = _card(lib)
    assert text.startswith("# Model card: load.load")
    for part in (
        "## Decision",
        "## Purpose and task",
        "## Intended use",
        "## Out of scope",
        "## Data",
        "## Training",
        "## Metrics",
        "## Flags",
        "## Conditions and known limits",
        "## Disclosures",
        "## Revalidation trigger",
        "## Baseline fallback",
    ):
        assert part in text
    assert "lightgbm 4.7.0" in text and "torch" not in text
    assert "held-out read: partition held-out, rows 3666" in text
    assert "20%" in text


def test_the_card_names_only_the_libraries_of_the_model(lib):
    assert "library versions: lightgbm 4.7.0\n" in _card(lib)
    assert "library versions: not recorded" in _card(lib, model="unknown_model")


def test_the_card_metric_table_hides_internal_fields(lib):
    text = _card(lib, bootstrap_resamples_valid=200.0, pred_std=1.0)
    table = text.split("## Metrics")[1].split("## Flags")[0]
    assert "skill_primary_ci_low" in table
    for hidden in ("bootstrap_", "pred_", "reproduc"):
        assert hidden not in table


def test_every_task_has_its_own_intended_use_and_a_model_library_entry(lib):
    texts = [lib["INTENDED_USE"][t["task_id"]] for t in lib["TASKS"]]
    assert len(set(texts)) == len(texts)
    assert "estimates the electricity load" in _card(lib).split("## Intended use")[1]


def test_the_model_card_carries_extra_facts_and_the_processor_note(lib):
    text = _card(lib, text_extra=["- events in the frozen data, train: 12 of 100 rows"])
    assert "events in the frozen data, train: 12 of 100 rows" in text
    assert "processor type: not recorded" in text


def test_the_model_card_holds_no_user_folder_email_or_planning_reference(lib):
    text = _card(lib)
    assert "someone@example.com" not in text and "/Users/someone" not in text
    assert not re.search(r"doc[s]/|ADR-\d|UC-\d|\bPhase \d|\bEntry \d{3}", text)


def test_a_segment_table_appears_on_the_card_when_segments_exist(lib):
    task_id = "bias.bias"
    task = lib["TASK_BY_ID"][task_id]
    selection = [_sel(task_id, task["baseline"], 0, True), _sel(task_id, "cand", 1)]
    results = _rows(
        task_id, "cand", **GOOD, mae__forecast_scope__offshore_wind=9339.0
    ) + _rows(
        task_id,
        task["baseline"],
        "baseline",
        mae__forecast_scope__offshore_wind=8449.0,
    )
    recommended = lib["recommend_task"](task, selection, results, SPEC, 1)
    rows = _table_rows(lib, recommended)
    rows[0]["recommended_decision"] = rows[0]["decision"]
    facts = {"results": results, "flags": [], "eval_context": [], "fit_context": []}
    text = lib["render_model_card"](task, rows[0], rows[-1], facts, SPEC, "stamp")
    assert "## Segments" in text and "offshore_wind" in text
