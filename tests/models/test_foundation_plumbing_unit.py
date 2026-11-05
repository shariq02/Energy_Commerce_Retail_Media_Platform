"""Unit tests for the foundation rules in the approval and registry libraries.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The rules R13 and R14, the ranking of a
foundation candidate among the forwarded candidates, the reason of a replacing
registry entry and the re-score tolerance are checked on constructed rows.
"""

from __future__ import annotations

import pytest

from tests._notebook_loader import load_foundation_libs, load_registry_libs

pytestmark = [pytest.mark.unit]

SPEC = {
    "degradation_relative": 0.2,
    "min_rows": 30,
    "min_blocks": 5,
    "coverage_target": 0.8,
    "coverage_tolerance": 0.1,
}


@pytest.fixture(scope="module")
def lib():
    return load_registry_libs()


def _rows(model, validation, evaluated, low=0.1, high=0.5):
    metrics = {
        "mae": (evaluated, validation),
        "skill_primary_ci_low": (low, None),
        "skill_primary_ci_high": (high, None),
        "primary_change_relative": (0.0, None),
    }
    return [
        {
            "model_name": model,
            "status": "ok",
            "detail": "",
            "metric": name,
            "value": value,
            "validation_value": val,
            "n_rows": 100,
        }
        for name, (value, val) in metrics.items()
    ]


def _selection(name, rank, family="tabular", baseline=False):
    return {
        "task_id": "load.load",
        "model_name": name,
        "family": family,
        "rank": rank,
        "is_baseline": baseline,
        "mlflow_run_id": f"run-{name}",
        "frozen_delta_version": 0,
    }


def _facts(**changes):
    facts = {
        "model_key": "chronos2",
        "repo_id": "amazon/chronos-2",
        "revision": "abc123def456",
        "licence": "apache-2.0",
        "mode": "zero_shot",
        "contamination": "unresolved",
        "contamination_note": "",
    }
    facts.update(changes)
    return facts


def _task(lib):
    return lib["TASK_BY_ID"]["load.load"]


def _recommend(lib, facts):
    selection = [
        _selection("seasonal_mean", 0, "baseline", True),
        _selection("gbt_lightgbm", 1),
        _selection("ridge", 2),
        _selection("chronos2_zero_shot", 3, "foundation_ts"),
    ]
    results = [
        *_rows("seasonal_mean", 140.0, 150.0),
        *_rows("gbt_lightgbm", 100.0, 105.0),
        *_rows("ridge", 120.0, 125.0),
        *_rows("chronos2_zero_shot", 90.0, 95.0),
    ]
    return lib["recommend_task"](
        _task(lib),
        selection,
        results,
        SPEC,
        1,
        {"chronos2_zero_shot": facts} if facts else None,
    )


def _roles(rows):
    return {(r["model_name"], r["role"]): r for r in rows}


# --- the names ------------------------------------------------------------------


def test_foundation_names_are_recognised_and_the_others_are_not(lib):
    ns = load_foundation_libs()
    names = [c[0] for c in ns["CANDIDATES"]] + [c[0] for c in ns["TAB_CANDIDATES"]]
    assert all(lib["is_foundation_model"](n) for n in names)
    for plain in ("gbt_lightgbm", "ridge", "seasonal_mean", "quantile_gbt_sklearn"):
        assert not lib["is_foundation_model"](plain)


# --- rules R13 and R14 ------------------------------------------------------------


def test_a_model_without_a_revision_or_licence_is_blocked_by_r14(lib):
    for missing in ("revision", "licence", "repo_id"):
        rules, conditions, blocked = lib["foundation_rules"](_facts(**{missing: None}))
        assert blocked and rules[0]["rule"] == "R14" and conditions == []


def test_an_unresolved_contamination_flag_gives_a_condition_r13(lib):
    rules, conditions, blocked = lib["foundation_rules"](_facts())
    assert not blocked and rules[0]["rule"] == "R13"
    assert "unresolved" in conditions[0]


def test_a_cleared_flag_gives_no_rule(lib):
    assert lib["foundation_rules"](_facts(contamination="cleared")) == ([], [], False)


def test_r14_is_a_blocking_rule_and_both_rules_are_in_the_table(lib):
    assert "R14" in lib["BLOCKING_RULES"]
    ids = {row[0] for row in lib["APPROVAL_RULES"]}
    assert {"R13", "R14"} <= ids


def test_a_foundation_decision_discloses_repository_revision_licence_and_mode(lib):
    rows = _rows("chronos2_zero_shot", 90.0, 95.0)
    out = lib["decide_selected"](
        _task(lib), rows, _rows("seasonal_mean", 140.0, 150.0), SPEC, 1, _facts()
    )
    assert out["decision"] == "approved_with_conditions"
    assert any("amazon/chronos-2" in n and "zero shot" in n for n in out["notes"])


def test_a_cleared_foundation_model_can_be_approved_without_conditions(lib):
    rows = _rows("chronos2_zero_shot", 90.0, 95.0)
    out = lib["decide_selected"](
        _task(lib),
        rows,
        _rows("seasonal_mean", 140.0, 150.0),
        SPEC,
        1,
        _facts(contamination="cleared"),
    )
    assert out["decision"] == "approved"


# --- ranking among the forwarded candidates ----------------------------------------


def test_the_accepted_ranking_is_unchanged_without_foundation_facts(lib):
    rows = _recommend(lib, None)
    roles = _roles(rows)
    assert ("gbt_lightgbm", "selected") in roles
    assert ("chronos2_zero_shot", "runner_up") in roles


def test_a_foundation_candidate_first_on_validation_becomes_the_selected_model(lib):
    rows = _recommend(lib, _facts())
    roles = _roles(rows)
    assert roles[("chronos2_zero_shot", "selected")]["decision"] == (
        "approved_with_conditions"
    )
    assert ("gbt_lightgbm", "runner_up") in roles
    assert ("seasonal_mean", "baseline_fallback") in roles


def test_a_blocked_foundation_candidate_does_not_displace_the_next_one(lib):
    rows = _recommend(lib, _facts(revision=None))
    roles = _roles(rows)
    assert roles[("gbt_lightgbm", "selected")]["decision"] == "approved"
    demoted = roles[("chronos2_zero_shot", "runner_up")]
    assert demoted["decision"] == "not_approved"
    assert (
        "ranked first on validation but not approved" in demoted["rules"][0]["detail"]
    )


def test_foundation_facts_take_the_register_before_the_parameters(lib):
    selection = [
        _selection("chronos2_zero_shot", 3, "foundation_ts"),
        _selection("gbt_lightgbm", 1),
    ]
    got = lib["foundation_facts"](
        selection,
        {
            "chronos2_zero_shot": {
                "model_key": "chronos2",
                "revision": "from-params",
                "licence": "mit",
                "mode": "lora",
            }
        },
        {"chronos2": {"repo_id": "amazon/chronos-2", "revision": "from-register"}},
        {"chronos2": {"flag": "cleared", "overlap_note": "n"}},
    )
    assert set(got) == {"chronos2_zero_shot"}
    facts = got["chronos2_zero_shot"]
    assert facts["revision"] == "from-register" and facts["licence"] == "mit"
    assert facts["contamination"] == "cleared" and facts["mode"] == "lora"


# --- registry ---------------------------------------------------------------------


def _entry(task, model, role="selected", version=1, status="registered"):
    return {
        "task_id": task,
        "model_name": model,
        "role": role,
        "version": version,
        "lifecycle_status": status,
    }


def test_a_foundation_model_replacing_a_selected_model_says_so(lib):
    existing = [_entry("load.load", "gbt_lightgbm")]
    wanted = [_entry("load.load", "chronos2_zero_shot")]
    got = lib["annotate_replacements"](wanted, existing, lib["is_foundation_model"])
    assert "replaces gbt_lightgbm" in got[0]["reason"]


def test_no_reason_is_added_for_a_first_entry_or_a_classic_model(lib):
    is_f = lib["is_foundation_model"]
    first = [_entry("load.load", "chronos2_zero_shot")]
    assert "reason" not in lib["annotate_replacements"](first, [], is_f)[0]
    classic = [_entry("load.load", "ridge")]
    existing = [_entry("load.load", "gbt_lightgbm")]
    assert "reason" not in lib["annotate_replacements"](classic, existing, is_f)[0]


def test_the_reason_reaches_the_first_version_row(lib):
    wanted = [
        {
            **_entry("load.load", "chronos2_zero_shot"),
            "reason": "replaces gbt_lightgbm as the selected model",
            "decision": "approved",
            "restriction": None,
            "conditions": "[]",
            "mlflow_run_id": "r",
            "frozen_delta_version": 0,
            "approval_run_id": "a",
            "card_path": "c",
        }
    ]
    new = lib["plan_versions"]([], wanted, {}, "rid", "now")
    assert new[0]["version"] == 1
    assert new[0]["reason"] == "replaces gbt_lightgbm as the selected model"


def test_a_foundation_model_has_the_looser_re_score_tolerance(lib):
    thresholds = {"reproduction_tolerance_relative": 1e-6}
    assert lib["tolerance_for"]("chronos2_lora", thresholds) == 1e-3
    assert lib["tolerance_for"]("gbt_lightgbm", thresholds) == 1e-6
