"""Unit tests for the pure parts of `databricks/models/lib/_model_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. Only functions that need no live Spark
session are exercised.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_model_common, load_model_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def c():
    return load_model_common()


def test_select_features_prefers_the_fitted_replacement(c):
    roles = {"a": "feature", "b": "feature", "k": "key", "t": "target", "f": "flag"}
    cols = ["a", "a_imputed", "a_is_missing", "b", "k", "t", "f", "ident"]
    out = c["select_features"](roles, cols)
    assert out == ["a_imputed", "a_is_missing", "b"]
    assert c["select_features"](roles, cols, id_features=["ident"])[-1] == "ident"
    assert "b" not in c["select_features"](roles, cols, drop=["b"])


def test_select_features_ignores_contract_columns_the_frame_lacks(c):
    assert c["select_features"]({"x": "feature"}, ["y"]) == []


def test_feature_encoder_handles_numbers_bools_text_and_dates(c):
    frame = pd.DataFrame(
        {
            "x": [1.0, 2.0, None, 4.0],
            "b": [True, False, True, False],
            "c": ["u", "v", "u", "w"],
            "d": [dt.date(1970, 1, 2)] * 4,
        }
    )
    enc = c["FeatureEncoder"](["x", "b", "c", "d"]).fit(frame)
    mat = enc.transform(frame)
    assert mat.shape == (4, 4)
    assert list(mat[:, 1]) == [1.0, 0.0, 1.0, 0.0]
    assert list(mat[:, 3]) == [1.0] * 4
    assert mat[0, 2] == mat[2, 2] == 0.0
    assert np.isnan(mat[2, 0])
    assert not np.isnan(enc.transform(frame, fill=True)).any()
    unseen = enc.transform(
        pd.DataFrame({"x": [1], "b": [True], "c": ["zzz"], "d": [dt.date(1970, 1, 2)]})
    )
    assert np.isnan(unseen[0, 2])


def test_rolling_folds_train_on_earlier_rows_only(c):
    frame = pd.DataFrame(
        {
            "partition": ["train"] * 6 + ["validation"],
            "fold_id": [np.nan, np.nan, 0, 0, 1, 1, np.nan],
        }
    )
    folds = c["fold_splits"](frame, "rolling")
    assert len(folds) == 2
    tr0, va0 = folds[0]
    assert tr0.tolist() == [True, True, False, False, False, False, False]
    assert va0.tolist() == [False, False, True, True, False, False, False]
    tr1, _ = folds[1]
    assert tr1.tolist() == [True, True, True, True, False, False, False]


def test_grouped_folds_train_on_the_other_folds(c):
    frame = pd.DataFrame({"partition": ["train"] * 6, "fold_id": [0, 0, 1, 1, 2, 2]})
    folds = c["fold_splits"](frame, "grouped")
    tr, va = folds[0]
    assert va.tolist() == [True, True, False, False, False, False]
    assert tr.tolist() == [False, False, True, True, True, True]


def test_no_folds_when_the_mode_is_none(c):
    frame = pd.DataFrame({"partition": ["train"], "fold_id": [0]})
    assert c["fold_splits"](frame, "none") == []


def test_pick_params_follows_the_mean_fold_score(c):
    grid = [{"a": 1}, {"a": 2}]
    folds = [(None, None)]

    def score(params, tr, va):
        return float(params["a"])

    assert c["pick_params"](grid, folds, score) == {"a": 1}
    assert c["pick_params"](grid, folds, score, lower_is_better=False) == {"a": 2}
    assert c["pick_params"](grid, [], score) == {"a": 1}


def test_only_train_and_validation_may_be_read(c):
    c["assert_no_test_partition"](("train", "validation"))
    with pytest.raises(RuntimeError):
        c["assert_no_test_partition"](("train", "test"))


def test_task_file_is_complete(c):
    tasks = c["TASKS"]
    assert len(tasks) == 36
    assert len({t["task_id"] for t in tasks}) == 36
    by_ecosystem = {e: sum(t["ecosystem"] == e for t in tasks) for e in c["ECOSYSTEMS"]}
    assert by_ecosystem == {"energy": 16, "commerce": 7, "genomics": 13}
    runners = load_model_libs()["RUNNERS"]
    for t in tasks:
        assert t["ecosystem"] in c["MODEL_SCHEMAS"]
        assert t["runner"] in runners, t["task_id"]


def test_check_only_sets_are_scored_by_a_trained_task(c):
    assert set(c["CHECK_ONLY"]) == {
        "benchmark_pathogenic_non_coding",
        "benchmark_spectralbio",
    }
    for entry in c["CHECK_ONLY"].values():
        assert entry["scored_by"] in c["TASK_BY_ID"]


def test_the_evaluation_spec_is_the_fit_spec_without_the_fit_only_keys(c):
    for t in c["TASKS"]:
        wanted = {k: v for k, v in t["fit"].items() if k not in c["FIT_ONLY_KEYS"]}
        assert set(wanted) <= set(c["eval_spec"](t["task_id"])), t["task_id"]


def test_both_ecosystems_share_one_model_schema(c):
    assert c["MODEL_SCHEMAS"] == dict.fromkeys(c["ECOSYSTEMS"], "ml_models")
    assert c["MODEL_SCHEMA"] == "ml_models"
    assert c["VOLUME_SCHEMA"] == "energy_ml_models"
    assert c["model_schema_for"]("commerce") == "ml_models"


def test_ddl_with_ecosystem_adds_the_column_once(c):
    ddl = c["MODEL_DDL"]
    for name, text in ddl.items():
        names = [n for n, _ in c["ddl_columns"](c["ddl_with_ecosystem"](text))]
        assert names.count("ecosystem") == 1, name
        assert names[-1] == "ecosystem", name
    plain = c["ddl_with_ecosystem"]("a string, b int")
    assert plain == "a string, b int, ecosystem string"


def test_ecosystem_predicate_scopes_every_row_predicate(c):
    assert c["ecosystem_predicate"]("energy") == "ecosystem = 'energy'"
    assert (
        c["ecosystem_predicate"]("commerce", "task_id = 'x' OR smoke")
        == "ecosystem = 'commerce' AND (task_id = 'x' OR smoke)"
    )


def test_model_ddl_holds_the_agreed_tables(c):
    assert set(c["MODEL_DDL"]) == {
        "evaluation_spec",
        "candidate_results",
        "candidate_selection",
        "library_availability",
        "task_run_context",
        "evaluation_run_context",
        "evaluation_results",
        "evaluation_flags",
        "approval_spec",
        "model_approval",
        "model_registry",
        "registry_check",
        "monitoring_spec",
        "reference_profile",
        "model_predictions",
        "monitoring_results",
        "monitoring_flags",
        "monitoring_triggers",
        "scoring_requests",
    }


def _row(
    task, model, stage, status, metric=None, value=None, artifact="ok", detail=None
):
    return {
        "task_id": task,
        "model_name": model,
        "stage": stage,
        "status": status,
        "metric": metric,
        "value": value,
        "n_rows": 10,
        "detail": detail,
        "frozen_delta_version": 2,
        "artifact_status": artifact,
        "run_id": "r1",
    }


def test_markdown_table_escapes_pipes_and_newlines(c):
    out = c["markdown_table"](["a", "b"], [("x|y", "line1\nline2")])
    assert out.splitlines()[0] == "| a | b |"
    assert out.splitlines()[2] == "| x/y | line1 line2 |"


def test_findings_list_baseline_candidates_failures_and_selection(c):
    task = "load.load"
    results = [
        _row(task, "seasonal_mean", "baseline", "ok", "mae", 10.0),
        _row(task, "seasonal_mean", "baseline", "ok", "rmse", 12.0),
        _row(task, "ridge", "candidate", "ok", "mae", 8.0),
        _row(task, "ridge", "candidate", "ok", "rmse", 9.5),
        _row(
            task,
            "gbt_lightgbm",
            "candidate",
            "skipped_library_unavailable",
            detail="missing: lightgbm",
        ),
        _row(
            task, "gbt_sklearn", "candidate", "failed", detail="ValueError: bad | shape"
        ),
    ]
    selection = [
        {"task_id": task, "model_name": "ridge", "rank": 1, "primary_value": 8.0}
    ]
    text = c["render_findings"]("energy", results, selection, smoke=False, stamp="t")
    assert "# ENERGY MODEL FINDINGS" in text
    assert "## load.load" in text
    assert "| gbt_lightgbm | candidate | skipped_library_unavailable |" in text
    assert "ValueError: bad / shape" in text
    assert "| mae | 10 | 8 |" in text
    assert "### forwarded for test evaluation" in text
    summary = next(
        line for line in text.splitlines() if line.startswith("| load.load |")
    )
    assert summary == "| load.load | yes | 1 | 1 | 1 |"


def test_findings_mark_tasks_without_results_and_hide_selection_for_smoke(c):
    text = c["render_findings"]("commerce", [], [], smoke=True, stamp="t")
    assert "# COMMERCE SMOKE RUN FINDINGS" in text
    assert "_no results recorded_" in text
    assert "forwarded" not in text


def _flag_task(primary="pr_auc", higher=True):
    return {"task_id": "t.x", "primary_metric": primary, "higher_is_better": higher}


def test_flags_raise_for_perfect_score_single_class_and_train_gap(c):
    results = [
        _row("t.x", "base", "baseline", "ok", "pr_auc", 0.5),
        _row("t.x", "m1", "candidate", "ok", "pr_auc", 1.0),
        _row("t.x", "m1", "candidate", "ok", "train_pr_auc", 1.0),
        _row("t.x", "m1", "candidate", "ok", "pred_positive_share", 1.0),
        _row("t.x", "m2", "candidate", "ok", "pr_auc", 0.2),
        _row("t.x", "m2", "candidate", "ok", "train_pr_auc", 0.9),
    ]
    found = {(m, f) for _, m, f, _ in c["findings_flags"]([_flag_task()], results)}
    assert ("m1", "near-perfect score") in found
    assert ("m1", "a single class is predicted") in found
    assert ("m2", "validation score far below training score") in found


def test_flags_skip_baselines_for_constant_predictions_and_unbounded_metrics(c):
    results = [
        _row("t.x", "base", "baseline", "ok", "reward_timing", 5e4),
        _row("t.x", "base", "baseline", "ok", "pred_positive_share", 0.0),
        _row("t.x", "base", "baseline", "ok", "pred_std_ratio", 0.0),
        _row("t.x", "m1", "candidate", "ok", "reward_timing", 6e4),
    ]
    flags = c["findings_flags"]([_flag_task("reward_timing")], results)
    assert not [f for f in flags if f[2] != "no candidate beats the baseline"]


def test_flags_report_single_class_and_single_tier_evaluations(c):
    results = [
        _row("t.x", "base", "baseline", "ok", "balanced_accuracy", 1.0),
        _row("t.x", "base", "baseline", "ok", "estimable__a", 1.0),
        _row("t.x", "base", "baseline", "ok", "estimable__b", 0.0),
        _row("t.x", "base", "baseline", "ok", "base_rate", 1.0),
    ]
    task = _flag_task("balanced_accuracy")
    found = {f for _, _, f, _ in c["findings_flags"]([task], results)}
    assert "fewer than two classes estimable in the evaluation" in found
    assert "single-class evaluation target" in found


def test_flags_report_a_sampled_model_frame_and_no_skill_over_a_diagnostic(c):
    results = [
        _row("t.x", "base", "baseline", "ok", "mae", 5.0),
        _row("t.x", "m1", "candidate", "ok", "mae", 4.0),
        _row("t.x", "m1", "candidate", "ok", "skill_mae_vs_scope_mean", -0.1),
    ]
    notes = '[{"rows": 10, "sample_fraction": 0.65}]'
    context = {"t.x": [{"status": "data_ready", "data_notes": notes}]}
    task = _flag_task("mae", False)
    found = {f for _, _, f, _ in c["findings_flags"]([task], results, context)}
    assert "model frame is a sample of the dataset" in found
    assert "no skill over the scope_mean diagnostic" in found


def test_flags_report_failures_bias_and_baseline_not_beaten(c):
    results = [
        _row("t.x", "base", "baseline", "ok", "mae", 5.0),
        _row("t.x", "m1", "candidate", "ok", "mae", 6.0),
        _row("t.x", "m1", "candidate", "ok", "mean_error_relative", 0.4),
        _row("t.x", "m2", "candidate", "failed", detail="ValueError: x"),
    ]
    found = {
        (m, f)
        for _, m, f, _ in c["findings_flags"]([_flag_task("mae", False)], results)
    }
    assert ("m1", "biased predictions") in found
    assert ("m2", "failed") in found
    assert ("base", "no candidate beats the baseline") in found


def test_findings_show_run_context_flags_and_the_other_mode(c):
    ctx = [
        {
            "task_id": "load.load",
            "smoke": True,
            "run_id": "r1",
            "status": s,
            "notebook_path": "/x/03_load",
            "job_id": "7",
            "job_run_id": "9",
            "library_versions": '{"torch": "2.1"}',
            "data_notes": None,
            "recorded_at": f"2026-10-01 0{i}:00:00",
        }
        for i, s in enumerate(["started", "data_ready"])
    ]
    text = c["render_findings"](
        "energy",
        [],
        [],
        smoke=True,
        stamp="t",
        context=ctx,
        other_mode={"load.load": (14, "2026-10-01T20:00:00")},
    )
    assert "## run context" in text
    assert "| load.load | r1 | started > data_ready | true | 7 | 9 |" in text
    assert "libraries: torch 2.1" in text
    assert "run did not finish" in text
    assert "the other run mode holds 14 result row(s)" in text


def test_findings_keep_user_folders_and_emails_out(c):
    ctx = [
        {
            "task_id": "load.load",
            "smoke": True,
            "run_id": "r1",
            "status": "finished",
            "notebook_path": "/Users/someone@example.com/Repo/databricks/models/energy/03_load",
            "job_id": "7",
            "job_run_id": "9",
            "library_versions": None,
            "data_notes": None,
            "recorded_at": "2026-10-01 01:00:00",
        }
    ]
    results = [
        _row(
            "load.load",
            "m",
            "candidate",
            "failed",
            detail="/Workspace/Users/someone@example.com/x.py failed",
        )
    ]
    text = c["render_findings"](
        "energy", results, [], smoke=True, stamp="t", context=ctx
    )
    assert "someone@example.com" not in text
    assert "/Users/someone" not in text
    assert "databricks/models/energy/03_load" in text
    assert "/Users/<user>/" in text


def test_the_front_library_folder_goes_first_and_the_main_folder_last(c, tmp_path):
    import sys

    main = tmp_path / "libs"
    front = tmp_path / "libs_front"
    main.mkdir()
    front.mkdir()
    saved = list(sys.path)
    original = c["library_folder"]
    try:
        c["library_folder"] = lambda: str(main)
        c["use_model_libraries"]()
        assert sys.path[0] == str(front)
        assert sys.path[-1] == str(main)
        assert c["front_library_folder"]() == str(front)
    finally:
        c["library_folder"] = original
        sys.path[:] = saved


def _context_row(status, minute, version=7, detail=None):
    return {
        "status": status,
        "frozen_delta_version": version,
        "detail": detail,
        "recorded_at": dt.datetime(2026, 11, 22, 10, minute, tzinfo=dt.UTC),
    }


def test_the_fingerprint_follows_the_settings_and_ignores_key_order(c):
    one = c["settings_fingerprint"]({"a": 1, "b": [2, 3]}, "x")
    assert one == c["settings_fingerprint"]({"b": [2, 3], "a": 1}, "x")
    assert one != c["settings_fingerprint"]({"a": 1, "b": [2, 4]}, "x")
    assert len(one) == 12


def test_a_finished_run_at_the_same_version_and_settings_is_skipped(c):
    detail = c["finished_detail"](True, "abc")
    rows = [_context_row("started", 1), _context_row("finished", 5, detail=detail)]
    assert c["run_is_current"](rows, 7, "abc")


@pytest.mark.parametrize(
    "version, fingerprint",
    [(8, "abc"), (7, "other")],
)
def test_other_data_or_other_settings_train_again(c, version, fingerprint):
    detail = c["finished_detail"](True, "abc")
    rows = [_context_row("finished", 5, detail=detail)]
    assert not c["run_is_current"](rows, version, fingerprint)


def test_a_later_unfinished_run_or_a_failed_baseline_trains_again(c):
    detail = c["finished_detail"](True, "abc")
    later = [
        _context_row("finished", 5, detail=detail),
        _context_row("data_ready", 9),
    ]
    assert not c["run_is_current"](later, 7, "abc")
    no_baseline = [
        _context_row("finished", 5, detail=c["finished_detail"](False, "abc"))
    ]
    assert not c["run_is_current"](no_baseline, 7, "abc")
    assert not c["run_is_current"]([], 7, "abc")


def test_skip_notes_group_the_skipped_tasks_by_ecosystem(c):
    first = c["TASKS"][0]
    notes = c["skip_notes"]([first["task_id"], f"{first['task_id']} (profile)"], "kept")
    assert list(notes) == [first["ecosystem"]]
    assert "Skipped, kept:" in notes[first["ecosystem"]][0]
    assert c["skip_notes"]([], "kept") == {}


def test_a_library_installed_under_another_distribution_name_has_a_version(
    c, monkeypatch
):
    from importlib import metadata

    def fake(name):
        if name == "mlflow-skinny":
            return "3.1.0"
        raise metadata.PackageNotFoundError(name)

    monkeypatch.setattr(c["_importlib_metadata"], "version", fake)
    assert c["library_version"]("mlflow") == "3.1.0"
    assert c["library_version"]("lifelines") is None


def _check(component, name, stamp, status="PASS"):
    return {
        "component": component,
        "metric_name": name,
        "status": status,
        "error_detail": "",
        "recorded_at": stamp,
    }


def test_current_check_rows_keep_the_latest_row_of_a_current_component(c):
    task = next(iter(c["TASK_BY_ID"]))
    rows = [
        _check("models/approve/04_approval_gate", "x", 1, "FAIL"),
        _check("models/approve/04_approval_gate", "x", 2, "PASS"),
        _check(f"models/evaluate/{task}", "baseline_evaluated", 3),
        _check("models/approve/gate/01_approval_guards", "x", 4),
        _check("models/evaluate/weak_supervision.labels", "x", 5),
        _check("models/approve/00_approval_setup", "x", 6),
    ]
    out = c["current_check_rows"](rows, {"04_approval_gate"})
    assert [(r["component"], r["status"]) for r in out] == [
        ("models/approve/04_approval_gate", "PASS"),
        (f"models/evaluate/{task}", "PASS"),
    ]


def test_a_check_only_task_is_a_current_component(c):
    entry = next(iter(c["CHECK_ONLY"].values()))
    rows = [_check(f"models/evaluate/{entry['task_id']}", "baseline_evaluated", 1)]
    assert len(c["current_check_rows"](rows, set())) == 1


def test_notebook_stems_name_the_notebooks_of_the_stage_folders(c):
    stems = c["notebook_stems"](("approve",))
    assert {"01_recommend", "04_approval_gate"} <= stems
    assert "01_train" not in stems


def test_near_perfect_is_one_rule_for_the_train_flag_and_the_approval(c):
    assert c["NEAR_PERFECT"] == 0.995
    flags = c["findings_flags"](
        [
            {
                "task_id": "t",
                "primary_metric": "pr_auc",
                "higher_is_better": True,
            }
        ],
        [
            {
                "task_id": "t",
                "model_name": "m",
                "stage": "candidate",
                "status": "ok",
                "detail": None,
                "metric": "pr_auc",
                "value": 1.0,
            }
        ],
    )
    assert ("t", "m", "near-perfect score", "pr_auc = 1") in flags
