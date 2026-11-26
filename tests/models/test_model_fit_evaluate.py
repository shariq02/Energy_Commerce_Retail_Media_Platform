"""Unit tests for the fit and evaluation libraries under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: November 2026

Loaded through `tests/_notebook_loader.py`. The shared library (tasks, tables, folds,
findings text and flags, skip rule), the known-answer metrics, the baselines and the
other fit helpers, the candidate guard (time limit, stack print and the recorded
outcome of a slow candidate, with fake record functions) and the evaluation (resampling,
scoring per paradigm, flag rules, findings text, partition integrity). No Spark session
and no stored model is used.
"""

from __future__ import annotations

import datetime as dt
import math
import re
import signal
import threading
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_model_common, load_model_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_model_libs()


@pytest.fixture(scope="module")
def c():
    return load_model_common()


@pytest.fixture
def recorded(lib, monkeypatch):
    rows = []
    monkeypatch.setitem(lib, "missing_libraries", lambda requires: [])
    monkeypatch.setitem(lib, "record_result", lambda *args: rows.append(args))
    monkeypatch.setitem(lib, "log_candidate_to_mlflow", lambda *args: ("run-1", "ok"))
    return rows


def _candidate_context():
    return SimpleNamespace(task_id="task.one")


# --- limit by family --------------------------------------------------------------


def test_other_families_get_the_default_limit(lib):
    assert lib["candidate_limit"]("tabular") == 1200
    assert lib["candidate_limit"]("sequence") == 1200


# --- the guard --------------------------------------------------------------------


def test_a_wait_over_the_limit_raises_a_timeout_with_the_label(lib):
    started = time.time()
    with (
        pytest.raises(TimeoutError, match="slow one: timeout after 1 s"),
        lib["candidate_guard"]("slow one", 1, stack_seconds=0),
    ):
        time.sleep(10)
    assert time.time() - started < 5


def test_a_fast_candidate_passes_and_leaves_no_alarm_behind(lib):
    has_alarm = hasattr(signal, "SIGALRM")
    before = signal.getsignal(signal.SIGALRM) if has_alarm else None
    with lib["candidate_guard"]("fast one", 5, stack_seconds=0):
        value = 1 + 1
    assert value == 2
    if has_alarm:
        assert signal.alarm(0) == 0
        assert signal.getsignal(signal.SIGALRM) == before
    else:
        assert not [t for t in threading.enumerate() if isinstance(t, threading.Timer)]


def test_the_stack_of_a_waiting_candidate_is_printed(lib, capsys):
    with lib["candidate_guard"]("waiting one", 5, stack_seconds=0.2):
        time.sleep(0.8)
    out = capsys.readouterr().out
    assert "STACK waiting one after" in out
    assert "sleep" in out


# --- stage times ------------------------------------------------------------------


def test_a_stage_prints_its_start_and_its_time(lib, capsys):
    with lib["timed_stage"]("load weights"):
        pass
    out = capsys.readouterr().out
    assert ".. load weights\n" in out
    assert ".. load weights: done in" in out


def test_a_failing_stage_prints_that_it_failed(lib, capsys):
    with (
        pytest.raises(ValueError, match="boom"),
        lib["timed_stage"]("fit rows"),
    ):
        raise ValueError("boom")
    assert ".. fit rows: failed after" in capsys.readouterr().out


# --- the runner records a timeout and goes on ---------------------------------------


def test_a_candidate_over_the_limit_is_recorded_as_failed_with_the_timeout(
    lib, recorded, capsys
):
    def slow():
        time.sleep(10)

    result = lib["run_candidate"](
        _candidate_context(), "slow_model", "tabular", slow, limit_seconds=1
    )
    assert result is None
    assert len(recorded) == 1
    row = recorded[0]
    assert row[1] == "slow_model" and row[4] == "failed"
    assert "TimeoutError" in row[7] and "timeout after 1 s" in row[7]
    out = capsys.readouterr().out
    assert "START slow_model [tabular] task=task.one limit=1 s" in out
    assert "FAIL slow_model" in out


def test_a_candidate_inside_the_limit_is_recorded_as_ok(lib, recorded):
    def fast():
        return object(), {"pr_auc": 0.5}, 7

    metrics = lib["run_candidate"](
        _candidate_context(), "fast_model", "tabular", fast, limit_seconds=5
    )
    assert metrics == {"pr_auc": 0.5}
    assert [row[4] for row in recorded] == ["ok"]


# --- the shared library -----------------------------------------------------------


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


def test_near_perfect_is_one_rule_for_the_train_flag_and_the_evaluation_flag(c, lib):
    assert c["NEAR_PERFECT"] == 0.995
    flags = c["findings_flags"](
        [{"task_id": "t", "primary_metric": "pr_auc", "higher_is_better": True}],
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
    rows = [
        _result("m", "candidate", "pr_auc", 1.0),
        _result("b", "baseline", "pr_auc", 0.999),
    ]
    flags = lib["evaluation_flag_rows"](rows, _THRESHOLDS)
    assert [(f[1], f[3]) for f in flags] == [("m", "near-perfect score")]
    assert flags[0][4] == "pr_auc = 1"


# --- the known-answer metrics -----------------------------------------------------


def test_regression_errors_and_skill(lib):
    assert lib["mae"]([1, 2, 3], [1, 2, 5]) == pytest.approx(2 / 3)
    assert lib["rmse"]([1, 2, 3], [1, 2, 5]) == pytest.approx(math.sqrt(4 / 3))
    assert lib["skill"](1.0, 2.0) == pytest.approx(0.5)
    assert math.isnan(lib["skill"](1.0, 0.0))


def test_nan_pairs_are_dropped(lib):
    assert lib["mae"]([1, np.nan, 3], [2, 5, 3]) == pytest.approx(0.5)


def test_pinball_penalises_the_right_side(lib):
    assert lib["pinball"]([10], [8], 0.9) == pytest.approx(1.8)
    assert lib["pinball"]([8], [10], 0.9) == pytest.approx(0.2)


def test_quantile_metrics_report_each_level_and_coverage(lib):
    y = np.arange(100, dtype=float)
    out = lib["quantile_metrics"](y, {0.1: y - 5, 0.5: y, 0.9: y + 5})
    assert out["pinball_q50"] == pytest.approx(0.0)
    assert out["interval_coverage_80"] == pytest.approx(1.0)


def test_average_precision(lib):
    average_precision = lib["average_precision"]
    assert average_precision([1, 1, 0, 0], [0.9, 0.8, 0.3, 0.1]) == pytest.approx(1.0)
    assert average_precision([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(
        (1 + 2 / 3) / 2
    )


def test_constant_scores_give_the_base_rate(lib):
    assert lib["average_precision"]([1, 0, 0, 0], [0.5] * 4) == pytest.approx(0.25)


def test_log_loss_and_calibration(lib):
    assert lib["log_loss"]([1, 0], [0.9, 0.1]) == pytest.approx(-math.log(0.9))
    assert lib["calibration_error"]([1, 1, 0, 0], [1, 1, 0, 0]) == pytest.approx(0.0)
    assert lib["calibration_error"]([1, 1, 1, 1], [0.0] * 4) == pytest.approx(1.0)


def test_classification_metrics_bundle(lib):
    out = lib["classification_metrics"]([1, 0, 1, 0], [0.9, 0.2, 0.8, 0.1])
    assert out["pr_auc"] == pytest.approx(1.0)
    assert out["base_rate"] == pytest.approx(0.5)


def test_concordance_index(lib):
    d, e = [1, 2, 3, 4], [1, 1, 1, 1]
    assert lib["concordance_index"](d, e, [4, 3, 2, 1]) == pytest.approx(1.0)
    assert lib["concordance_index"](d, e, [1, 2, 3, 4]) == pytest.approx(0.0)
    assert lib["concordance_index"](d, e, [1, 1, 1, 1]) == pytest.approx(0.5)


def test_kaplan_meier_curve_and_lookup(lib):
    times, surv = lib["kaplan_meier"]([1, 2, 3], [1, 1, 1])
    assert list(times) == [1, 2, 3]
    assert list(surv) == pytest.approx([2 / 3, 1 / 3, 0.0])
    at = lib["km_at"](times, surv, [0.5, 1, 2.5])
    assert list(at) == pytest.approx([1.0, 2 / 3, 1 / 3])


def test_kaplan_meier_handles_censoring(lib):
    _, surv = lib["kaplan_meier"]([1, 2, 3], [1, 0, 1])
    assert list(surv) == pytest.approx([2 / 3, 0.0])


def test_brier_and_calibration_gap(lib):
    assert lib["brier_at"]([1, 5], [1, 1], [0.0, 1.0], 3) == pytest.approx(0.0)
    assert lib["brier_at"]([1, 5], [1, 1], [0.5, 0.5], 3) == pytest.approx(0.25)
    gap = lib["survival_calibration_gap"]([1, 2, 3], [1, 1, 1], [1 / 3] * 3, 2)
    assert gap == pytest.approx(0.0)


def test_ranking_metrics(lib):
    out = lib["ranking_metrics"]([1, 2, math.inf], ks=(1, 2))
    assert out["recall_at_1"] == pytest.approx(1 / 3)
    assert out["recall_at_2"] == pytest.approx(2 / 3)
    assert out["ndcg_at_2"] == pytest.approx((1 + 1 / math.log2(3)) / 3)
    assert out["mrr"] == pytest.approx(0.5)


def test_masked_mae_uses_only_masked_positions(lib):
    masked = lib["masked_mae"]([1, 2, 3], [2, 2, 5], [True, False, True])
    assert masked == pytest.approx(1.5)


def test_detection_metrics(lib):
    out = lib["detection_metrics"]([1, 1, 0, 0], [1, 0, 1, 0], 0.02)
    assert out["precision"] == pytest.approx(0.5)
    assert out["recall"] == pytest.approx(0.5)
    assert out["flag_rate"] == pytest.approx(0.02)


def test_agreement_metrics(lib):
    out = lib["agreement_metrics"](["a", "b", "a"], ["a", "a", "a"], [1, 2, 3])
    assert out["action_agreement"] == pytest.approx(2 / 3)
    assert out["reward_when_agree"] == pytest.approx(2.0)
    assert out["reward_when_disagree"] == pytest.approx(2.0)
    assert out["reward_weighted_agreement"] == pytest.approx(4 / 6)


def test_per_class_precision_recall_marks_thin_classes(lib):
    y_true = ["a"] * 12 + ["b"] * 2
    y_pred = ["a"] * 12 + ["a", "b"]
    out = lib["per_class_precision_recall"](y_true, y_pred, ["a", "b"], min_rows=10)
    p, r, n, ok = out["a"]
    assert (p, r, n, ok) == (pytest.approx(12 / 13), pytest.approx(1.0), 12, True)
    assert out["b"][3] is False
    assert math.isnan(out["b"][0])


def test_bias_metrics_report_signed_error_and_spread(lib):
    out = lib["bias_metrics"]([1, 2, 3, 4], [2, 3, 4, 5])
    assert out["mean_error"] == pytest.approx(1.0)
    assert out["mean_error_relative"] == pytest.approx(0.4)
    assert out["pred_std_ratio"] == pytest.approx(1.0)
    constant = lib["bias_metrics"]([1, 2, 3, 4], [2.5] * 4)
    assert constant["pred_std_ratio"] == pytest.approx(0.0)


# --- baselines, injection, survival, rankers and policies ---------------------------


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


# --- the evaluation: resampling, scoring, flags, findings ---------------------------


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


def test_every_task_has_a_specification_and_an_evaluator(lib):
    assert set(lib["EVAL_TASKS"]) == {t["task_id"] for t in lib["TASKS"]}
    for cfg in lib["EVAL_TASKS"].values():
        assert lib["EVALUATOR_NAMES"][cfg["kind"]] in lib
        assert cfg["blocks"]["by"] in ("month", "group", "row")


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
