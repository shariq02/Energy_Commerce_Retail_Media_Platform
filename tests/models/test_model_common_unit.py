"""Unit tests for the pure parts of `databricks/models/_model_common.py`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. Only functions that need no live Spark
session are exercised.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_model_common

pytestmark = [pytest.mark.unit]

_MODELS = Path(__file__).resolve().parents[2] / "databricks" / "models"


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


def test_group_assignment_puts_every_group_in_one_partition(c):
    groups = [(f"g{i}", "A") for i in range(100)] + [("h0", "B"), ("h1", "B")]
    out = c["assign_group_partitions"](groups, (70, 15, 15))
    assert set(out) == {g for g, _ in groups}
    a = [p for g, p in out.items() if g.startswith("g")]
    assert (a.count("train"), a.count("validation"), a.count("test")) == (70, 15, 15)


def test_small_strata_stay_in_train(c):
    out = c["assign_group_partitions"]([("h0", "B"), ("h1", "B")], (70, 15, 15))
    assert set(out.values()) == {"train"}


def test_group_assignment_is_deterministic_and_seeded(c):
    groups = [(f"g{i}", "A") for i in range(50)]
    first = c["assign_group_partitions"](groups, (60, 20, 20))
    assert first == c["assign_group_partitions"](groups, (60, 20, 20))
    assert first != c["assign_group_partitions"](groups, (60, 20, 20), seed="other")


def test_asset_text_normalisation(c):
    assert (
        c["normalise_asset_text"]("  kraftwerk   neurath a ") == "KRAFTWERK NEURATH A"
    )
    assert c["normalise_asset_text"](None) is None


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


def test_task_registry_is_complete_and_points_at_real_notebooks(c):
    tasks = c["TASKS"]
    assert len(tasks) == 26
    assert len({t["task_id"] for t in tasks}) == 26
    assert len({(t["ecosystem"], t["dataset_id"]) for t in tasks}) == 24
    for t in tasks:
        assert (_MODELS / t["notebook"]).exists(), t["task_id"]
        assert t["ecosystem"] in c["MODEL_SCHEMAS"]
        assert t["notebook"].startswith(t["ecosystem"] + "/")


def test_model_schemas_are_the_agreed_names(c):
    assert c["MODEL_SCHEMAS"] == {
        "energy": "energy_ml_models",
        "commerce": "commerce_ml_models",
    }
    assert set(c["MODEL_DDL"]) == {
        "task_registry",
        "evaluation_split_manifest",
        "evaluation_spec",
        "candidate_results",
        "candidate_selection",
        "library_availability",
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
