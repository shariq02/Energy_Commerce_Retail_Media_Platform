"""Unit tests for the sampling of the rows that a context model scores, under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The sample, the recorded parameters, the
accepted models scored on the same sample and the sampled evaluation frames are
checked with fake bundles and fake record functions; no Spark session is used.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import load_eval_libs, load_foundation_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


@pytest.fixture(scope="module")
def ev():
    return load_eval_libs()


# --- the sample ---------------------------------------------------------------------


def test_the_sample_limit_is_one_hundred_thousand_rows(lib):
    assert lib["IN_CONTEXT_SAMPLE_ROWS"] == 100_000


def test_a_frame_under_the_limit_is_kept_whole(lib):
    assert list(lib["context_positions"](5, 100_000, 42)) == [0, 1, 2, 3, 4]


def test_the_sample_is_seeded_sorted_and_of_the_limit_size(lib):
    first = lib["context_positions"](1_000, 100, 42)
    again = lib["context_positions"](1_000, 100, 42)
    assert len(first) == len(set(first)) == 100
    assert list(first) == sorted(first) == list(again)


def test_the_recorded_sample_parameters_name_rows_total_and_fraction(lib):
    assert lib["sample_params"](1_310_000, 100_000) == {
        "validation_rows": 100_000,
        "validation_rows_total": 1_310_000,
        "validation_fraction": 0.0763,
    }


# --- the accepted models on the same sample -------------------------------------------


@pytest.fixture
def recorded(lib, monkeypatch):
    rows = []
    monkeypatch.setitem(lib, "missing_libraries", lambda requires: [])
    monkeypatch.setitem(lib, "record_result", lambda *args: rows.append(args))
    monkeypatch.setitem(lib, "log_candidate_to_mlflow", lambda *args: ("run", "none"))
    return rows


class _Bundle:
    def __init__(self, value):
        self.value = value

    def predict(self, frame):
        return np.full(len(frame), self.value)


def _selection():
    return [
        {"model_name": "lightgbm", "family": "gbt", "mlflow_run_id": "r1"},
        {"model_name": "logistic", "family": "linear", "mlflow_run_id": "r2"},
    ]


def test_each_accepted_model_is_recorded_as_a_reference_on_the_sample(
    lib, recorded, monkeypatch
):
    monkeypatch.setitem(
        lib, "load_bundle", lambda model: _Bundle(float(model["mlflow_run_id"][1:]))
    )
    sample = lib["sample_params"](1_000, 100)
    ctx = SimpleNamespace(task_id="task.one")
    lib["record_references"](
        ctx,
        lambda bundle: {"pr_auc": float(bundle.predict(range(3))[0])},
        100,
        sample,
        selection=_selection(),
    )
    assert [row[1] for row in recorded] == [
        "lightgbm@validation_sample",
        "logistic@validation_sample",
    ]
    assert {row[3] for row in recorded} == {"reference"}
    assert [row[4] for row in recorded] == ["ok", "ok"]
    assert [row[5]["pr_auc"] for row in recorded] == [1.0, 2.0]
    assert recorded[0][6] == 100
    assert recorded[0][10]["validation_rows_total"] == 1_000
    assert recorded[0][9] == "none"


def test_a_reference_that_cannot_be_loaded_is_recorded_as_failed_and_the_next_runs(
    lib, recorded, monkeypatch
):
    def load(model):
        if model["mlflow_run_id"] == "r1":
            raise RuntimeError("no stored artifact for this model")
        return _Bundle(0.5)

    monkeypatch.setitem(lib, "load_bundle", load)
    lib["record_references"](
        SimpleNamespace(task_id="task.one"),
        lambda bundle: {"pr_auc": 0.5},
        10,
        {},
        selection=_selection(),
    )
    assert [row[4] for row in recorded] == ["failed", "ok"]
    assert "no stored artifact" in recorded[0][7]


def test_no_accepted_model_means_no_reference_row(lib, recorded):
    lib["record_references"](
        SimpleNamespace(task_id="task.one"), lambda b: {}, 10, {}, selection=[]
    )
    assert recorded == []


# --- the held-out evaluation frames -----------------------------------------------------


def _evaluator(rows_eval, rows_validation):
    frames = {
        "eval": pd.DataFrame({"a": range(rows_eval)}),
        "validation": pd.DataFrame({"a": range(rows_validation)}),
    }
    return SimpleNamespace(
        ctx=SimpleNamespace(partition="test", thresholds={"bootstrap_seed": 7}),
        frames=frames,
        sampler="old",
        sample_note="",
    )


_CONTEXT = {"params": "{'row_fraction': 1.0, 'mode': 'in_context'}"}
_ACCEPTED = {"params": "{'row_fraction': 1.0}"}


def test_a_context_model_is_found_by_its_recorded_mode(ev):
    assert ev["uses_context_model"]([_ACCEPTED, _CONTEXT])
    assert not ev["uses_context_model"]([_ACCEPTED, {"params": None}])


def test_both_frames_are_sampled_when_a_context_model_is_forwarded(ev):
    evaluator = _evaluator(250, 120)
    ev["sample_eval_frames"](evaluator, [_ACCEPTED, _CONTEXT], max_rows=100)
    assert len(evaluator.frames["eval"]) == 100
    assert len(evaluator.frames["validation"]) == 100
    assert evaluator.sampler is None
    assert "test sample 100 of 250 rows" in evaluator.sample_note
    assert "validation sample 100 of 120 rows" in evaluator.sample_note


def test_the_sample_of_a_frame_is_the_same_on_every_call(ev):
    first, second = _evaluator(250, 10), _evaluator(250, 10)
    ev["sample_eval_frames"](first, [_CONTEXT], max_rows=100)
    ev["sample_eval_frames"](second, [_CONTEXT], max_rows=100)
    assert list(first.frames["eval"]["a"]) == list(second.frames["eval"]["a"])


def test_a_frame_under_the_limit_is_not_changed(ev):
    evaluator = _evaluator(50, 20)
    ev["sample_eval_frames"](evaluator, [_CONTEXT], max_rows=100)
    assert len(evaluator.frames["eval"]) == 50
    assert evaluator.sample_note == ""


def test_without_a_context_model_the_frames_stay_whole(ev):
    evaluator = _evaluator(250, 120)
    ev["sample_eval_frames"](evaluator, [_ACCEPTED], max_rows=100)
    assert len(evaluator.frames["eval"]) == 250
    assert evaluator.sample_note == ""


def test_a_frame_that_is_not_a_table_of_rows_stays_as_it_is(ev):
    evaluator = _evaluator(250, 120)
    evaluator.frames["eval"] = {"sessions": list(range(500))}
    ev["sample_eval_frames"](evaluator, [_CONTEXT], max_rows=100)
    assert evaluator.frames["eval"] == {"sessions": list(range(500))}


# --- skipped models and the reproduction of a sampled frame ---------------------------


_TABPFN = {
    "model_name": "tabpfn_25_in_context",
    "family": "foundation_tab",
    "params": "{'row_fraction': 1.0, 'mode': 'in_context'}",
}


def test_tabpfn_on_cpu_is_skipped_above_the_row_limit(ev, monkeypatch):
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    assert ev["skip_reason"](_TABPFN, _evaluator(1_500, 10)) == ev["CPU_ROW_REASON"]
    assert ev["skip_reason"](_TABPFN, _evaluator(1_000, 10)) is None
    other = {**_TABPFN, "model_name": "tabicl_v2_in_context"}
    assert ev["skip_reason"](other, _evaluator(1_500, 10)) is None


def test_tabpfn_with_a_gpu_is_not_skipped(ev, monkeypatch):
    monkeypatch.setitem(ev, "gpu_available", lambda: True)
    assert ev["skip_reason"](_TABPFN, _evaluator(1_500, 10)) is None


def test_the_survival_context_model_is_skipped_without_a_gpu(ev, monkeypatch):
    survival = {"model_name": "x", "family": ev["GPU_ONLY_FAMILIES"][0]}
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    assert ev["skip_reason"](survival, _evaluator(10, 10)) == ev["GPU_ONLY_REASON"]
    monkeypatch.setitem(ev, "gpu_available", lambda: True)
    assert ev["skip_reason"](survival, _evaluator(10, 10)) is None


def test_a_skipped_context_model_does_not_set_the_sample(ev, monkeypatch):
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    evaluator = _evaluator(1_500, 1_200)
    ev["sample_eval_frames"](evaluator, [_ACCEPTED, _TABPFN], max_rows=100)
    assert len(evaluator.frames["eval"]) == 1_500
    assert len(evaluator.frames["validation"]) == 1_200
    assert evaluator.sample_note == ""


def test_a_sampled_validation_frame_is_marked_and_named_in_the_note(ev):
    evaluator = _evaluator(250, 120)
    evaluator.validation_sampled = False
    ev["sample_eval_frames"](evaluator, [_CONTEXT], max_rows=100)
    assert evaluator.validation_sampled
    assert "reproduction not checked: validation sampled" in evaluator.sample_note


def _enrich_inputs(sampled):
    class Evaluator:
        can_reproduce = True
        validation_sampled = sampled
        base_name = "base"
        scored = False

        def frame(self, part):
            return pd.DataFrame({"a": [1, 2]})

        def segments(self, sc):
            return {}

        def score(self, bundle, part, bundles):
            self.scored = True
            return SimpleNamespace(metrics={"pr_auc": 0.5})

    ctx = SimpleNamespace(primary_metric="pr_auc", higher_is_better=True)
    model = {"model_name": "m", "validation": {"pr_auc": 0.5}}
    sc = SimpleNamespace(
        metrics={"pr_auc": 0.4}, pred=np.array([0.1, 0.2]), target=np.array([0, 1])
    )
    return ctx, Evaluator(), model, sc


def test_reproduction_is_not_checked_on_a_sampled_validation_frame(ev):
    ctx, evaluator, model, sc = _enrich_inputs(sampled=True)
    out = ev["enrich"](ctx, evaluator, model, object(), sc, None, {})
    assert "reproduction_gap_relative" not in out
    assert not evaluator.scored


def test_reproduction_is_checked_on_a_whole_validation_frame(ev):
    ctx, evaluator, model, sc = _enrich_inputs(sampled=False)
    out = ev["enrich"](ctx, evaluator, model, object(), sc, None, {})
    assert out["reproduction_gap_relative"] == 0.0
    assert evaluator.scored
