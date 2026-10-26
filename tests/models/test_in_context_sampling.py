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


# --- event cap, time limit, TimesFM batch and libraries ------------------------------


def _events(per_variable, variables=3):
    return {
        j: [(j, i, 5 + i % 3) for i in range(per_variable)] for j in range(variables)
    }


def test_the_event_cap_keeps_the_first_events_of_each_variable(ev):
    events = _events(1500)
    capped, kept, drawn, digest = ev["cap_events"](events, 500)
    assert (kept, drawn) == (1500, 4500)
    assert all(capped[j] == events[j][:500] for j in events)
    assert len(digest) == 8


def test_a_larger_cap_keeps_the_events_of_a_smaller_cap(ev):
    events = _events(1500)
    small, *_ = ev["cap_events"](events, 500)
    large, *_ = ev["cap_events"](events, 1000)
    assert all(large[j][:500] == small[j] for j in events)


def test_the_event_digest_changes_with_the_events_and_is_stable(ev):
    events = _events(1500)
    first = ev["cap_events"](events, 500)[3]
    assert first == ev["cap_events"](events, 500)[3]
    assert first != ev["cap_events"](events, 400)[3]


def test_without_a_cap_the_events_stay_whole(ev):
    events = _events(1500)
    same, kept, drawn, digest = ev["cap_events"](events, None)
    assert same is events and kept == drawn == 4500 and digest == ""


def test_only_a_slow_reconstruction_model_sets_the_event_cap(ev):
    slow = [{"model_name": "causal_baseline"}, {"model_name": "moment_zero_shot"}]
    fast = [
        {"model_name": "causal_baseline"},
        {"model_name": "energy_tspulse_zero_shot"},
    ]
    assert ev["recon_event_cap"](slow) == ev["SLOW_RECON_EVENTS_PER_VARIABLE"] == 500
    assert ev["recon_event_cap"](fast) is None
    assert ev["ReconstructionEvaluator"].event_cap is None


def test_moment_has_a_longer_time_limit_than_the_other_models(ev):
    assert ev["MODEL_LIMIT_SECONDS"] == {"moment_zero_shot": 5400}
    assert ev["EVALUATION_LIMIT_SECONDS"] == 1800


def _fake_timesfm(monkeypatch):
    import dataclasses
    import sys
    import types

    calls = []

    @dataclasses.dataclass
    class ForecastConfig:
        max_context: int = 0
        max_horizon: int = 0
        per_core_batch_size: int = 1
        normalize_inputs: bool = False
        use_continuous_quantile_head: bool = False
        force_flip_invariance: bool = True
        infer_is_positive: bool = True
        fix_quantile_crossing: bool = False

    class Model:
        def compile(self, config):
            calls.append(config)

    class Klass:
        @staticmethod
        def from_pretrained(_path):
            return Model()

    module = types.ModuleType("timesfm")
    module.ForecastConfig = ForecastConfig
    module.TimesFM_2p5_200M_torch = Klass
    monkeypatch.setitem(sys.modules, "timesfm", module)
    return calls


def test_timesfm_compiles_with_a_batch_of_series(lib, monkeypatch):
    calls = _fake_timesfm(monkeypatch)
    monkeypatch.setitem(lib, "_device", lambda: "cpu")
    backend = lib["TimesFMBackend"]("weights")
    backend.load(2, 512)
    backend.load(2, 512)
    assert len(calls) == 1
    assert calls[0].per_core_batch_size == lib["TIMESFM_BATCH"] == 64
    assert (calls[0].max_context, calls[0].max_horizon) == (512, 2)


def test_timesfm_batch_can_be_set_for_a_probe(lib, monkeypatch):
    calls = _fake_timesfm(monkeypatch)
    monkeypatch.setitem(lib, "_device", lambda: "cpu")
    lib["TimesFMBackend"]("weights", batch=1).load(2, 512)
    assert calls[0].per_core_batch_size == 1


def test_the_library_installer_lists_peft():
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    path = root / "databricks/models/00_model_libraries.py"
    tree = ast.parse(path.read_text(encoding="utf-8").replace("# MAGIC", "#"))
    packages = next(
        n.value
        for n in tree.body
        if isinstance(n, ast.Assign)
        and any(getattr(t, "id", "") == "PACKAGES" for t in n.targets)
    )
    assert "peft" in [k.value for k in packages.keys]


def test_a_model_name_gives_the_libraries_of_its_bundle(ev):
    libs = ev["model_libraries"]
    assert libs({"model_name": "gbt_lightgbm"}) == ["lightgbm"]
    assert libs({"model_name": "chronos2_lora"}) == ["chronos", "peft"]
    assert libs({"model_name": "ttm_r2_zero_shot"}) == ["tsfm_public"]
    assert libs({"model_name": "seasonal_naive"}) == []


def test_missing_libraries_stop_the_task_before_any_data_is_read(ev, monkeypatch):
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    monkeypatch.setitem(
        ev, "library_origin", lambda name: None if name == "lightgbm" else "x"
    )
    models = [
        {"model_name": "gbt_lightgbm", "family": "gbt"},
        {"model_name": "seasonal_naive", "family": "baseline"},
    ]
    with pytest.raises(RuntimeError, match=r"x\.y: libraries missing .*lightgbm"):
        ev["check_model_libraries"]("x.y", models)


def test_a_gpu_only_model_needs_no_library_on_cpu(ev, monkeypatch):
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    monkeypatch.setitem(ev, "library_origin", lambda name: None)
    models = [{"model_name": "tabicl_discrete_in_context", "family": "foundation_surv"}]
    ev["check_model_libraries"]("x.y", models)


def test_available_libraries_are_printed_with_version_and_origin(
    ev, monkeypatch, capsys
):
    monkeypatch.setitem(ev, "gpu_available", lambda: False)
    monkeypatch.setitem(ev, "library_version", lambda name: "4.7.0")
    monkeypatch.setitem(
        ev, "library_origin", lambda name: "site_packages_aarch64/lightgbm"
    )
    ev["check_model_libraries"](
        "x.y", [{"model_name": "gbt_lightgbm", "family": "gbt"}]
    )
    assert (
        "OK   lightgbm: 4.7.0 from site_packages_aarch64/lightgbm"
        in capsys.readouterr().out
    )


def test_the_library_origin_is_found_without_an_import(ev):
    assert ev["library_origin"]("json_not_installed_xyz") is None
    assert ev["library_origin"]("json") is not None
