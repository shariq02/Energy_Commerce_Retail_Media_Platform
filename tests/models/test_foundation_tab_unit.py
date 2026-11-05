"""Unit tests for the foundation tabular library under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The context rows, the bundle (with a fake
estimator), the task settings and the weight files are checked; no model is loaded.
"""

from __future__ import annotations

import fnmatch

import numpy as np
import pandas as pd
import pytest

from scripts.models import foundation_model_upload as up
from tests._notebook_loader import REPO_ROOT, load_eval_libs, load_foundation_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


class _ClassifierOnly:
    def fit(self, x, y):
        self.rate = float(np.mean(y))
        return self

    def predict_proba(self, x):
        p = np.clip(x[:, 0] / 10.0, 0, 1)
        return np.column_stack([1 - p, p])


class _RegressorWithoutOutputType:
    def fit(self, x, y):
        return self

    def predict(self, x):
        return x[:, 0] * 2.0


class _RegressorWithOutputType(_RegressorWithoutOutputType):
    def predict(self, x, output_type="mean"):
        return x[:, 0] * (3.0 if output_type == "median" else 2.0)


def _bundle(lib, kind, backend, estimator, monkeypatch, batch=2):
    frame = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [5.0, 6.0, 7.0, 8.0]})
    enc = lib["FeatureEncoder"](["a", "b"]).fit(frame)
    cfg = {"backend": backend, "kind": kind, "fill": False, "model_path": "unused"}
    monkeypatch.setitem(lib, "build_estimator", lambda cfg: estimator)
    monkeypatch.setitem(lib, "PREDICT_BATCH", batch)
    x = enc.transform(frame)
    return lib["TabularContextBundle"](cfg, enc, x, np.array([0, 1, 0, 1])), frame


# --- context rows ---------------------------------------------------------------


def test_all_rows_are_context_when_they_fit(lib):
    assert list(lib["context_positions"](5, 10, 1)) == [0, 1, 2, 3, 4]


def test_a_sample_is_seeded_sorted_and_the_size_of_the_limit(lib):
    first = lib["context_positions"](1000, 100, 42)
    again = lib["context_positions"](1000, 100, 42)
    other = lib["context_positions"](1000, 100, 43)
    assert len(first) == 100 and len(set(first)) == 100
    assert list(first) == sorted(first) == list(again)
    assert list(first) != list(other)
    assert first.max() < 1000


# --- bundle with a fake estimator -----------------------------------------------


def test_a_classification_bundle_returns_the_positive_probability_in_batches(
    lib, monkeypatch
):
    bundle, frame = _bundle(
        lib, "classification", "tabicl_v2", _ClassifierOnly(), monkeypatch
    )
    pred = bundle.predict(frame)
    assert pred.shape == (4,)
    assert list(pred) == pytest.approx([0.1, 0.2, 0.3, 0.4])
    assert bundle.task == "classification"


def test_a_regression_bundle_falls_back_when_the_estimator_has_no_output_type(
    lib, monkeypatch
):
    bundle, frame = _bundle(
        lib, "regression", "tabpfn_v2", _RegressorWithoutOutputType(), monkeypatch
    )
    assert list(bundle.predict(frame)) == [2.0, 4.0, 6.0, 8.0]


def test_a_tabpfn_regression_bundle_asks_for_the_median(lib, monkeypatch):
    bundle, frame = _bundle(
        lib, "regression", "tabpfn_25", _RegressorWithOutputType(), monkeypatch
    )
    assert list(bundle.predict(frame)) == [3.0, 6.0, 9.0, 12.0]


def test_the_stored_state_holds_no_estimator_but_the_context_rows(lib, monkeypatch):
    bundle, frame = _bundle(
        lib, "classification", "tabicl_v2", _ClassifierOnly(), monkeypatch
    )
    bundle.predict(frame)
    state = bundle.__getstate__()
    assert state["_estimator"] is None
    assert state["x"].shape == (4, 2) and state["x"].dtype == np.float32


# --- tasks, candidates and weight files -------------------------------------------


def test_every_tabular_task_is_registered_with_a_matching_kind(lib):
    for task_id, cfg in lib["TAB_TASKS"].items():
        task = lib["TASK_BY_ID"][task_id]
        assert (task["task_type"] == "classification") == (
            cfg["kind"] == "classification"
        ), task_id


def test_the_task_settings_agree_with_the_evaluation_specifications():
    ns = load_eval_libs()
    tab = load_foundation_libs()["TAB_TASKS"]
    for task_id, cfg in tab.items():
        theirs = ns["EVAL_TASKS"][task_id]["spec"]
        ours = cfg["spec"]
        for key in ("target", "key_cols", "id_features", "date_col"):
            assert ours.get(key) == theirs.get(key), (task_id, key)
        for key in ("kind", "cols"):
            assert ours.get("baseline", {}).get(key) == theirs.get("baseline", {}).get(
                key
            ), (task_id, key)


def test_the_foundation_tasks_are_those_of_every_foundation_library(lib):
    ids = lib["foundation_task_ids"]()
    expected = set()
    for name in lib["FOUNDATION_TASK_TABLES"]:
        expected |= set(lib[name])
    assert set(ids) == expected
    assert len(ids) == len(set(ids)) == 24


def test_every_candidate_has_weights_limits_and_a_fill_rule(lib):
    for _, backend, _ in lib["TAB_CANDIDATES"]:
        assert set(lib["TAB_WEIGHTS"][backend]) == {"classification", "regression"}
        assert backend in lib["TAB_LIMITS"] and backend in lib["TAB_FILL"]


def test_every_checkpoint_file_is_selected_by_the_upload_configuration(lib):
    models = {m["key"]: m for m in up.load_config()}
    for backend, kinds in lib["TAB_WEIGHTS"].items():
        for key, file in kinds.values():
            assert key in models, (backend, key)
            patterns = models[key]["include"]
            assert any(fnmatch.fnmatch(file, p) for p in patterns), (key, file)


def test_the_tabular_notebooks_exist_and_use_the_runner():
    root = REPO_ROOT / "databricks" / "models"
    for rel in (
        "foundation/02_foundation_tab_energy.py",
        "foundation/03_foundation_tab_commerce.py",
    ):
        text = (root / rel).read_text(encoding="utf-8")
        assert "run_foundation_tab(ctx, df)" in text
        assert 'dbutils.widgets.text("smoke"' in text
