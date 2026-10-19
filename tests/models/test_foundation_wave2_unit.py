"""Unit tests for the second set of foundation libraries under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The anomaly and reconstruction windows, the
survival and tier bundles, the episode arrays of the offline policy, the stacked bundle
and the task settings are checked on small constructed data with fake backends and
estimators; no model is loaded and no Spark call is made. The tests that need torch are
skipped when it is not installed.
"""

from __future__ import annotations

import types

import numpy as np
import pandas as pd
import pytest

from scripts.models import foundation_model_upload as up
from tests._notebook_loader import (
    load_approval_libs,
    load_eval_libs,
    load_foundation_libs,
)

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


@pytest.fixture(scope="module")
def evaluation():
    return load_eval_libs()["EVAL_TASKS"]


# --- task settings agree with the evaluation specifications ---------------------


def _same(ours, theirs, keys, task_id):
    for key in keys:
        assert ours.get(key) == theirs.get(key), (task_id, key)


def test_anomaly_and_reconstruction_settings_agree_with_the_evaluation(lib, evaluation):
    for task_id, cfg in lib["ANOMALY_TASKS"].items():
        keys = ("series_cols", "time_col", "target", "key_cols", "drop")
        _same(cfg["spec"], evaluation[task_id]["spec"], keys, task_id)
    for task_id, cfg in lib["RECON_TASKS"].items():
        assert cfg["spec"]["sets"] == evaluation[task_id]["spec"]["sets"]


def test_survival_tier_and_ranking_settings_agree_with_the_evaluation(lib, evaluation):
    for task_id, cfg in lib["SURVIVAL_TASKS"].items():
        keys = ("duration_col", "event_col", "key_cols", "group_col", "drop")
        _same(cfg["spec"], evaluation[task_id]["spec"], keys, task_id)
    for task_id, cfg in lib["TIER_TASKS"].items():
        keys = ("text_col", "target", "key_cols")
        _same(cfg["spec"], evaluation[task_id]["spec"], keys, task_id)
    for task_id, cfg in lib["RANK_TASKS"].items():
        assert cfg["spec"] == evaluation[task_id]["spec"]


def test_policy_settings_agree_with_the_evaluation(lib, evaluation):
    for task_id, cfg in lib["POLICY_TASKS"].items():
        theirs = evaluation[task_id]["spec"]
        assert {k: v for k, v in cfg["spec"].items()} == theirs, task_id
        assert evaluation[task_id].get("sample_users", False) == cfg.get(
            "sample_users", False
        )


def test_every_foundation_task_is_a_registered_task(lib):
    for task_id in lib["foundation_task_ids"]():
        assert task_id in lib["TASK_BY_ID"]


# --- candidate names and weight files ---------------------------------------------


def test_hub_candidates_are_foundation_models_and_trained_ones_are_not(lib):
    hub = []
    for table in ("CANDIDATES", "RECON_CANDIDATES", "SURV_CANDIDATES"):
        hub += [c[0] for c in lib[table]]
    hub += [c[0] for c in lib["STACK_CANDIDATES"]]
    assert all(lib["is_foundation_model"](n) for n in hub)
    for trained in (lib["SASREC_NAME"], lib["DT_NAME"]):
        assert not lib["is_foundation_model"](trained)


def test_every_backend_has_an_entry_in_the_upload_configuration(lib):
    keys = {m["key"] for m in up.load_config()}
    backends = {c[1] for c in lib["CANDIDATES"] + lib["RECON_CANDIDATES"]}
    assert backends <= keys
    assert {"survpfn_weights", "survpfn_code", "tabicl_v2", lib["STACK_MODEL"]} <= keys


def test_the_trained_families_are_not_foundation_families(lib):
    assert not lib["RANK_FAMILY"].startswith("foundation_")
    assert not lib["POLICY_FAMILY"].startswith("foundation_")
    for family in ("RECON_FAMILY", "SURV_FAMILY", "STACK_FAMILY"):
        assert lib[family].startswith("foundation_")


def test_the_energy_load_model_runs_on_the_load_task_only(lib):
    assert lib["CANDIDATE_TASKS"] == {"energy_ttm_zero_shot": ("load.load",)}
    backend = lib["EnergyTTMBackend"]
    assert (backend.context_length, backend.max_horizon) == (168, 24)
    assert {"toto2", "energy_ttm"} <= set(lib["BACKENDS"])


def test_the_model_cards_know_the_libraries_of_the_new_candidates(lib):
    names = []
    for table in ("RECON_CANDIDATES", "SURV_CANDIDATES", "STACK_CANDIDATES"):
        names += [c[0] for c in lib[table]]
    names += [lib["SASREC_NAME"], lib["DT_NAME"]]
    names += ["toto2_zero_shot", "energy_ttm_zero_shot"]
    known = load_approval_libs()["MODEL_LIBRARIES"]
    assert [n for n in names if n not in known] == []


# --- windows and reconstruction errors ----------------------------------------------


def test_windows_cover_every_row_and_the_last_one_ends_at_the_last_row(lib):
    assert lib["window_starts"](100) == [0]
    starts = lib["window_starts"](1100)
    assert starts == [0, 512, 588]
    covered = set()
    for s in starts:
        covered |= set(range(s, min(s + 512, 1100)))
    assert covered == set(range(1100))


def test_a_short_window_is_padded_on_the_left_and_a_gap_is_not_observed(lib):
    win, obs = lib["padded_window"]([1.0, np.nan, 3.0], 0)
    assert len(win) == len(obs) == 512
    assert list(win[-3:]) == [1.0, 0.0, 3.0] and list(obs[-3:]) == [1.0, 0.0, 1.0]
    assert obs[:-3].sum() == 0


class _OffsetBackend:
    """Reconstructs every value one unit too high."""

    def reconstruct(self, windows, observed):
        return windows + 1.0


def test_the_error_of_a_row_is_in_units_of_the_series_spread(lib):
    values = np.arange(1100, dtype="float64")
    errors = lib["series_errors"](_OffsetBackend(), values)
    assert errors.shape == (1100,)
    assert np.allclose(errors, 1.0 / values.std())


def test_a_missing_value_gets_no_error_and_other_rows_are_unchanged(lib):
    values = np.arange(50, dtype="float64")
    values[10] = np.nan
    errors = lib["series_errors"](_OffsetBackend(), values)
    assert np.isnan(errors[10]) and np.isfinite(np.delete(errors, 10)).all()


def test_sampled_windows_leave_the_other_rows_empty(lib):
    values = np.arange(5000, dtype="float64")
    errors = lib["series_errors"](_OffsetBackend(), values, max_windows=2)
    assert 0 < np.isfinite(errors).sum() <= 2 * 512


def test_the_scorer_scores_each_series_of_a_frame_on_its_own(lib):
    frame = pd.DataFrame(
        {
            "s": ["a"] * 20 + ["b"] * 20,
            "y": np.concatenate([np.arange(20.0), np.arange(20.0) * 10]),
        }
    )
    scorer = lib["ReconstructionScorer"]({"series_cols": ["s"], "target": "y"})
    scorer._backend = _OffsetBackend()
    scores = scorer.score(frame)
    # one unit of error in both series, divided by each own spread
    assert scores[:20] == pytest.approx(1.0 / np.arange(20.0).std())
    assert scores[20:] == pytest.approx(1.0 / (np.arange(20.0) * 10).std())
    assert scorer.__getstate__()["_backend"] is None


class _RecordingBackend:
    def __init__(self):
        self.windows = None
        self.observed = None

    def reconstruct(self, windows, observed):
        self.windows, self.observed = windows.copy(), observed.copy()
        return np.full(windows.shape, 7.0)


def test_a_masked_block_is_filled_from_the_window_that_ends_at_the_block(lib):
    arr = np.arange(100, dtype="float64")
    arr[40:] = 1e9  # nothing after the block may reach the model
    sset = types.SimpleNamespace(arrays=[arr[:, None]])
    recon = lib["BlockReconstructor"]({"backend": "moment", "weights_dir": "x"})
    backend = _RecordingBackend()
    recon._backend = backend
    out = recon.predict(sset, [(0, 30, 10)], [0])
    assert list(out) == [7.0] * 10
    assert backend.windows.max() < 1e9
    # the block is hidden, the 30 values before it are known
    assert backend.observed[0, -10:].sum() == 0
    assert backend.observed[0, -40:-10].sum() == 30
    assert list(backend.windows[0, -40:-10]) == list(range(30))


def test_the_blocks_of_several_events_come_back_in_order(lib):
    arr = np.arange(100, dtype="float64")[:, None]
    sset = types.SimpleNamespace(arrays=[arr, arr])
    recon = lib["BlockReconstructor"]({"backend": "moment", "weights_dir": "x"})
    recon._backend = _RecordingBackend()
    out = recon.predict(sset, [(0, 10, 3), (1, 50, 5)], [0, 0])
    assert len(out) == 8
    assert recon.predict(sset, [], []).shape == (0,)


# --- survival ------------------------------------------------------------------------


class _Step:
    def __init__(self, lo, hi, value):
        self.domain, self.value = (lo, hi), value

    def __call__(self, t):
        assert self.domain[0] <= t <= self.domain[1]
        return self.value


def test_a_step_function_is_read_inside_and_outside_its_domain(lib):
    fn = _Step(1.0, 4.0, 0.6)
    assert lib["step_value"](fn, 0.5) == 1.0
    assert lib["step_value"](fn, 3.0) == 0.6
    assert lib["step_value"](fn, 9.0) == 0.6


def test_person_interval_rows_follow_the_discrete_time_rule(lib):
    x = np.array([[1.0], [2.0], [3.0]])
    duration = np.array([0.5, 2.5, 5.0])
    event = np.array([1.0, 0.0, 1.0])
    rows, index, label = lib["person_period"](x, duration, event, 3)
    assert len(rows) == len(index) == len(label) == 6
    # the first unit dies in interval 0, the censored unit has two intervals
    got = zip(index.tolist(), label.tolist(), rows[:, 0].tolist(), strict=True)
    assert sorted(got) == [
        (0, 0, 2.0),
        (0, 0, 3.0),
        (0, 1, 1.0),
        (1, 0, 2.0),
        (1, 0, 3.0),
        (2, 0, 3.0),
    ]


class _HazardEstimator:
    """The hazard of interval k is 0.1 * (k + 1); k is the last column."""

    classes_ = np.array([0, 1])

    def fit(self, x, y):
        return self

    def predict_proba(self, x):
        h = 0.1 * (x[:, -1] + 1)
        return np.column_stack([1 - h, h])


def test_discrete_survival_multiplies_the_yearly_hazards(lib, monkeypatch):
    frame = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
    enc = lib["FeatureEncoder"](["a"]).fit(frame)
    monkeypatch.setitem(lib, "build_estimator", lambda cfg: _HazardEstimator())
    context = np.column_stack([np.zeros((4, 1)), np.arange(4)])
    bundle = lib["DiscreteSurvivalBundle"]({}, enc, context, [0, 0, 1, 0])
    surv = bundle.survival(frame)
    assert surv.shape == (3, 3)
    assert surv[0] == pytest.approx([0.9, 0.9 * 0.8 * 0.7, 0.9 * 0.8 * 0.7 * 0.6 * 0.5])
    assert bundle.risk(frame) == pytest.approx([1 - 0.504] * 3)
    assert bundle.__getstate__()["_estimator"] is None


class _SurvModel:
    def predict(self, x):
        return x[:, 0]

    def predict_survival_function(self, x):
        return [_Step(1.0, 4.0, 1.0 - 0.1 * v) for v in x[:, 0]]


def test_survpfn_reads_risk_and_the_curves_at_the_horizons(lib):
    frame = pd.DataFrame({"a": [1.0, 2.0]})
    enc = lib["FeatureEncoder"](["a"]).fit(frame)
    bundle = lib["SurvPFNBundle"]({}, enc, [[1.0]], [2.0], [1.0])
    bundle._model = _SurvModel()
    assert list(bundle.risk(frame)) == [1.0, 2.0]
    surv = bundle.survival(frame)
    # horizon 1 is the start of the domain, 3 and 5 lie inside or after it
    assert surv.shape == (2, 3)
    assert surv[1] == pytest.approx([0.8, 0.8, 0.8])
    assert bundle.__getstate__()["_model"] is None


# --- match tier -----------------------------------------------------------------------


class _TierEstimator:
    classes_ = np.array([0, 2])

    def fit(self, x, y):
        return self

    def predict_proba(self, x):
        p = np.clip(x[:, 0] / 10.0, 0, 1)
        return np.column_stack([1 - p, p])


def test_the_tier_bundle_returns_the_tier_names_of_the_classes_it_saw(lib, monkeypatch):
    frame = pd.DataFrame({"a": [1.0, 9.0, 2.0, 8.0]})
    enc = lib["FeatureEncoder"](["a"]).fit(frame)
    monkeypatch.setitem(lib, "build_estimator", lambda cfg: _TierEstimator())
    monkeypatch.setitem(lib, "PREDICT_BATCH", 3)
    names = ["exact", "fuzzy", "none"]
    bundle = lib["MulticlassContextBundle"](
        {"fill": False}, enc, enc.transform(frame), [0, 2, 0, 2], names
    )
    out = bundle.predict(frame)
    assert list(out) == ["exact", "none", "exact", "none"]
    assert bundle.classes == ["exact", "fuzzy", "none"]
    assert bundle.__getstate__()["_estimator"] is None


# --- episodes of the offline policy ----------------------------------------------


def test_each_row_gets_the_last_steps_of_its_own_episode(lib):
    windows = lib["episode_windows"](np.array([0, 0, 0, 1, 1]), 3)
    assert windows.tolist() == [
        [-1, -1, 0],
        [-1, 0, 1],
        [0, 1, 2],
        [-1, -1, 3],
        [-1, 3, 4],
    ]


def _episodes():
    # rows are not sorted; the original positions must come back
    return pd.DataFrame(
        {
            "ep": ["b", "a", "a", "b", "a"],
            "step": [2, 1, 2, 1, 3],
            "x": [5.0, 1.0, 2.0, 4.0, 3.0],
            "r": [1.0, 1.0, 2.0, 3.0, 4.0],
            "a": [10.0, 0.0, 1.0, 5.0, 2.0],
        }
    )


def _norms(classes=None, target=100.0):
    return {
        "mean": np.zeros(1),
        "std": np.ones(1),
        "classes": classes,
        "a_mean": 0.0,
        "a_std": 1.0,
        "target_return": target,
        "rtg_scale": 1.0,
    }


def _cols():
    return {"action": "a", "reward": "r", "episode": "ep", "step": "step"}


def test_the_return_asked_for_is_the_total_less_the_reward_so_far(lib):
    frame = _episodes()
    enc = lib["FeatureEncoder"](["x"]).fit(frame)
    prep = lib["policy_arrays"](frame, _cols(), enc, _norms(), True)
    # sorted: b1 b2 a1 a2 a3; episode b totals 4 and episode a totals 7
    assert prep["rtg"].tolist() == [4.0, 1.0, 7.0, 6.0, 4.0]
    asked = lib["policy_arrays"](frame, _cols(), enc, _norms(), False)
    assert asked["rtg"].tolist() == [100.0, 97.0, 100.0, 99.0, 97.0]


def test_the_original_order_is_restored(lib):
    frame = _episodes()
    enc = lib["FeatureEncoder"](["x"]).fit(frame)
    prep = lib["policy_arrays"](frame, _cols(), enc, _norms(), True)
    assert prep["state"][prep["back"], 0].tolist() == frame["x"].tolist()


def test_a_class_action_is_a_one_based_index_and_an_unknown_class_is_zero(lib):
    frame = _episodes().assign(a=["up", "down", "up", "none", "up"])
    enc = lib["FeatureEncoder"](["x"]).fit(frame)
    prep = lib["policy_arrays"](frame, _cols(), enc, _norms(["down", "up"]), True)
    assert prep["action"].tolist() == [0, 2, 1, 2, 2]


def test_the_gathered_window_is_zero_where_the_episode_has_no_step(lib):
    frame = _episodes()
    enc = lib["FeatureEncoder"](["x"]).fit(frame)
    prep = lib["policy_arrays"](frame, _cols(), enc, _norms(), True)
    rtg, state, ok, action = lib["gather_window"](prep, np.array([0]))
    assert ok.shape == (1, 10) and ok.sum() == 1 and ok[0, -1] == 1
    assert rtg[0, :-1].sum() == 0 and state[0, :-1].sum() == 0
    assert action[0, :-1].sum() == 0


def test_the_transformer_ignores_the_action_of_the_current_step(lib, monkeypatch):
    pytest.importorskip("torch")
    rng = np.random.default_rng(0)
    n = 60
    frame = pd.DataFrame(
        {
            "ep": np.repeat(np.arange(6), 10),
            "step": np.tile(np.arange(10), 6),
            "x": rng.normal(size=n),
            "r": rng.normal(size=n),
            "a": rng.normal(size=n),
        }
    )
    monkeypatch.setitem(lib, "DT_EPOCHS", 1)
    policy, params = lib["fit_decision_transformer"](
        frame, "pumped", {}, ["x"], _cols()
    )
    base = policy.action(frame)
    changed = frame.copy()
    changed.loc[25, "a"] += 50.0  # the current action of one row
    after = policy.action(changed)
    # rows after row 25 in the same episode read it as history; row 25 and the rows
    # before it must not move
    assert after[:26] == pytest.approx(base[:26])
    assert params["train_rows"] == n and len(base) == n


def test_the_sasrec_network_scores_every_item(lib):
    torch = pytest.importorskip("torch")
    net = lib["_make_sasrec"](vocab=20)
    out = net(torch.zeros(4, lib["SEQ_LENGTH"], dtype=torch.long))
    assert out.shape == (4, 21)


# --- stacking ------------------------------------------------------------------


class _Reference:
    def __init__(self):
        self.last_report = {"items": 1, "rows_without_context": 0, "rule_violations": 0}

    def predict(self, frame):
        return frame["base"].to_numpy() * 2.0


class _Tree:
    def predict(self, x):
        return x[:, -1] + 1.0


def test_the_stacked_bundle_adds_the_forecast_as_the_last_feature(lib):
    frame = pd.DataFrame({"a": [1.0, 2.0], "base": [10.0, 20.0]})
    enc = lib["FeatureEncoder"](["a", lib["STACK_FEATURE"]])
    enc.kinds = {"a": "num", lib["STACK_FEATURE"]: "num"}
    bundle = lib["StackedForecastBundle"](_Reference(), enc, _Tree())
    assert list(bundle.predict(frame)) == [21.0, 41.0]
    assert bundle.task == "regression"


# --- the upload command: a GitHub source and the licence of the card -------------


class _Reply:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _model(licence):
    return {
        "key": "a",
        "repo_id": "o/a",
        "revision": "r",
        "licence": licence,
        "include": ["*"],
    }


class _Web:
    def __init__(self, payloads):
        self.payloads, self.urls = payloads, []

    def get(self, url, **_):
        self.urls.append(url)
        for part, payload in self.payloads.items():
            if part in url:
                return _Reply(payload)
        raise AssertionError(url)


def test_a_github_model_is_pinned_and_listed_without_the_hub_token(monkeypatch):
    web = _Web(
        {
            "/commits/main": {"sha": "abc"},
            "/git/trees/abc": {
                "tree": [
                    {"type": "blob", "path": "src/SurvPFN.py", "size": 5},
                    {"type": "tree", "path": "src"},
                ]
            },
        }
    )
    monkeypatch.setattr(up, "plain_session", lambda: web)
    hub = _Web({})
    assert up.latest_revision(hub, "o/r", "main", "github") == "abc"
    tree = up.list_tree(hub, "o/r", "abc", "github")
    assert tree == [{"type": "file", "path": "src/SurvPFN.py", "size": 5}]
    assert hub.urls == [] and len(web.urls) == 2


def test_the_licence_is_read_from_the_card_or_from_a_tag():
    card = _Web({"/api/models/o/a": {"cardData": {"license": "mit"}}})
    assert up.hub_licence(card, "o/a") == "mit"
    tags = _Web({"/api/models/o/b": {"tags": ["x", "license:apache-2.0"]}})
    assert up.hub_licence(tags, "o/b") == "apache-2.0"
    assert up.hub_licence(_Web({"/api/models/o/c": {}}), "o/c") == ""


def test_a_model_with_a_hub_licence_stops_when_the_card_names_none(monkeypatch):
    session = _Web({"/api/models/o/a": {}})
    model = _model("hub")
    with pytest.raises(ValueError, match="names no licence"):
        up.plan_model(session, model, None)


def test_a_hub_model_with_only_json_files_is_refused():
    tree = [{"type": "file", "path": "config.json", "size": 1}]
    session = _Web({"/tree/r": tree})
    model = {**_model("mit"), "include": ["*.json"]}
    with pytest.raises(ValueError, match="no weight file"):
        up.plan_model(session, model, None)


def test_the_resolved_licence_goes_into_the_plan(monkeypatch):
    tree = [{"type": "file", "path": "model.safetensors", "size": 3}]
    card = {"cardData": {"license": "mit"}}
    session = _Web({"/tree/r": tree, "/api/models/o/a": card})
    model = _model("hub")
    plan = up.plan_model(session, model, None)
    assert plan["model"]["licence"] == "mit" and len(plan["files"]) == 1


class _Failing:
    def get(self, url, **_):
        raise RuntimeError("401 Unauthorized")


def test_a_repository_that_cannot_be_read_is_named_in_the_error():
    model = {"key": "a", "repo_id": "o/a", "ref": "main", "revision": None}
    with pytest.raises(ValueError, match=r"a: cannot read o/a@main: 401"):
        up.pin_revisions(_Failing(), [model])


def test_the_energy_models_come_from_the_public_energyfm_repositories():
    models = {m["key"]: m for m in up.load_config()}
    assert models["energy_ttm"]["repo_id"] == "EnergyFM/energy-ttm"
    assert models["energy_tspulse"]["repo_id"] == "EnergyFM/energy-tspulse"
