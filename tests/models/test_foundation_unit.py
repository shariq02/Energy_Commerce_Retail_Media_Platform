"""Unit tests for the foundation time-series library under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The forecast plan, the context rule, the
reference bundle (with a fake backend and a given history) and the findings text are
checked on small constructed series; no model is loaded and no Spark call is made.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tests._notebook_loader import REPO_ROOT, load_eval_libs, load_foundation_libs

pytestmark = [pytest.mark.unit]

_MODELS = REPO_ROOT / "databricks" / "models"


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


def _days(n, start="2025-01-01"):
    return pd.date_range(start, periods=n, freq="D")


# --- grid and series ids -----------------------------------------------------


def test_grid_index_counts_steps_since_the_epoch(lib):
    days = lib["grid_index"](["1970-01-02", "1970-01-04"], 86400)
    assert list(days) == [1.0, 3.0]
    quarter = lib["grid_index"](["1970-01-01 00:30:00+00:00"], 900)
    assert list(quarter) == [2.0]


def test_an_unreadable_timestamp_gives_nan(lib):
    assert np.isnan(lib["grid_index"](["not a date"], 86400)[0])


def test_series_ids_join_the_key_columns(lib):
    frame = pd.DataFrame({"a": ["x", "y"], "b": [1, 2]})
    assert list(lib["series_ids"](frame, ["a", "b"])) == ["x|1", "y|2"]


def test_fill_gaps_fills_both_ends_and_refuses_an_empty_series(lib):
    out = lib["fill_gaps"]([np.nan, 1.0, np.nan, 3.0, np.nan])
    assert list(out) == [1.0, 1.0, 1.0, 3.0, 3.0]
    assert lib["fill_gaps"]([np.nan, np.nan]) is None


# --- forecast plan -------------------------------------------------------------


def _known(values, sid="s"):
    return pd.DataFrame(
        {"sid": sid, "k": np.arange(len(values), dtype="float64"), "y": values}
    )


def _wanted(ks, sid="s"):
    return pd.DataFrame(
        {"sid": sid, "k": np.asarray(ks, dtype="float64"), "pos": np.arange(len(ks))}
    )


def test_a_daily_row_gets_the_context_that_ends_at_its_time_minus_the_gap(lib):
    values = np.arange(20, dtype="float64")
    plan = lib["plan_forecasts"](
        _known(values),
        _wanted([10, 11]),
        gap_steps=2,
        block=1,
        context=5,
        min_values=3,
    )
    assert plan["horizon"] == 2
    assert list(plan["step"]) == [2, 2]
    assert list(plan["contexts"][0]) == [4.0, 5.0, 6.0, 7.0, 8.0]
    assert list(plan["contexts"][1]) == [5.0, 6.0, 7.0, 8.0, 9.0]
    assert plan["violations"] == 0


def test_no_value_after_the_origin_reaches_the_context(lib):
    values = np.concatenate([np.arange(11, dtype="float64"), np.full(9, 1e9)])
    plan = lib["plan_forecasts"](
        _known(values), _wanted([12]), gap_steps=2, block=1, context=50
    )
    assert plan["contexts"][0].max() == 10.0


def test_rows_of_one_day_share_one_item_and_the_horizon_covers_the_day(lib):
    values = np.arange(400, dtype="float64")
    ks = [288, 289, 383]
    plan = lib["plan_forecasts"](
        _known(values), _wanted(ks), gap_steps=192, block=96, context=100
    )
    assert plan["horizon"] == 192 + 95
    assert len(set(plan["item"])) == 1
    assert list(plan["step"]) == [192, 193, 287]
    assert plan["contexts"][0][-1] == 96.0


def test_too_little_history_gives_no_context(lib):
    plan = lib["plan_forecasts"](
        _known(np.arange(5, dtype="float64")),
        _wanted([4]),
        gap_steps=2,
        block=1,
        context=10,
        min_values=8,
    )
    assert plan["contexts"] == [None]


def test_a_row_before_the_first_known_value_gives_no_context(lib):
    values = np.arange(30, dtype="float64")
    known = _known(values)
    known["k"] += 100
    plan = lib["plan_forecasts"](
        known, _wanted([101]), gap_steps=2, block=1, context=10
    )
    assert plan["contexts"] == [None]


def test_assemble_places_each_step_on_its_row_and_leaves_the_rest_empty(lib):
    plan = {
        "contexts": [np.zeros(3), None],
        "pos": np.array([0, 1, 2]),
        "item": np.array([0, 0, 1]),
        "step": np.array([1, 2, 1]),
        "horizon": 2,
    }
    got = lib["assemble_forecasts"](plan, {0.5: np.array([[10.0, 20.0]])}, 3, (0.5,))
    assert got[0.5][:2].tolist() == [10.0, 20.0]
    assert np.isnan(got[0.5][2])


# --- reference bundle with a fake backend ---------------------------------------


class _Fake:
    name = "fake"
    max_horizon = 1000

    def forecast(self, contexts, horizon, levels, seconds):
        base = np.array([[c[-1] + s + 1 for s in range(horizon)] for c in contexts])
        return {q: base + (q - 0.5) for q in levels}


def _config(**changes):
    cfg = {
        "task_id": "t",
        "backend": "chronos2",
        "series": ["area"],
        "time": "day",
        "target": "y",
        "seconds": 86400,
        "seconds_col": None,
        "gap_days": 2,
        "context": 10,
        "history_table": "unused",
        "frozen_version": 0,
        "has_quantiles": False,
        "quantiles": [0.1, 0.5, 0.9],
        "weights_dir": "",
        "adapter_dir": None,
    }
    cfg.update(changes)
    return cfg


def _history():
    days = _days(30)
    return pd.DataFrame({"area": "a", "day": days, "y": np.arange(30, dtype="float64")})


def test_the_bundle_forecasts_each_row_from_the_value_two_days_back(lib):
    bundle = lib["ForecastReference"](_config())
    bundle._backend = _Fake()
    frame = pd.DataFrame({"area": "a", "day": _days(3, "2025-01-20"), "y": np.nan})
    pred = bundle.predict(frame, history=_history())
    # the day index is the value: day 19 is forecast from day 17 plus two steps
    assert list(pred) == [19.0, 20.0, 21.0]
    assert bundle.last_report["rule_violations"] == 0
    assert bundle.last_report["rows_without_context"] == 0


def test_a_target_in_the_frame_serves_as_context_for_later_rows(lib):
    bundle = lib["ForecastReference"](_config())
    bundle._backend = _Fake()
    hist = _history().iloc[:20]
    frame = pd.DataFrame(
        {
            "area": "a",
            "day": _days(4, "2025-01-21"),
            "y": [20.0, 21.0, 22.0, 23.0],
        }
    )
    pred = bundle.predict(frame, history=hist)
    assert list(pred) == [20.0, 21.0, 22.0, 23.0]


def test_a_quantile_bundle_returns_sorted_levels_for_every_row(lib):
    bundle = lib["ForecastReference"](_config(has_quantiles=True))
    bundle._backend = _Fake()
    frame = pd.DataFrame({"area": "a", "day": _days(2, "2025-01-20"), "y": np.nan})
    pred = bundle.predict(frame, history=_history())
    assert bundle.task == "quantile"
    assert sorted(pred) == [0.1, 0.5, 0.9]
    assert np.all(pred[0.1] <= pred[0.5]) and np.all(pred[0.5] <= pred[0.9])


def test_rows_without_history_are_counted_not_guessed(lib):
    bundle = lib["ForecastReference"](_config())
    bundle._backend = _Fake()
    frame = pd.DataFrame({"area": "other", "day": _days(2, "2025-01-20"), "y": np.nan})
    pred = bundle.predict(frame, history=_history())
    assert np.isnan(pred).all()
    assert bundle.last_report["rows_without_context"] == 2


def test_a_horizon_over_the_backend_limit_is_refused(lib):
    bundle = lib["ForecastReference"](_config())
    backend = _Fake()
    backend.max_horizon = 1
    bundle._backend = backend
    frame = pd.DataFrame({"area": "a", "day": _days(1, "2025-01-20"), "y": np.nan})
    with pytest.raises(ValueError, match="over the limit"):
        bundle.predict(frame, history=_history())


def test_the_stored_state_holds_no_backend_and_no_history(lib):
    bundle = lib["ForecastReference"](_config())
    bundle._backend = _Fake()
    bundle._history = _history()
    state = bundle.__getstate__()
    assert state["_backend"] is None and state["_history"] is None
    assert state["cfg"]["backend"] == "chronos2"


def test_a_history_read_may_only_name_train_and_validation(lib):
    with pytest.raises(RuntimeError, match="train and validation"):
        lib["read_history"]("t", 0, ["a"], ("train", "holdout"))


# --- tasks, tables and the findings text ------------------------------------------


def test_every_time_series_task_is_a_registered_energy_task(lib):
    for task_id in lib["TS_TASKS"]:
        assert lib["TASK_BY_ID"][task_id]["ecosystem"] == "energy"


def test_the_task_settings_agree_with_the_evaluation_specifications():
    ns = load_eval_libs()
    ts = load_foundation_libs()["TS_TASKS"]
    for task_id, cfg in ts.items():
        theirs = ns["EVAL_TASKS"][task_id]["spec"]
        ours = cfg["spec"]
        for key in ("target", "key_cols", "id_features", "price", "date_col"):
            assert ours.get(key) == theirs.get(key), (task_id, key)
        for key in ("kind", "column", "cols", "denominator"):
            assert ours["baseline"].get(key) == theirs["baseline"].get(key), (
                task_id,
                key,
            )


def test_the_series_and_time_columns_are_in_the_key_columns(lib):
    for task_id, cfg in lib["TS_TASKS"].items():
        keys = cfg["spec"]["key_cols"]
        assert set(cfg["series"]) <= set(keys), task_id
        assert cfg["time"] in keys, task_id


def test_every_candidate_names_a_known_backend_and_model_key(lib):
    for _, backend, _, _ in lib["CANDIDATES"]:
        assert backend in lib["BACKENDS"]


def test_foundation_results_use_the_columns_of_the_accepted_results(lib):
    ddl = lib["FOUNDATION_DDL"]
    assert ddl["foundation_results"] == lib["RESULT_COLUMNS"]
    assert ddl["foundation_run_context"] == lib["RUN_CONTEXT_COLUMNS"]
    assert ddl["foundation_selection"] == lib["MODEL_DDL"]["candidate_selection"]
    assert not set(ddl) & set(lib["MODEL_DDL"])


def test_the_foundation_context_writes_to_its_own_tables(lib):
    cls = lib["FoundationContext"]
    assert cls.results_table == "foundation_results"
    assert cls.context_table == "foundation_run_context"
    assert lib["FOUNDATION_RANK"] == lib["FORWARD_TOP_N"] + 1


def test_findings_name_the_models_and_every_task(lib):
    rows = [
        {
            "task_id": "load.load",
            "model_name": "chronos2_zero_shot",
            "family": "foundation_ts",
            "stage": "candidate",
            "status": "ok",
            "metric": "mae",
            "value": 5.0,
            "n_rows": 10,
            "detail": "",
            "frozen_delta_version": 0,
            "artifact_status": "ok",
            "run_id": "r",
            "run_at": "2026-10-18",
        },
        {
            "task_id": "load.load",
            "model_name": "seasonal_mean",
            "family": "baseline",
            "stage": "baseline",
            "status": "ok",
            "metric": "mae",
            "value": 9.0,
            "n_rows": 10,
            "detail": "",
            "frozen_delta_version": 0,
            "artifact_status": "none",
            "run_id": "r",
            "run_at": "2026-10-18",
        },
    ]
    register = [
        {
            "model_key": "chronos2",
            "repo_id": "amazon/chronos-2",
            "revision": "abc123def4567",
            "licence": "apache-2.0",
            "licence_restriction": "none",
        }
    ]
    text = lib["render_foundation_findings"](
        "energy", rows, [], [], register, [], smoke=False, stamp="now"
    )
    assert "chronos2_zero_shot" in text and "amazon/chronos-2" in text
    assert text.count("\n## ") >= len(lib["TS_TASKS"])
    assert "_no results recorded_" in text


def test_the_foundation_notebooks_and_gates_exist():
    for rel in (
        "foundation/00_foundation_setup.py",
        "foundation/01_foundation_ts_energy.py",
        "foundation/02_foundation_tab_energy.py",
        "foundation/03_foundation_tab_commerce.py",
        "foundation/04_foundation_anomaly_energy.py",
        "foundation/05_foundation_survival_matching_energy.py",
        "foundation/06_foundation_rl_energy.py",
        "foundation/07_foundation_sequence_commerce.py",
        "foundation/08_foundation_stacking_energy.py",
        "lib/_fit_foundation_tab.py",
        "lib/_fit_foundation_anomaly.py",
        "lib/_fit_foundation_survival.py",
        "lib/_fit_foundation_rank.py",
        "lib/_fit_foundation_rl.py",
        "lib/_fit_foundation_stack.py",
        "gate/04_foundation_guards.py",
        "gate/05_foundation_selection.py",
        "gate/06_foundation_findings.py",
        "evaluate/gate/04_foundation_contamination.py",
        "register/03_foundation_references.py",
        "operate/04_score_request.py",
        "lib/_fit_foundation_ts.py",
    ):
        assert (_MODELS / rel).is_file(), rel


# --- findings section of one notebook ------------------------------------------


def _block(key, stamp):
    return f"<!-- BEGIN {key} -->\n## {key}\n\n_Generated: {stamp}_\n\nbody\n<!-- END {key} -->"


def test_a_rerun_replaces_its_own_section_and_keeps_the_others(lib):
    import datetime

    now = datetime.datetime(2026, 10, 20, 12, 0, tzinfo=datetime.UTC)
    old = (
        _block("01_a", "2026-10-20T11:00Z")
        + "\n\n"
        + _block("02_b", "2026-10-20T11:00Z")
    )
    new = _block("01_a", "2026-10-20T12:00Z")
    out = lib["merge_findings_sections"](old, "01_a", new, now)
    assert out.count("<!-- BEGIN") == 2
    assert "2026-10-20T12:00Z" in out and "STALE" not in out
    assert out.index("01_a") < out.index("02_b")


def test_a_section_older_than_a_day_is_marked_stale(lib):
    import datetime

    now = datetime.datetime(2026, 10, 20, 12, 0, tzinfo=datetime.UTC)
    old = _block("02_b", "2026-10-18T11:00Z")
    out = lib["merge_findings_sections"](
        old, "01_a", _block("01_a", "2026-10-20T12:00Z"), now
    )
    stale = out.split("<!-- BEGIN 02_b -->")[1]
    assert "> STALE" in stale
    assert "> STALE" not in out.split("<!-- BEGIN 02_b -->")[0]


def test_the_section_body_drops_the_title_and_lowers_the_headings(lib):
    text = "# T\n\n_Auto-generated_\n\n## summary\n\nrow\n### candidates\n"
    body = lib["_section_body"](text)
    assert body.splitlines()[0] == "### summary"
    assert "### candidates" not in body.splitlines()[0]
    assert "#### candidates" in body and "# T" not in body


def test_every_foundation_notebook_writes_its_findings_for_its_own_tasks(lib):
    import re

    tasks = {t["task_id"]: t["ecosystem"] for t in lib["TASKS"]}
    for path in sorted((_MODELS / "foundation").glob("0[1-8]_*.py")):
        text = path.read_text(encoding="utf-8")
        used = set(re.findall(r'FoundationContext\("([\w.]+)"', text))
        call = text.split("write_foundation_findings(")[1]
        eco = re.search(r'"(energy|commerce)"', call).group(1)
        named = set(re.findall(r'"([a-z_0-9]+\.[a-z_0-9]+)"', call))
        assert named == used, path.name
        assert {tasks[t] for t in named} == {eco}, path.name


# --- training byproducts stay out of the repository ----------------------------


def test_every_training_call_of_the_models_libraries_names_an_output_folder():
    import re

    for path in sorted((_MODELS / "lib").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"\.fit\(\s*inputs=", text):
            call = text[match.start() : text.index("\n        )", match.start())]
            assert "output_dir=" in call, path.name
