"""Unit tests for the candidate guard and the prediction batch size under `databricks/models/lib`.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loaded through `tests/_notebook_loader.py`. The time limit, the stack print and the
recorded outcome of a slow candidate are checked with fake record functions; no Spark
session and no model is used.
"""

from __future__ import annotations

import signal
import time
from types import SimpleNamespace

import pytest

from tests._notebook_loader import load_foundation_libs

pytestmark = [pytest.mark.unit]


@pytest.fixture(scope="module")
def lib():
    return load_foundation_libs()


@pytest.fixture
def recorded(lib, monkeypatch):
    rows = []
    monkeypatch.setitem(lib, "missing_libraries", lambda requires: [])
    monkeypatch.setitem(lib, "record_result", lambda *args: rows.append(args))
    monkeypatch.setitem(lib, "log_candidate_to_mlflow", lambda *args: ("run-1", "ok"))
    return rows


def _ctx():
    return SimpleNamespace(task_id="task.one")


# --- limit by family --------------------------------------------------------------


def test_foundation_families_get_the_longer_limit(lib):
    assert lib["candidate_limit"]("foundation_tab") == 1800
    assert lib["candidate_limit"]("foundation_surv") == 1800


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
    before = signal.getsignal(signal.SIGALRM)
    with lib["candidate_guard"]("fast one", 5, stack_seconds=0):
        value = 1 + 1
    assert value == 2
    assert signal.alarm(0) == 0
    assert signal.getsignal(signal.SIGALRM) == before


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
        _ctx(), "slow_model", "foundation_tab", slow, limit_seconds=1
    )
    assert result is None
    assert len(recorded) == 1
    row = recorded[0]
    assert row[1] == "slow_model" and row[4] == "failed"
    assert "TimeoutError" in row[7] and "timeout after 1 s" in row[7]
    out = capsys.readouterr().out
    assert "START slow_model [foundation_tab] task=task.one limit=1 s" in out
    assert "FAIL slow_model" in out


def test_a_candidate_inside_the_limit_is_recorded_as_ok(lib, recorded):
    def fast():
        return object(), {"pr_auc": 0.5}, 7

    metrics = lib["run_candidate"](
        _ctx(), "fast_model", "foundation_tab", fast, limit_seconds=5
    )
    assert metrics == {"pr_auc": 0.5}
    assert [row[4] for row in recorded] == ["ok"]


def test_the_default_limit_comes_from_the_family(lib, recorded, capsys):
    lib["run_candidate"](
        _ctx(), "m", "foundation_ts", lambda: (object(), {"x": 1.0}, 1)
    )
    assert "limit=1800 s" in capsys.readouterr().out


# --- prediction batch size ----------------------------------------------------------


def test_a_small_context_keeps_the_full_batch(lib):
    assert lib["predict_batch"](500) == 10_000
    assert lib["predict_batch"](10_000) == 10_000


def test_a_larger_context_gets_a_smaller_batch(lib):
    assert lib["predict_batch"](20_000) == 5_000
    assert lib["predict_batch"](50_000) == 2_000


def test_the_batch_never_falls_under_the_minimum(lib):
    assert lib["predict_batch"](10_000_000) == lib["MIN_PREDICT_BATCH"]


def test_the_context_of_tabpfn_25_is_capped_under_the_library_maximum(lib):
    assert lib["TAB_LIMITS"]["tabpfn_25"]["max_rows"] == 20_000
