"""Unit tests for the registry libraries under `databricks/models/lib`.

The entry rules, the version planner, the reproducibility outcome and the findings
text are plain Python; each test builds the recorded rows by hand."""

import datetime as dt
import json
import platform

import pytest

from tests._notebook_loader import load_registry_libs

pytestmark = [pytest.mark.schema, pytest.mark.unit]

NOW = dt.datetime(2026, 10, 5, 12, 0, tzinfo=dt.UTC)
LATER = dt.datetime(2026, 10, 6, 12, 0, tzinfo=dt.UTC)


@pytest.fixture(scope="module")
def lib():
    return load_registry_libs()


def _columns(ddl: str) -> list:
    return [part.split()[0] for part in ddl.split(", ")]


def _approval(task_id, model, role, decision="approved", run="run-1", by="owner"):
    return {
        "task_id": task_id,
        "dataset_id": "dataset",
        "model_name": model,
        "role": role,
        "decision": decision,
        "restriction": None,
        "conditions": "[]",
        "primary_metric": "mae",
        "validation_value": 1.5,
        "frozen_delta_version": 3,
        "mlflow_run_id": run,
        "run_id": "model-approval-1",
        "decided_at": NOW,
        "decided_by": by,
    }


def _task(lib):
    return lib["TASKS"][0]["task_id"]


def _approvals(lib, **kw):
    tid = _task(lib)
    return [
        _approval(tid, "cand", "selected", **kw),
        _approval(tid, "base", "baseline_fallback", run="run-0"),
        _approval(tid, "other", "runner_up", decision="not_approved", run="run-2"),
    ]


def test_only_the_approved_model_and_its_fallback_are_entries(lib):
    entries = lib["registry_entries"](_approvals(lib), {})
    assert [(e["model_name"], e["role"]) for e in entries] == [
        ("cand", "selected"),
        ("base", "baseline_fallback"),
    ]


@pytest.mark.parametrize("decision", ["not_approved", "deferred"])
def test_a_task_that_is_not_approved_has_no_entry(lib, decision):
    assert lib["registry_entries"](_approvals(lib, decision=decision), {}) == []


def test_a_decision_not_made_by_the_owner_gives_no_entry(lib):
    assert lib["registry_entries"](_approvals(lib, by="rule_engine"), {}) == []


def test_an_entry_links_run_artifact_card_and_fit_environment(lib):
    tid = _task(lib)
    eco = lib["TASK_BY_ID"][tid]["ecosystem"]
    entry = lib["registry_entries"](_approvals(lib), {tid: '{"torch": "2"}'})[0]
    assert entry["artifact_uri"] == "runs:/run-1/candidate/candidate.pkl"
    assert entry["card_path"] == f"src/schemas/model_cards/{eco}/{tid}.md"
    assert entry["approval_run_id"] == "model-approval-1"
    assert entry["fit_library_versions"] == '{"torch": "2"}'


def test_a_first_registration_is_version_one(lib):
    wanted = lib["registry_entries"](_approvals(lib), {})
    new = lib["plan_versions"]([], wanted, {}, "rid-1", NOW)
    assert [r["version"] for r in new] == [1, 1]
    assert {r["lifecycle_status"] for r in new} == {"registered"}
    assert {r["run_id"] for r in new} == {"rid-1"}


def test_the_new_rows_have_exactly_the_registry_columns(lib):
    wanted = lib["registry_entries"](_approvals(lib), {})
    new = lib["plan_versions"]([], wanted, {}, "rid-1", NOW)
    assert set(new[0]) == set(_columns(lib["MODEL_DDL"]["model_registry"]))


def test_registering_twice_adds_nothing(lib):
    wanted = lib["registry_entries"](_approvals(lib), {})
    first = lib["plan_versions"]([], wanted, {}, "rid-1", NOW)
    assert lib["plan_versions"](first, wanted, {}, "rid-2", LATER) == []


def test_a_changed_link_creates_the_next_version_and_keeps_the_old_row(lib):
    first = lib["plan_versions"](
        [], lib["registry_entries"](_approvals(lib), {}), {}, "rid-1", NOW
    )
    moved = lib["registry_entries"](_approvals(lib, run="run-9"), {})
    new = lib["plan_versions"](first, moved, {}, "rid-2", LATER)
    assert [(r["model_name"], r["version"]) for r in new] == [("cand", 2)]
    assert "mlflow_run_id" in new[0]["reason"]
    assert first[0]["mlflow_run_id"] == "run-1"


def test_a_lifecycle_change_needs_a_reason_and_a_registered_entry(lib):
    tid = _task(lib)
    first = lib["plan_versions"](
        [], lib["registry_entries"](_approvals(lib), {}), {}, "rid-1", NOW
    )
    key = (tid, "cand", "selected")
    new = lib["plan_versions"](
        first, [], {key: {"status": "deprecated", "reason": "replaced"}}, "r2", LATER
    )
    assert (new[0]["version"], new[0]["lifecycle_status"]) == (2, "deprecated")
    assert new[0]["reason"] == "replaced"
    with pytest.raises(ValueError, match="reason"):
        lib["plan_versions"](
            first, [], {key: {"status": "retired", "reason": " "}}, "r2", LATER
        )
    with pytest.raises(ValueError, match="not registered"):
        lib["plan_versions"](
            [], [], {key: {"status": "retired", "reason": "x"}}, "r2", LATER
        )
    with pytest.raises(ValueError, match="not valid"):
        lib["plan_versions"](
            first, [], {key: {"status": "gone", "reason": "x"}}, "r2", LATER
        )


def test_the_latest_version_of_each_entry_is_found(lib):
    rows = [
        {"task_id": "t", "model_name": "m", "role": "selected", "version": v}
        for v in (1, 3, 2)
    ]
    assert lib["latest_versions"](rows)[("t", "m", "selected")]["version"] == 3


@pytest.mark.parametrize(
    ("recorded", "rescored", "comparable", "expected"),
    [
        (1.0, 1.0, True, "matched"),
        (1.0, 1.0 + 1e-9, True, "matched"),
        (1.0, 1.01, True, "not_matched"),
        (None, 1.0, True, "no_recorded_value"),
        (float("nan"), 1.0, True, "no_recorded_value"),
        (1.0, None, True, "score_failed"),
        (1.0, float("inf"), True, "score_failed"),
        (1.0, 5.0, False, "not_comparable"),
    ],
)
def test_reproduction_outcome(lib, recorded, rescored, comparable, expected):
    status, _gap = lib["reproduction_outcome"](recorded, rescored, 1e-6, comparable)
    assert status == expected


def test_the_gap_is_relative_to_the_recorded_value(lib):
    _status, gap = lib["reproduction_outcome"](200.0, 202.0, 1e-6)
    assert gap == pytest.approx(0.01)


@pytest.mark.parametrize(
    ("reload", "rescore", "expected"),
    [
        ("ok", "matched", "passed"),
        ("ok", "not_comparable", "passed"),
        ("ok", "not_matched", "failed"),
        ("ok", "no_recorded_value", "failed"),
        ("failed", "matched", "failed"),
    ],
)
def test_a_check_passes_only_when_the_model_reloads_and_reproduces(
    lib, reload, rescore, expected
):
    assert lib["check_status"](reload, rescore) == expected


def test_the_environment_record_names_the_processor_type(lib):
    env = lib["environment_record"]()
    assert env["processor_type"] == platform.machine()
    assert env["python_version"] == platform.python_version()
    assert set(json.loads(env["library_versions"])) == set(lib["LIBRARIES"])


def test_a_check_row_has_exactly_the_check_columns(lib):
    entry = {
        "task_id": "t",
        "model_name": "m",
        "role": "selected",
        "version": 1,
        "validation_value": 1.0,
    }
    env = {"processor_type": "x86_64", "python_version": "3", "library_versions": "{}"}
    row = lib["check_row"](entry, env, "rid", NOW, status="passed")
    assert set(row) == set(_columns(lib["MODEL_DDL"]["registry_check"]))
    assert row["status"] == "passed" and row["recorded_value"] == 1.0


def test_the_latest_check_of_each_processor_type_is_kept(lib):
    def row(proc, at, status):
        return {
            "task_id": "t",
            "model_name": "m",
            "role": "selected",
            "processor_type": proc,
            "checked_at": at,
            "status": status,
        }

    rows = [
        row("x86_64", NOW, "failed"),
        row("x86_64", LATER, "passed"),
        row("aarch64", NOW, "passed"),
    ]
    kept = lib["latest_checks"](rows)
    assert len(kept) == 2
    assert kept[("t", "m", "selected", "x86_64")]["status"] == "passed"


def _findings(lib):
    tid = _task(lib)
    registry = lib["plan_versions"](
        [], lib["registry_entries"](_approvals(lib), {}), {}, "rid-1", NOW
    )
    env = {
        "processor_type": "aarch64",
        "python_version": "3.12.3",
        "library_versions": json.dumps({"lightgbm": "4.7.0", "torch": None}),
    }
    entry = {**registry[0], "version": 1}
    checks = [
        lib["check_row"](
            entry,
            env,
            "rid-2",
            NOW,
            reload_status="ok",
            rescore_status="matched",
            rescored_value=1.5,
            gap_relative=0.0,
            tolerance=1e-6,
            status="passed",
            detail="/Users/someone@example.com/notebook failed",
        )
    ]
    results = [
        {
            "component": "models/register/gate/01_registry_guards",
            "metric_name": "every_entry_has_a_card",
            "status": "PASS",
            "error_detail": "",
            "recorded_at": NOW,
        }
    ]
    eco = lib["TASK_BY_ID"][tid]["ecosystem"]
    return eco, lib["render_registry_findings"](eco, registry, checks, results, "stamp")


def test_the_findings_carry_the_registry_the_checks_and_the_environment(lib):
    _eco, text = _findings(lib)
    for part in (
        "## registry",
        "## reproducibility checks",
        "## environment of the checks",
        "## check results",
        "aarch64",
        "lightgbm",
        "not installed",
        "every_entry_has_a_card",
    ):
        assert part in text


def test_the_findings_hold_no_private_path_or_address(lib):
    _eco, text = _findings(lib)
    assert "someone@example.com" not in text and "/Users/someone" not in text
