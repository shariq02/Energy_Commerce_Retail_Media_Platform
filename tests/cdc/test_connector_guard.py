"""CDC -- the change driver cannot run without healthy Debezium capture.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import pytest

from src.ingestion.cdc import change_driver, connect
from src.ingestion.cdc import run_cdc_cycle as cycle

pytestmark = [pytest.mark.cdc, pytest.mark.unit]

_RUNNING = {
    "connector": {"state": "RUNNING"},
    "tasks": [{"id": 0, "state": "RUNNING"}],
}
_TASK_FAILED = {
    "connector": {"state": "RUNNING"},
    "tasks": [{"id": 0, "state": "FAILED"}],
}
_NO_TASKS = {"connector": {"state": "RUNNING"}, "tasks": []}


# --- connect.is_healthy --------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (_RUNNING, True),
        (_TASK_FAILED, False),
        (_NO_TASKS, False),
        ({"connector": {"state": "FAILED"}, "tasks": [{"state": "RUNNING"}]}, False),
        (None, False),
    ],
)
def test_is_healthy(monkeypatch, payload, expected):
    monkeypatch.setattr(connect, "status", lambda: payload)
    assert connect.is_healthy() is expected


# --- run_cdc_cycle gate ------------------------------------------------


def test_cycle_refuses_changes_when_connector_unhealthy(monkeypatch, capsys):
    monkeypatch.setattr(cycle.connect, "is_healthy", lambda: False)
    monkeypatch.setattr(cycle.connect, "status", lambda: _TASK_FAILED)
    monkeypatch.setattr(cycle.config, "ensure_dirs", lambda: None)

    ran: list[tuple] = []
    monkeypatch.setattr(cycle, "_run", lambda module, *a: ran.append((module, a)) or 0)
    monkeypatch.setattr(
        "sys.argv", ["run_cdc_cycle", "--cycles", "1", "--skip", "consume", "sync"]
    )

    rc = cycle.main()

    assert rc == 2
    # the baseline snapshot may run; the change (--cycles) step must not
    assert not any(any("--cycles" in x for x in a) for _, a in ran)
    assert "refusing to run the change driver" in capsys.readouterr().out


def test_cycle_proceeds_when_connector_healthy(monkeypatch):
    monkeypatch.setattr(cycle.connect, "is_healthy", lambda: True)
    monkeypatch.setattr(cycle.config, "ensure_dirs", lambda: None)
    ran: list[tuple] = []
    monkeypatch.setattr(cycle, "_run", lambda module, *a: ran.append((module, a)) or 0)
    monkeypatch.setattr(
        "sys.argv", ["run_cdc_cycle", "--cycles", "1", "--skip", "consume", "sync"]
    )

    rc = cycle.main()

    assert rc == 0
    assert any(any("--cycles" in x for x in a) for _, a in ran)


# --- change_driver stands alone --------------------------------------


def test_change_driver_main_fails_closed_without_connector(monkeypatch):
    monkeypatch.setattr(change_driver.connect, "is_healthy", lambda: False)
    monkeypatch.setattr("sys.argv", ["change_driver", "--cycles", "1"])

    def _boom(*a, **k):
        raise AssertionError("must not connect to PostgreSQL when capture is down")

    monkeypatch.setattr(change_driver.psycopg2, "connect", _boom)
    assert change_driver.main() == 2
