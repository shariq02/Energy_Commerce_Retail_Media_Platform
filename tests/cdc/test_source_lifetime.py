"""CDC -- resume state is scoped to a source database lifetime.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

A rebuilt/restored source restarts its log lower; stale resume state from the
previous lifetime must not make those events look already-applied.
"""

from __future__ import annotations

import json

import pytest

from src.ingestion.cdc import consumer as mod
from src.ingestion.cdc.consumer import CdcConsumer
from src.ingestion.cdc.state import ConsumerState

pytestmark = [pytest.mark.cdc, pytest.mark.unit]

ORDERS = "ecrmap.operational.orders"
_TS = 1_724_000_000_000


class FakeMsg:
    def __init__(self, topic, partition, offset, value):
        self._t, self._p, self._o, self._v = topic, partition, offset, value

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def key(self):
        return None

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def subscribe(self, topics, on_assign=None):
        if on_assign is not None:
            on_assign(self, [])

    def assign(self, partitions):
        pass

    def commit(self, *, offsets=None, asynchronous=True):
        self.commits.append(offsets)

    def close(self):
        pass

    def poll(self, timeout=None):
        return None


def _order_event(offset, *, op, key, lsn):
    payload = {
        "before": None if op == "c" else {"order_id": key},
        "after": None if op == "d" else {"order_id": key, "v": str(lsn)},
        "source": {"lsn": lsn, "ts_ms": _TS, "table": "orders"},
        "op": op,
        "ts_ms": _TS,
    }
    raw = json.dumps({"payload": payload, "schema": {}}).encode("utf-8")
    return FakeMsg(ORDERS, 0, offset, raw)


@pytest.fixture
def paths(tmp_path, monkeypatch):
    monkeypatch.setattr(mod.config, "LANDING_DIR", tmp_path / "landing")
    monkeypatch.setattr(mod.config, "STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(mod.config, "STATE_DB", tmp_path / "state" / "s.db")
    monkeypatch.setattr(mod.config, "SNAPSHOT_DIR", tmp_path / "snap")
    monkeypatch.setattr(
        mod.config, "CONNECT_OFFSETS", tmp_path / "c" / "offsets" / "connect.offsets"
    )
    monkeypatch.setattr(mod, "Consumer", lambda _cfg: FakeConsumer())
    return tmp_path


def _make(epoch, **kw):
    return CdcConsumer(idle_timeout=0.0, epoch_resolver=lambda: epoch, **kw)


def _drive(consumer, monkeypatch, *batches):
    queue = list(batches)
    monkeypatch.setattr(
        consumer, "_poll_batch", lambda *a, **k: queue.pop(0) if queue else []
    )
    return consumer.run()


# --- ConsumerState lineage primitives -------------------------------------


def test_state_binds_and_reports_source_epoch(tmp_path):
    state = ConsumerState(tmp_path / "s.db")
    assert state.source_epoch() is None
    state.bind_source_epoch("pg-system-1")
    assert state.source_epoch() == "pg-system-1"
    state.close()


def test_reset_for_new_source_clears_positions_and_rebinds(tmp_path):
    state = ConsumerState(tmp_path / "s.db")
    with state.transaction():
        state.record_applied("orders", "o-1", 999, "u", "2026-08-30T00:00:00+00:00")
        state.bump_watermark("orders", 10, applied=True, stale=False, late=False)
    state.bind_source_epoch("pg-system-1")

    state.reset_for_new_source("pg-system-2")

    assert state.source_epoch() == "pg-system-2"
    assert state.last_lsn("orders", "o-1") is None
    assert state.watermark_ms("orders") == 0
    state.close()


# --- consumer reconciles its state against the live source lifetime -------


def test_first_run_binds_the_current_epoch(paths):
    consumer = _make("pg-system-A")
    assert consumer._state.source_epoch() == "pg-system-A"
    assert consumer._rewind_on_assign is False


def test_same_epoch_keeps_resume_state(paths, monkeypatch):
    c1 = _make("pg-system-A")
    with c1._state.transaction():
        c1._state.record_applied("orders", "o-1", 500, "u", "2026-08-30T00:00:00+00:00")

    c2 = _make("pg-system-A")
    assert c2._state.last_lsn("orders", "o-1") == 500
    assert c2._rewind_on_assign is False


def test_changed_epoch_wipes_state_and_forces_rewind(paths):
    c1 = _make("pg-system-A")
    with c1._state.transaction():
        c1._state.record_applied(
            "orders", "o-1", 9_999_999_999, "u", "2026-08-30T00:00:00+00:00"
        )

    c2 = _make("pg-system-B")
    assert c2._state.source_epoch() == "pg-system-B"
    assert c2._state.last_lsn("orders", "o-1") is None
    assert c2._rewind_on_assign is True


def test_low_lsn_events_after_source_rebuild_are_not_stale(paths, monkeypatch):
    # Old lifetime recorded a very high LSN for this key.
    c1 = _make("pg-system-A")
    with c1._state.transaction():
        c1._state.record_applied(
            "orders", "o-1", 8_000_000_000_000, "u", "2026-08-30T00:00:00+00:00"
        )

    # New lifetime: the same key re-appears via a fresh snapshot at a low LSN.
    c2 = _make("pg-system-B")
    manifest = _drive(c2, monkeypatch, [_order_event(0, op="r", key="o-1", lsn=42)])
    assert manifest["events_applied"] == 1
    assert manifest["events_stale"] == 0


# --- idempotent replay / restart within one lifetime ---------------------


def test_replaying_the_same_batch_applies_nothing_the_second_time(paths, monkeypatch):
    batch = [
        _order_event(0, op="r", key="o-1", lsn=100),
        _order_event(1, op="u", key="o-1", lsn=200),
    ]
    first = _drive(_make("pg-system-A"), monkeypatch, list(batch))
    assert first["events_applied"] == 2

    second_consumer = _make("pg-system-A")
    second = _drive(second_consumer, monkeypatch, list(batch))
    assert second["events_applied"] == 0
    assert second["events_stale"] == 2
    # offsets are still committed so the group does not re-read forever
    assert second_consumer._consumer.commits and second_consumer._consumer.commits[0]
