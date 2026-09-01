"""CDC -- the consumer commits explicit Kafka offsets for every processed batch.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import json

import pytest

from src.ingestion.cdc import consumer as mod
from src.ingestion.cdc.consumer import CdcConsumer, _end_offsets

pytestmark = [pytest.mark.cdc, pytest.mark.unit]

ORDERS = "ecrmap.operational.orders"
CUSTOMERS = "ecrmap.operational.customers"
_TS = 1_724_000_000_000


class FakeMsg:
    def __init__(self, topic, partition, offset, value=b"", key=None):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._key = value, key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits: list = []
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics, on_assign=None):
        self.subscribed = list(topics)
        self.on_assign = on_assign

    def assign(self, partitions):
        self.assigned = list(partitions)

    def commit(self, *, offsets=None, asynchronous=True):
        self.commits.append(offsets)

    def close(self):
        self.closed = True

    def poll(self, timeout=None):
        return None


def _event_msg(topic, partition, offset, *, op="c", key="o-1", lsn=10):
    payload = {
        "before": None if op == "c" else {"order_id": key},
        "after": None if op == "d" else {"order_id": key, "v": str(lsn)},
        "source": {"lsn": lsn, "ts_ms": _TS, "table": topic.rsplit(".", 1)[-1]},
        "op": op,
        "ts_ms": _TS,
    }
    raw = json.dumps({"payload": payload, "schema": {}}).encode("utf-8")
    return FakeMsg(topic, partition, offset, raw, None)


@pytest.fixture
def cdc_consumer(tmp_path, monkeypatch):
    monkeypatch.setattr(mod.config, "LANDING_DIR", tmp_path / "landing")
    monkeypatch.setattr(mod.config, "STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(mod.config, "STATE_DB", tmp_path / "state" / "s.db")
    monkeypatch.setattr(mod.config, "SNAPSHOT_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(
        mod.config,
        "CONNECT_OFFSETS",
        tmp_path / "connect" / "offsets" / "connect.offsets",
    )
    monkeypatch.setattr(mod, "Consumer", lambda _cfg: FakeConsumer())
    return CdcConsumer(idle_timeout=0.0, epoch_resolver=lambda: "test-epoch")


def _drive(consumer, monkeypatch, batches):
    queue = list(batches)
    monkeypatch.setattr(
        consumer, "_poll_batch", lambda *a, **k: queue.pop(0) if queue else []
    )
    return consumer.run()


# --- 1. _end_offsets over multiple topic/partition pairs --------------------


def test_end_offsets_returns_max_plus_one_per_topic_partition():
    batch = [
        FakeMsg(ORDERS, 0, 5),
        FakeMsg(ORDERS, 0, 7),
        FakeMsg(ORDERS, 0, 6),
        FakeMsg(ORDERS, 1, 2),
        FakeMsg(CUSTOMERS, 0, 9),
    ]
    got = {(tp.topic, tp.partition): tp.offset for tp in _end_offsets(batch)}
    assert got == {
        (ORDERS, 0): 8,
        (ORDERS, 1): 3,
        (CUSTOMERS, 0): 10,
    }


# --- 2. non-empty batch commits explicit offsets; empty batch does not -----


def test_non_empty_batch_commits_explicit_offsets(cdc_consumer, monkeypatch):
    batch = [
        _event_msg(ORDERS, 0, 3, key="o-1", lsn=11),
        _event_msg(ORDERS, 1, 8, key="o-2", lsn=12),
    ]
    _drive(cdc_consumer, monkeypatch, [batch])

    fake = cdc_consumer._consumer
    assert len(fake.commits) == 1
    committed = {(tp.topic, tp.partition): tp.offset for tp in fake.commits[0]}
    assert committed == {(ORDERS, 0): 4, (ORDERS, 1): 9}


def test_empty_batch_does_not_commit(cdc_consumer, monkeypatch):
    _drive(cdc_consumer, monkeypatch, [])
    assert cdc_consumer._consumer.commits == []


# --- 3. an all-stale batch still commits its Kafka offsets ------------------


def test_all_stale_batch_still_commits_offsets(cdc_consumer, monkeypatch):
    with cdc_consumer._state.transaction():
        cdc_consumer._state.record_applied(
            "orders", "o-1", 999, "u", "2026-08-30T00:00:00+00:00"
        )
        cdc_consumer._state.record_applied(
            "orders", "o-2", 999, "u", "2026-08-30T00:00:00+00:00"
        )

    batch = [
        _event_msg(ORDERS, 0, 40, op="u", key="o-1", lsn=10),
        _event_msg(ORDERS, 0, 41, op="u", key="o-2", lsn=10),
    ]
    manifest = _drive(cdc_consumer, monkeypatch, [batch])

    assert manifest["events_applied"] == 0
    assert manifest["events_stale"] == 2
    fake = cdc_consumer._consumer
    assert len(fake.commits) == 1
    committed = {(tp.topic, tp.partition): tp.offset for tp in fake.commits[0]}
    assert committed == {(ORDERS, 0): 42}
