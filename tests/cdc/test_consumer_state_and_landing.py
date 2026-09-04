"""CDC -- consumer dedup/ordering state and the landing writer.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import json

import pytest

from src.ingestion.cdc.debezium import parse_value
from src.ingestion.cdc.landing import META_COLUMNS, LandingWriter
from src.ingestion.cdc.state import ConsumerState

pytestmark = [pytest.mark.cdc, pytest.mark.unit]


def _event(op, lsn, key="o-1", ts=1_724_000_000_000):
    payload = {
        "before": {"order_id": key} if op != "c" else None,
        "after": None if op == "d" else {"order_id": key, "v": str(lsn)},
        "source": {"lsn": lsn, "ts_ms": ts, "table": "orders"},
        "op": op,
        "ts_ms": ts,
    }
    return parse_value(
        {"payload": payload, "schema": {}}, table="orders", pk_column="order_id"
    )


def test_state_detects_stale_lsn(tmp_path):
    state = ConsumerState(tmp_path / "s.db")
    with state.transaction():
        state.record_applied("orders", "o-1", 200, "u", "2026-08-30T00:00:00+00:00")
    assert state.last_lsn("orders", "o-1") == 200
    assert state.last_lsn("orders", "o-2") is None
    # A later, lower LSN is stale; a higher one is fresh.
    assert 150 <= state.last_lsn("orders", "o-1")
    state.close()


def test_watermark_tracks_max_and_counts(tmp_path):
    state = ConsumerState(tmp_path / "s.db")
    with state.transaction():
        state.bump_watermark("orders", 100, applied=True, stale=False, late=False)
        state.bump_watermark("orders", 90, applied=False, stale=True, late=True)
    assert state.watermark_ms("orders") == 100
    row = next(r for r in state.summary() if r["table_name"] == "orders")
    assert row["events_seen"] == 2
    assert row["events_stale"] == 1
    assert row["events_late"] == 1
    state.close()


def test_landing_writer_emits_all_metadata(tmp_path):
    writer = LandingWriter(tmp_path, "runX")
    writer.write(_event("c", 10))
    writer.write(_event("u", 20))
    counts = writer.close()
    assert counts["orders"] == 2

    path = tmp_path / "orders" / "orders__runX.jsonl"
    lines = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2
    for record in lines:
        for column in META_COLUMNS:
            assert column in record
        assert record["_table"] == "orders"
        assert "before" in record and "after" in record
    assert lines[0]["_op"] == "c"
    assert lines[1]["_lsn"] == 20
