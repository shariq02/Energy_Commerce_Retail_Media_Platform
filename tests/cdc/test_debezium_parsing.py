"""CDC -- Debezium envelope parsing.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import pytest

from src.ingestion.cdc.debezium import MalformedEvent, parse_value

pytestmark = [pytest.mark.cdc, pytest.mark.unit]


def _envelope(op, before, after, lsn, ts_ms=1_724_000_000_000):
    return {
        "schema": {"type": "struct"},
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "lsn": lsn,
                "ts_ms": ts_ms,
                "table": "orders",
                "schema": "operational",
            },
            "op": op,
            "ts_ms": ts_ms + 5,
        },
    }


def test_parse_insert():
    ev = parse_value(
        _envelope("c", None, {"order_id": "o-1", "total_eur": "10.00"}, 100),
        table="orders",
        pk_column="order_id",
    )
    assert ev.op == "c"
    assert ev.row_key == "o-1"
    assert ev.lsn == 100
    assert ev.is_delete is False
    assert ev.state_row == {"order_id": "o-1", "total_eur": "10.00"}


def test_parse_update_keeps_both_images():
    ev = parse_value(
        _envelope(
            "u", {"order_id": "o-1"}, {"order_id": "o-1", "total_eur": "12.00"}, 200
        ),
        table="orders",
        pk_column="order_id",
    )
    assert ev.op == "u"
    assert ev.lsn == 200
    assert ev.before == {"order_id": "o-1"}


def test_parse_delete_uses_before_for_key():
    ev = parse_value(
        _envelope("d", {"order_id": "o-9"}, None, 300),
        table="orders",
        pk_column="order_id",
    )
    assert ev.is_delete is True
    assert ev.row_key == "o-9"
    assert ev.state_row is None


def test_snapshot_read_without_lsn():
    payload = {
        "before": None,
        "after": {"order_id": "o-2"},
        "source": {"ts_ms": 1, "table": "orders"},
        "op": "r",
        "ts_ms": 1,
    }
    ev = parse_value(
        {"payload": payload, "schema": {}}, table="orders", pk_column="order_id"
    )
    assert ev.op == "r"
    assert ev.lsn == 0


def test_unwrapped_payload_also_parses():
    payload = _envelope("c", None, {"tariff_id": "t-1"}, 5)["payload"]
    ev = parse_value(payload, table="tariffs", pk_column="tariff_id")
    assert ev.row_key == "t-1"


def test_malformed_event_rejected():
    with pytest.raises(MalformedEvent):
        parse_value({"not": "debezium"}, table="orders", pk_column="order_id")
    with pytest.raises(MalformedEvent):
        parse_value(
            _envelope("x", None, {"order_id": "o"}, 1),
            table="orders",
            pk_column="order_id",
        )
