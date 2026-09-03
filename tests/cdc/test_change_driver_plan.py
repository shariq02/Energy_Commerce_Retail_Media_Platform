"""CDC -- the change driver's planning is deterministic and invariant-aware.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from src.generators import config as gen
from src.ingestion.cdc.change_driver import _RECOMPUTE_HEADER, Catalog, build_plan

pytestmark = [pytest.mark.cdc, pytest.mark.unit]


def _full_catalog() -> Catalog:
    return Catalog(
        customer_ids=[f"c-{i}" for i in range(20)],
        active_contract_ids=[f"k-{i}" for i in range(20)],
        active_meter_ids=[f"m-{i}" for i in range(20)],
        tariff_ids=[f"t-{i}" for i in range(4)],
        product_prices={f"SKU-{i}": 10.0 + i for i in range(8)},
        open_order_ids=[f"o-{i}" for i in range(15)],
        max_customer_seq=2000,
        max_contract_seq=2300,
        max_meter_seq=2500,
        max_order_seq=2000,
    )


def test_plan_is_deterministic_for_a_seed():
    cat = _full_catalog()
    a = build_plan(cat, cycles=25, seed=42)
    b = build_plan(cat, cycles=25, seed=42)
    assert [(o.kind, o.table, o.detail) for o in a] == [
        (o.kind, o.table, o.detail) for o in b
    ]
    assert build_plan(cat, cycles=25, seed=7) != a


def test_plan_exercises_every_op_kind_and_table():
    plan = build_plan(_full_catalog(), cycles=60, seed=1)
    kinds = {(o.kind, o.table) for o in plan}
    assert ("insert", "customers") in kinds
    assert ("insert", "orders") in kinds
    assert ("update", "orders") in kinds
    assert ("update", "meters") in kinds
    assert ("delete", "order_items") in kinds


def test_order_item_delete_targets_last_line_only():
    plan = build_plan(_full_catalog(), cycles=60, seed=1)
    deletes = [o for o in plan if (o.kind, o.table) == ("delete", "order_items")]
    assert deletes
    assert all(o.detail["which"] == "last" for o in deletes)


def test_header_recompute_sql_maintains_the_invariant():
    sql = " ".join(_RECOMPUTE_HEADER.split())
    assert "items_subtotal_eur = COALESCE(s.subtotal, 0)" in sql
    assert "total_eur = COALESCE(s.subtotal, 0) + o.shipping_fee_eur" in sql
    assert "SUM(line_total_eur)" in sql


def test_empty_catalog_yields_no_operations():
    assert build_plan(Catalog(), cycles=10, seed=1) == []


def _order_item_ids(plan) -> set[str]:
    """The order_item_ids _insert_orders would generate for an order plan."""
    ids = set()
    for op in plan:
        if (op.kind, op.table) != ("insert", "orders"):
            continue
        for line in op.detail["lines"]:
            ids.add(
                gen.entity_uuid(
                    "order_items", f"cdc:{op.detail['seq']}:{line['line_no']}"
                )
            )
    return ids


def test_order_lines_are_indexed_within_the_order():
    plan = build_plan(_full_catalog(), cycles=40, seed=3)
    for op in plan:
        if (op.kind, op.table) != ("insert", "orders"):
            continue
        line_nos = [line["line_no"] for line in op.detail["lines"]]
        assert line_nos == list(range(len(line_nos)))


def test_order_item_ids_do_not_collide_across_consecutive_cycles():
    cat = _full_catalog()
    cycle1 = build_plan(cat, cycles=30, seed=11)

    # A committed cycle advances max_order_seq past every order it inserted.
    last_order_seq = max(
        (
            op.detail["seq"]
            for op in cycle1
            if (op.kind, op.table) == ("insert", "orders")
        ),
        default=cat.max_order_seq,
    )
    cycle2 = build_plan(replace(cat, max_order_seq=last_order_seq), cycles=30, seed=12)

    assert _order_item_ids(cycle1).isdisjoint(_order_item_ids(cycle2))
