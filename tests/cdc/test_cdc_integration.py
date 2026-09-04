"""CDC -- live integration checks. Each skips when its dependency is absent.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from src.ingestion.cdc import config
from src.ingestion.cdc.change_driver import (
    Catalog,
    ChangeExecutor,
    build_plan,
    load_catalog,
)

pytestmark = [pytest.mark.cdc, pytest.mark.integration]


def _redpanda_up() -> bool:
    if shutil.which("rpk") is None:
        return False
    result = subprocess.run(
        ["rpk", "cluster", "info", "--brokers", config.BOOTSTRAP_SERVERS],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    return result.returncode == 0


@pytest.mark.streaming
def test_cdc_topics_can_be_created_and_listed():
    if not _redpanda_up():
        pytest.skip("Redpanda broker not reachable")
    from src.ingestion.cdc import topics

    assert topics.create() == 0
    live = topics.list_topics()
    for topic in config.all_topics():
        assert topic in live


def test_change_driver_preserves_order_invariants(pg_conn):
    if os.getenv("CDC_ALLOW_DB_MUTATION") != "1":
        pytest.skip(
            "set CDC_ALLOW_DB_MUTATION=1 to run the mutating change-driver test"
        )
    with pg_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('operational.orders')")
        if cur.fetchone()[0] is None:
            pytest.skip("operational schema not present")
        cur.execute("SELECT count(*) FROM operational.customers")
        if cur.fetchone()[0] == 0:
            pytest.skip("operational tables are empty -- load the seed first")

    catalog = load_catalog(pg_conn)
    plan = build_plan(catalog, cycles=15, seed=99)
    ChangeExecutor(pg_conn).run(plan)

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM operational.orders o JOIN ("
            "  SELECT order_id, SUM(line_total_eur) s FROM operational.order_items "
            "  GROUP BY 1) x USING (order_id) "
            "WHERE o.items_subtotal_eur <> x.s "
            "   OR o.total_eur <> o.items_subtotal_eur + o.shipping_fee_eur"
        )
        assert cur.fetchone()[0] == 0
    pg_conn.rollback()


def test_load_catalog_shape_is_usable():
    empty = Catalog()
    assert empty.customer_ids == []
    assert build_plan(empty, cycles=5, seed=1) == []
