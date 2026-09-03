"""CDC pipeline -- operational change driver.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: apply a deterministic stream of genuine INSERT / UPDATE / DELETE
activity to ecrmap.operational so Debezium has real changes to capture -- new
sign-ups and orders, order-status progression, meter faults and replacements,
and a few hard deletes. Every order or order_items change recomputes the
order header in the same transaction, so the items_subtotal / total
invariants always hold after a commit.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from src.generators import config as gen
from src.generators import reference_data as ref
from src.ingestion.cdc import config as cdc
from src.ingestion.cdc import connect

load_dotenv()

SCHEMA = "operational"

_DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecrmap"),
    "user": os.getenv("POSTGRES_USER", ""),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}


@dataclass
class Catalog:
    """A light snapshot of the ids the planner draws from."""

    customer_ids: list[str] = field(default_factory=list)
    active_contract_ids: list[str] = field(default_factory=list)
    active_meter_ids: list[str] = field(default_factory=list)
    tariff_ids: list[str] = field(default_factory=list)
    product_prices: dict[str, float] = field(default_factory=dict)
    open_order_ids: list[str] = field(default_factory=list)
    max_customer_seq: int = 0
    max_contract_seq: int = 0
    max_meter_seq: int = 0
    max_order_seq: int = 0


@dataclass
class Operation:
    kind: str
    table: str
    detail: dict


def _now() -> datetime:
    return datetime.now(tz=UTC)


def _ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S+00")


# --------------------------------------------------------------------------
# Planning -- pure, driven by the catalog snapshot and a fixed seed
# --------------------------------------------------------------------------


def build_plan(catalog: Catalog, *, cycles: int, seed: int) -> list[Operation]:
    rng = np.random.default_rng(seed)
    ops: list[Operation] = []
    cust_seq = catalog.max_customer_seq
    contract_seq = catalog.max_contract_seq
    meter_seq = catalog.max_meter_seq
    order_seq = catalog.max_order_seq
    skus = list(catalog.product_prices)

    for _ in range(cycles):
        # New sign-up: customer + contract + meter.
        if catalog.tariff_ids and rng.random() < 0.7:
            cust_seq += 1
            contract_seq += 1
            meter_seq += 1
            ops.append(
                Operation(
                    "insert",
                    "customers",
                    {
                        "seq": cust_seq,
                        "contract_seq": contract_seq,
                        "meter_seq": meter_seq,
                        "tariff_id": catalog.tariff_ids[
                            int(rng.integers(0, len(catalog.tariff_ids)))
                        ],
                        "rand": float(rng.random()),
                    },
                )
            )

        # New order for an existing customer (most frequent insert).
        if catalog.customer_ids and skus and rng.random() < 0.9:
            order_seq += 1
            n_lines = 1 + int(rng.integers(0, 3))
            chosen = sorted(
                {skus[int(rng.integers(0, len(skus)))] for _ in range(n_lines)}
            )
            lines = []
            for line_no, sku in enumerate(chosen):
                lines.append(
                    {
                        "line_no": line_no,
                        "sku": sku,
                        "unit_price": catalog.product_prices[sku],
                        "quantity": 1 + int(rng.integers(0, 3)),
                    }
                )
            ops.append(
                Operation(
                    "insert",
                    "orders",
                    {
                        "seq": order_seq,
                        "customer_id": catalog.customer_ids[
                            int(rng.integers(0, len(catalog.customer_ids)))
                        ],
                        "lines": lines,
                    },
                )
            )

        # Order-status progression / cancellation.
        if catalog.open_order_ids and rng.random() < 0.8:
            oid = catalog.open_order_ids[
                int(rng.integers(0, len(catalog.open_order_ids)))
            ]
            roll = rng.random()
            target = (
                "cancelled"
                if roll < 0.1
                else (
                    "paid" if roll < 0.55 else "shipped" if roll < 0.85 else "delivered"
                )
            )
            ops.append(
                Operation("update", "orders", {"order_id": oid, "order_status": target})
            )

        # Remove a line from an unshipped order, then recompute the header.
        if catalog.open_order_ids and rng.random() < 0.2:
            oid = catalog.open_order_ids[
                int(rng.integers(0, len(catalog.open_order_ids)))
            ]
            ops.append(
                Operation("delete", "order_items", {"order_id": oid, "which": "last"})
            )

        # Contract lifecycle: activate a pending one or end an active one.
        if catalog.active_contract_ids and rng.random() < 0.3:
            cid = catalog.active_contract_ids[
                int(rng.integers(0, len(catalog.active_contract_ids)))
            ]
            if rng.random() < 0.5 and catalog.tariff_ids:
                ops.append(
                    Operation(
                        "update",
                        "customer_contracts",
                        {
                            "contract_id": cid,
                            "tariff_id": catalog.tariff_ids[
                                int(rng.integers(0, len(catalog.tariff_ids)))
                            ],
                        },
                    )
                )
            else:
                ops.append(
                    Operation(
                        "update",
                        "customer_contracts",
                        {"contract_id": cid, "status": "ended"},
                    )
                )

        # Meter fault -> replacement meter.
        if catalog.active_meter_ids and rng.random() < 0.2:
            mid = catalog.active_meter_ids[
                int(rng.integers(0, len(catalog.active_meter_ids)))
            ]
            meter_seq += 1
            ops.append(
                Operation(
                    "update",
                    "meters",
                    {"meter_id": mid, "status": "faulty", "replacement_seq": meter_seq},
                )
            )

        # Occasional catalogue price change.
        if skus and rng.random() < 0.15:
            sku = skus[int(rng.integers(0, len(skus)))]
            ops.append(
                Operation(
                    "update",
                    "products",
                    {"sku": sku, "factor": float(rng.uniform(0.97, 1.06))},
                )
            )

    return ops


# --------------------------------------------------------------------------
# Execution -- each operation in its own transaction
# --------------------------------------------------------------------------

_RECOMPUTE_HEADER = f"""
UPDATE {SCHEMA}.orders o SET
    items_subtotal_eur = COALESCE(s.subtotal, 0),
    total_eur = COALESCE(s.subtotal, 0) + o.shipping_fee_eur,
    updated_at = now()
FROM (
    SELECT %s::uuid AS order_id,
           (SELECT SUM(line_total_eur) FROM {SCHEMA}.order_items WHERE order_id = %s::uuid) AS subtotal
) s
WHERE o.order_id = s.order_id
"""


def load_catalog(conn) -> Catalog:
    cat = Catalog()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT customer_id FROM {SCHEMA}.customers WHERE status = 'active'"
        )
        cat.customer_ids = [r["customer_id"] for r in cur.fetchall()]
        cur.execute(
            f"SELECT contract_id FROM {SCHEMA}.customer_contracts "
            "WHERE status IN ('active', 'pending')"
        )
        cat.active_contract_ids = [r["contract_id"] for r in cur.fetchall()]
        cur.execute(f"SELECT meter_id FROM {SCHEMA}.meters WHERE status = 'active'")
        cat.active_meter_ids = [r["meter_id"] for r in cur.fetchall()]
        cur.execute(f"SELECT tariff_id FROM {SCHEMA}.tariffs WHERE active")
        cat.tariff_ids = [r["tariff_id"] for r in cur.fetchall()]
        cur.execute(f"SELECT sku, unit_price_eur FROM {SCHEMA}.products WHERE active")
        cat.product_prices = {
            r["sku"]: float(r["unit_price_eur"]) for r in cur.fetchall()
        }
        cur.execute(
            f"SELECT order_id FROM {SCHEMA}.orders "
            "WHERE order_status IN ('placed', 'paid')"
        )
        cat.open_order_ids = [r["order_id"] for r in cur.fetchall()]
        for table, col, attr in (
            ("customers", "customer_number", "max_customer_seq"),
            ("customer_contracts", "contract_number", "max_contract_seq"),
            ("orders", "order_number", "max_order_seq"),
        ):
            cur.execute(
                f"SELECT COALESCE(MAX(NULLIF(regexp_replace({col}, '\\D', '', 'g'), '')::bigint), 0) AS m "
                f"FROM {SCHEMA}.{table}"
            )
            setattr(cat, attr, int(cur.fetchone()["m"]))
        cur.execute(
            f"SELECT COALESCE(MAX(NULLIF(regexp_replace(meter_serial, '\\D', '', 'g'), '')::bigint), 0) AS m "
            f"FROM {SCHEMA}.meters"
        )
        cat.max_meter_seq = int(cur.fetchone()["m"])
    return cat


class ChangeExecutor:
    def __init__(self, conn):
        self._conn = conn
        self.applied: dict[str, int] = {}

    def _bump(self, kind: str) -> None:
        self.applied[kind] = self.applied.get(kind, 0) + 1

    def run(self, plan: list[Operation]) -> dict[str, int]:
        for op in plan:
            handler = getattr(self, f"_{op.kind}_{op.table}", None)
            if handler is None:
                continue
            try:
                handler(op.detail)
                self._conn.commit()
                self._bump(f"{op.kind}:{op.table}")
            except psycopg2.Error:
                self._conn.rollback()
                raise
        return self.applied

    # inserts -----------------------------------------------------------

    def _insert_customers(self, d: dict) -> None:
        now = _now()
        seq = d["seq"]
        cid = gen.entity_uuid("customers", f"cdc:{seq}")
        contract_id = gen.entity_uuid("customer_contracts", f"cdc:{d['contract_seq']}")
        meter_id = gen.entity_uuid("meters", f"cdc:{d['meter_seq']}")
        idx = int(d["rand"] * len(ref.CITIES))
        city, plz = ref.CITIES[idx % len(ref.CITIES)]
        first = ref.FIRST_NAMES[seq % len(ref.FIRST_NAMES)]
        last = ref.LAST_NAMES[seq % len(ref.LAST_NAMES)]
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {SCHEMA}.customers (customer_id, customer_number, "
                "first_name, last_name, email, phone, street, house_number, "
                "postal_code, city, country_code, signed_up_at, status, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'DE', %s, 'active', %s, %s)",
                (
                    cid,
                    f"C{seq:07d}",
                    first,
                    last,
                    f"{first.lower()}.{last.lower()}.cdc{seq}@example-mail.de",
                    f"+49 170 {seq % 9000000 + 1000000}",
                    f"{ref.STREET_STEMS[seq % len(ref.STREET_STEMS)]}strasse",
                    str(seq % 180 + 1),
                    plz,
                    city,
                    _ts(now),
                    _ts(now),
                    _ts(now),
                ),
            )
            cur.execute(
                f"INSERT INTO {SCHEMA}.customer_contracts (contract_id, contract_number, "
                "customer_id, tariff_id, start_date, status, billing_day, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s)",
                (
                    contract_id,
                    f"K{d['contract_seq']:08d}",
                    cid,
                    d["tariff_id"],
                    now.date().isoformat(),
                    (seq % 28) + 1,
                    _ts(now),
                    _ts(now),
                ),
            )
            cur.execute(
                f"INSERT INTO {SCHEMA}.meters (meter_id, meter_serial, contract_id, "
                "meter_type, installed_on, status, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'electricity', %s, 'active', %s, %s)",
                (
                    meter_id,
                    f"1EL{d['meter_seq']:09d}",
                    contract_id,
                    now.date().isoformat(),
                    _ts(now),
                    _ts(now),
                ),
            )

    def _insert_orders(self, d: dict) -> None:
        now = _now()
        seq = d["seq"]
        order_id = gen.entity_uuid("orders", f"cdc:{seq}")
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {SCHEMA}.orders (order_id, order_number, customer_id, "
                "order_status, ordered_at, currency, items_subtotal_eur, shipping_fee_eur, "
                "total_eur, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'placed', %s, 'EUR', 0, 0, 0, %s, %s)",
                (
                    order_id,
                    f"O{seq:08d}",
                    d["customer_id"],
                    _ts(now),
                    _ts(now),
                    _ts(now),
                ),
            )
            for line in d["lines"]:
                cur.execute(
                    f"SELECT product_id FROM {SCHEMA}.products WHERE sku = %s",
                    (line["sku"],),
                )
                product_id = cur.fetchone()[0]
                line_total = round(line["unit_price"] * line["quantity"], 2)
                cur.execute(
                    f"INSERT INTO {SCHEMA}.order_items (order_item_id, order_id, "
                    "product_id, quantity, unit_price_eur, line_total_eur, created_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        gen.entity_uuid(
                            "order_items", f"cdc:{d['seq']}:{line['line_no']}"
                        ),
                        order_id,
                        product_id,
                        line["quantity"],
                        round(line["unit_price"], 2),
                        line_total,
                        _ts(now),
                    ),
                )
            # Set subtotal, shipping and total together so the
            # total = subtotal + shipping invariant holds at every step.
            finalise_sql = (
                f"UPDATE {SCHEMA}.orders o SET "
                "items_subtotal_eur = s.subtotal, "
                "shipping_fee_eur = CASE WHEN s.subtotal >= 50 THEN 0 ELSE 4.95 END, "
                "total_eur = s.subtotal "
                "+ CASE WHEN s.subtotal >= 50 THEN 0 ELSE 4.95 END, "
                "updated_at = now() "
                "FROM (SELECT COALESCE((SELECT SUM(line_total_eur) "
                f"FROM {SCHEMA}.order_items WHERE order_id = %s::uuid), 0) AS subtotal) s "
                "WHERE o.order_id = %s::uuid"
            )
            cur.execute(finalise_sql, (order_id, order_id))

    # updates ----------------------------------------------------------

    def _update_orders(self, d: dict) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SCHEMA}.orders SET order_status = %s, updated_at = now() "
                "WHERE order_id = %s",
                (d["order_status"], d["order_id"]),
            )

    def _update_customer_contracts(self, d: dict) -> None:
        with self._conn.cursor() as cur:
            if "tariff_id" in d:
                cur.execute(
                    f"UPDATE {SCHEMA}.customer_contracts SET tariff_id = %s, "
                    "status = 'active', updated_at = now() WHERE contract_id = %s",
                    (d["tariff_id"], d["contract_id"]),
                )
            else:
                cur.execute(
                    f"UPDATE {SCHEMA}.customer_contracts SET status = %s, "
                    "end_date = CURRENT_DATE, updated_at = now() WHERE contract_id = %s",
                    (d["status"], d["contract_id"]),
                )

    def _update_meters(self, d: dict) -> None:
        now = _now()
        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SCHEMA}.meters SET status = 'removed', removed_on = CURRENT_DATE, "
                "updated_at = now() WHERE meter_id = %s RETURNING contract_id, meter_type",
                (d["meter_id"],),
            )
            row = cur.fetchone()
            if row is None:
                return
            contract_id, meter_type = row
            prefix = "1EL" if meter_type == "electricity" else "7GA"
            cur.execute(
                f"INSERT INTO {SCHEMA}.meters (meter_id, meter_serial, contract_id, "
                "meter_type, installed_on, status, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, CURRENT_DATE, 'active', %s, %s)",
                (
                    gen.entity_uuid("meters", f"cdc:{d['replacement_seq']}"),
                    f"{prefix}{d['replacement_seq']:09d}",
                    contract_id,
                    meter_type,
                    _ts(now),
                    _ts(now),
                ),
            )

    def _update_products(self, d: dict) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SCHEMA}.products SET unit_price_eur = "
                "ROUND(unit_price_eur * %s, 2), updated_at = now() WHERE sku = %s",
                (d["factor"], d["sku"]),
            )

    # deletes --------------------------------------------------------

    def _delete_order_items(self, d: dict) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT order_item_id FROM {SCHEMA}.order_items WHERE order_id = %s "
                "ORDER BY created_at DESC, order_item_id DESC",
                (d["order_id"],),
            )
            rows = cur.fetchall()
            if len(rows) <= 1:
                return
            cur.execute(
                f"DELETE FROM {SCHEMA}.order_items WHERE order_item_id = %s",
                (rows[0][0],),
            )
            cur.execute(_RECOMPUTE_HEADER, (d["order_id"], d["order_id"]))


def export_snapshot(conn, path) -> dict:
    """Row counts per operational table -- the reconciliation baseline."""
    counts = {}
    with conn.cursor() as cur:
        for table in cdc.TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
            counts[table] = int(cur.fetchone()[0])
    payload = {
        "captured_at": _now().isoformat(),
        "schema": SCHEMA,
        "row_counts": counts,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Operational CDC change driver")
    parser.add_argument("--cycles", type=int, default=40)
    parser.add_argument("--seed", type=int, default=gen.SEED)
    parser.add_argument(
        "--snapshot", action="store_true", help="only export the baseline"
    )
    parser.add_argument(
        "--skip-connector-check",
        action="store_true",
        help="do not verify the Debezium connector before mutating the source",
    )
    args = parser.parse_args()

    if not args.snapshot and not args.skip_connector_check and not connect.is_healthy():
        print(
            "FAIL  Debezium connector is not RUNNING -- changes would not be "
            "captured. Start it (make cdc-connect-start) or pass "
            "--skip-connector-check for an intentionally uncaptured run."
        )
        return 2

    conn = psycopg2.connect(connect_timeout=10, **_DB)
    conn.autocommit = False
    try:
        cdc.ensure_dirs()
        if args.snapshot:
            payload = export_snapshot(
                conn, cdc.SNAPSHOT_DIR / "pre_change_row_counts.json"
            )
            print(f"snapshot: {payload['row_counts']}")
            return 0

        catalog = load_catalog(conn)
        plan = build_plan(catalog, cycles=args.cycles, seed=args.seed)
        applied = ChangeExecutor(conn).run(plan)
        export_snapshot(conn, cdc.SNAPSHOT_DIR / "post_change_row_counts.json")
    finally:
        conn.close()

    print("=" * 70)
    print("CHANGE DRIVER SUMMARY")
    print("=" * 70)
    for kind, n in sorted(applied.items()):
        print(f"  {kind:<28} {n}")
    print(f"  {'total operations':<28} {sum(applied.values())}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
