"""Synthetic operational seed data -- determinism and integrity.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

No database required -- checks the in-process output of src.generators.build.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal

import pytest

from src.generators import config
from src.generators.build import build_all

pytestmark = [pytest.mark.data_quality, pytest.mark.generator]

_ENUMS = {
    ("tariffs", "energy_type"): {"electricity", "gas"},
    ("products", "category"): None,
    ("customers", "status"): {"active", "inactive", "churned"},
    ("customers", "country_code"): {"DE"},
    ("customer_contracts", "status"): {"active", "pending", "ended", "cancelled"},
    ("meters", "meter_type"): {"electricity", "gas"},
    ("meters", "status"): {"active", "removed", "faulty"},
    ("orders", "order_status"): {
        "placed",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
        "refunded",
    },
    ("orders", "currency"): {"EUR"},
}

_PK = {
    "tariffs": "tariff_id",
    "products": "product_id",
    "customers": "customer_id",
    "customer_contracts": "contract_id",
    "meters": "meter_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}

_DATE_ORDER = [
    ("tariffs", "valid_from", "valid_to"),
    ("customer_contracts", "start_date", "end_date"),
    ("meters", "installed_on", "removed_on"),
]


def _digest(tables: dict[str, list[dict]]) -> str:
    h = hashlib.sha256()
    for name in sorted(tables):
        h.update(name.encode())
        h.update(json.dumps(tables[name], sort_keys=True, default=str).encode())
    return h.hexdigest()


def test_deterministic():
    assert _digest(build_all()) == _digest(build_all())


def test_all_tables_present_and_non_empty(built_tables):
    assert set(built_tables) == set(config.TABLE_LOAD_ORDER)
    for name, rows in built_tables.items():
        assert rows, f"{name} is empty"
    assert len(built_tables["customers"]) == config.VOLUMES["customers"]
    assert len(built_tables["tariffs"]) == 8
    assert len(built_tables["products"]) == 24


def test_primary_keys_unique(built_tables):
    for table, pk in _PK.items():
        values = [r[pk] for r in built_tables[table]]
        assert len(values) == len(set(values)), f"{table}.{pk} not unique"


@pytest.mark.parametrize(
    ("child", "fk", "parent", "ppk"),
    [
        ("customer_contracts", "customer_id", "customers", "customer_id"),
        ("customer_contracts", "tariff_id", "tariffs", "tariff_id"),
        ("meters", "contract_id", "customer_contracts", "contract_id"),
        ("orders", "customer_id", "customers", "customer_id"),
        ("order_items", "order_id", "orders", "order_id"),
        ("order_items", "product_id", "products", "product_id"),
    ],
)
def test_foreign_keys_resolve(built_tables, child, fk, parent, ppk):
    parent_ids = {r[ppk] for r in built_tables[parent]}
    for row in built_tables[child]:
        assert row[fk] in parent_ids, f"{child}.{fk}={row[fk]} has no {parent}"


def test_order_header_equals_line_totals(built_tables):
    lines: dict[str, Decimal] = {}
    for it in built_tables["order_items"]:
        assert Decimal(it["line_total_eur"]) == Decimal(it["unit_price_eur"]) * int(
            it["quantity"]
        )
        lines[it["order_id"]] = lines.get(it["order_id"], Decimal(0)) + Decimal(
            it["line_total_eur"]
        )
    for o in built_tables["orders"]:
        assert lines[o["order_id"]] == Decimal(o["items_subtotal_eur"])
        assert Decimal(o["total_eur"]) == Decimal(o["items_subtotal_eur"]) + Decimal(
            o["shipping_fee_eur"]
        )


@pytest.mark.parametrize(("table", "lo", "hi"), _DATE_ORDER)
def test_date_ordering(built_tables, table, lo, hi):
    for row in built_tables[table]:
        if row[hi]:
            assert date.fromisoformat(row[hi]) >= date.fromisoformat(row[lo]), row


def test_enum_domains(built_tables):
    for (table, col), allowed in _ENUMS.items():
        if allowed is None:
            continue
        seen = {r[col] for r in built_tables[table]}
        assert seen <= allowed, f"{table}.{col} has {seen - allowed}"


def test_german_localisation(built_tables):
    for c in built_tables["customers"]:
        assert len(c["postal_code"]) == 5 and c["postal_code"].isdigit()
        assert c["country_code"] == "DE"
    assert all(o["currency"] == "EUR" for o in built_tables["orders"])


def test_referential_completeness(built_tables):
    with_contract = {r["customer_id"] for r in built_tables["customer_contracts"]}
    assert with_contract == {r["customer_id"] for r in built_tables["customers"]}
    with_meter = {r["contract_id"] for r in built_tables["meters"]}
    assert with_meter == {r["contract_id"] for r in built_tables["customer_contracts"]}


def test_timestamps_are_utc_and_ordered(built_tables):
    for table in ("customers", "orders"):
        for row in built_tables[table]:
            for col in ("created_at", "updated_at"):
                assert row[col].endswith("+00")
                datetime.strptime(row[col], "%Y-%m-%d %H:%M:%S+00")  # noqa: DTZ007
            assert row["updated_at"] >= row["created_at"]
