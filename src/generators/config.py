"""Synthetic operational data generator -- configuration.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: fixed seed, deterministic UUID namespace, the history window, output
location, and per-table target volumes for the Phase 5 operational dataset.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = REPO_ROOT / "postgres" / "seed"

# Determinism.
SEED = 20260830
UUID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_DNS, "operational.ecrmap.energy-commerce-retail-media"
)

# Operational history window (~3 years ending "today").
HISTORY_START = date(2023, 9, 1)
HISTORY_END = date(2026, 8, 30)
NOW = datetime(2026, 8, 30, 12, 0, 0, tzinfo=UTC)

# Per-table target volumes (demo scale). Child-table counts are ranges driven by
# the relationship rules in build.py; the numbers below are the direct targets.
VOLUMES = {
    "customers": 2000,
    "ordering_customer_fraction": 0.40,
}

# Load order (parents first) -- also the truncate order reversed.
TABLE_LOAD_ORDER = [
    "tariffs",
    "products",
    "customers",
    "customer_contracts",
    "meters",
    "orders",
    "order_items",
]

# Column order per table for the CSV files (must match the DDL for COPY).
TABLE_COLUMNS = {
    "tariffs": [
        "tariff_id",
        "tariff_code",
        "name",
        "energy_type",
        "unit_price_eur_per_kwh",
        "standing_charge_eur_per_month",
        "contract_term_months",
        "active",
        "valid_from",
        "valid_to",
        "created_at",
        "updated_at",
    ],
    "products": [
        "product_id",
        "sku",
        "name",
        "category",
        "unit_price_eur",
        "active",
        "created_at",
        "updated_at",
    ],
    "customers": [
        "customer_id",
        "customer_number",
        "first_name",
        "last_name",
        "email",
        "phone",
        "street",
        "house_number",
        "postal_code",
        "city",
        "country_code",
        "date_of_birth",
        "signed_up_at",
        "status",
        "created_at",
        "updated_at",
    ],
    "customer_contracts": [
        "contract_id",
        "contract_number",
        "customer_id",
        "tariff_id",
        "start_date",
        "end_date",
        "status",
        "billing_day",
        "created_at",
        "updated_at",
    ],
    "meters": [
        "meter_id",
        "meter_serial",
        "contract_id",
        "meter_type",
        "melo_id",
        "installed_on",
        "removed_on",
        "status",
        "created_at",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "order_number",
        "customer_id",
        "order_status",
        "ordered_at",
        "currency",
        "items_subtotal_eur",
        "shipping_fee_eur",
        "total_eur",
        "created_at",
        "updated_at",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_eur",
        "line_total_eur",
        "created_at",
    ],
}


def entity_uuid(table: str, natural_key: str) -> str:
    """Deterministic UUIDv5 for a row, stable across runs."""
    return str(uuid.uuid5(UUID_NAMESPACE, f"{table}:{natural_key}"))
