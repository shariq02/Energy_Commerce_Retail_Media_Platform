"""Synthetic operational data generator -- entity builders.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: build all 7 operational tables as lists of row dicts, deterministically
from a fixed seed, with referential integrity and the internal-consistency
invariants (orders header total = sum of its order_items line totals).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import numpy as np

from src.generators import config
from src.generators import reference_data as ref

_TWOPLACES = Decimal("0.01")
_FIVEPLACES = Decimal("0.00001")


def _money(value: float | Decimal) -> str:
    return str(Decimal(value).quantize(_TWOPLACES, rounding=ROUND_HALF_UP))


def _rate(value: float) -> str:
    return str(Decimal(value).quantize(_FIVEPLACES, rounding=ROUND_HALF_UP))


def _d(value: date | None) -> str:
    return "" if value is None else value.isoformat()


def _ts(value: datetime | None) -> str:
    return "" if value is None else value.strftime("%Y-%m-%d %H:%M:%S+00")


def _rand_date(rng: np.random.Generator, start: date, end: date) -> date:
    span = (end - start).days
    return start + timedelta(days=int(rng.integers(0, span + 1)))


def _rand_ts(rng: np.random.Generator, day: date) -> datetime:
    secs = int(rng.integers(6 * 3600, 22 * 3600))
    return datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(seconds=secs)


def _streams() -> dict[str, np.random.Generator]:
    names = [
        "customers",
        "contracts",
        "meters",
        "orders",
        "order_items",
    ]
    children = np.random.SeedSequence(config.SEED).spawn(len(names))
    return {n: np.random.default_rng(s) for n, s in zip(names, children, strict=True)}


def build_all() -> dict[str, list[dict]]:
    rng = _streams()
    tables: dict[str, list[dict]] = {}

    tariffs = _build_tariffs()
    products = _build_products()
    tables["tariffs"] = tariffs
    tables["products"] = products

    customers = _build_customers(rng["customers"])
    tables["customers"] = customers

    contracts = _build_contracts(rng["contracts"], customers, tariffs)
    tables["customer_contracts"] = contracts

    tables["meters"] = _build_meters(rng["meters"], contracts)

    orders, order_items = _build_orders(
        rng["orders"], rng["order_items"], customers, products
    )
    tables["orders"] = orders
    tables["order_items"] = order_items

    return tables


# --------------------------------------------------------------------------


def _build_tariffs() -> list[dict]:
    created = datetime(2022, 1, 1, 9, 0, tzinfo=UTC)
    rows = []
    for code, name, etype, unit, standing, term in ref.TARIFFS:
        rows.append(
            {
                "tariff_id": config.entity_uuid("tariffs", code),
                "tariff_code": code,
                "name": name,
                "energy_type": etype,
                "unit_price_eur_per_kwh": _rate(unit),
                "standing_charge_eur_per_month": _money(standing),
                "contract_term_months": term,
                "active": "true",
                "valid_from": _d(date(2022, 1, 1)),
                "valid_to": "",
                "created_at": _ts(created),
                "updated_at": _ts(created),
            }
        )
    return rows


def _build_products() -> list[dict]:
    created = datetime(2022, 1, 1, 9, 0, tzinfo=UTC)
    rows = []
    for sku, name, category, price in ref.PRODUCTS:
        rows.append(
            {
                "product_id": config.entity_uuid("products", sku),
                "sku": sku,
                "name": name,
                "category": category,
                "unit_price_eur": _money(price),
                "active": "true",
                "created_at": _ts(created),
                "updated_at": _ts(created),
            }
        )
    return rows


def _build_customers(rng: np.random.Generator) -> list[dict]:
    n = config.VOLUMES["customers"]
    rows = []
    for i in range(1, n + 1):
        signup_day = _rand_date(rng, config.HISTORY_START, config.HISTORY_END)
        signed_up = _rand_ts(rng, signup_day)
        first = ref.FIRST_NAMES[int(rng.integers(0, len(ref.FIRST_NAMES)))]
        last = ref.LAST_NAMES[int(rng.integers(0, len(ref.LAST_NAMES)))]
        city, plz = ref.CITIES[int(rng.integers(0, len(ref.CITIES)))]
        stem = ref.STREET_STEMS[int(rng.integers(0, len(ref.STREET_STEMS)))]
        suffix = ref.STREET_SUFFIXES[int(rng.integers(0, len(ref.STREET_SUFFIXES)))]
        churned = bool(rng.random() < 0.08)
        status = (
            "churned" if churned else ("inactive" if rng.random() < 0.05 else "active")
        )
        dob_year = int(rng.integers(1955, 2004))
        dob = date(dob_year, int(rng.integers(1, 13)), int(rng.integers(1, 29)))
        rows.append(
            {
                "customer_id": config.entity_uuid("customers", str(i)),
                "customer_number": f"C{i:07d}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{i}@example-mail.de",
                "phone": f"+49 {int(rng.integers(150, 179))} {int(rng.integers(1000000, 9999999))}",
                "street": f"{stem}{suffix}",
                "house_number": str(int(rng.integers(1, 199))),
                "postal_code": plz,
                "city": city,
                "country_code": "DE",
                "date_of_birth": _d(dob),
                "signed_up_at": _ts(signed_up),
                "status": status,
                "created_at": _ts(signed_up),
                "updated_at": _ts(signed_up),
            }
        )
    return rows


def _build_contracts(rng, customers, tariffs) -> list[dict]:
    elec = [t for t in tariffs if t["energy_type"] == "electricity"]
    gas = [t for t in tariffs if t["energy_type"] == "gas"]
    rows = []
    seq = 0
    for cust in customers:
        signup = datetime.strptime(
            cust["signed_up_at"], "%Y-%m-%d %H:%M:%S+00"
        ).replace(tzinfo=UTC)
        n_contracts = 1 if rng.random() < 0.85 else 2
        pools = (
            [elec]
            if n_contracts == 1 and rng.random() < 0.6
            else [elec, gas][:n_contracts]
        )
        for pool in pools:
            seq += 1
            tariff = pool[int(rng.integers(0, len(pool)))]
            start = min(
                signup.date() + timedelta(days=int(rng.integers(0, 21))),
                config.HISTORY_END,
            )
            churned = cust["status"] == "churned"
            term = tariff["contract_term_months"]
            end = None
            status = "active"
            if churned:
                end = start + timedelta(days=int(rng.integers(120, 30 * term)))
                end = max(min(end, config.HISTORY_END), start)
                status = "ended"
            elif rng.random() < 0.05:
                status = "pending"
            created = _rand_ts(rng, start)
            rows.append(
                {
                    "contract_id": config.entity_uuid("customer_contracts", str(seq)),
                    "contract_number": f"K{seq:08d}",
                    "customer_id": cust["customer_id"],
                    "tariff_id": tariff["tariff_id"],
                    "start_date": _d(start),
                    "end_date": _d(end),
                    "status": status,
                    "billing_day": int(rng.integers(1, 29)),
                    "created_at": _ts(created),
                    "updated_at": _ts(created),
                    "_energy_type": tariff["energy_type"],
                    "_start": start,
                }
            )
    return rows


def _build_meters(rng, contracts) -> list[dict]:
    rows = []
    seq = 0
    for contract in contracts:
        etype = contract.pop("_energy_type")
        start = contract.pop("_start")
        n_meters = 2 if rng.random() < 0.10 else 1
        installed = start
        for m in range(n_meters):
            seq += 1
            removed = None
            status = "active"
            if n_meters == 2 and m == 0:
                removed = installed + timedelta(days=int(rng.integers(90, 500)))
                removed = max(min(removed, config.HISTORY_END), installed)
                status = "removed"
            created = _rand_ts(rng, installed)
            rows.append(
                {
                    "meter_id": config.entity_uuid("meters", str(seq)),
                    "meter_serial": f"{'1EL' if etype == 'electricity' else '7GA'}{seq:09d}",
                    "contract_id": contract["contract_id"],
                    "meter_type": etype,
                    "melo_id": f"DE{int(rng.integers(10**10, 10**11 - 1)):011d}"
                    f"{int(rng.integers(10**9, 10**10 - 1)):010d}",
                    "installed_on": _d(installed),
                    "removed_on": _d(removed),
                    "status": status,
                    "created_at": _ts(created),
                    "updated_at": _ts(created),
                }
            )
            installed = min(
                (removed or installed) + timedelta(days=1), config.HISTORY_END
            )
    return rows


def _build_orders(rng, item_rng, customers, products) -> tuple[list[dict], list[dict]]:
    orders: list[dict] = []
    items: list[dict] = []
    order_seq = 0
    item_seq = 0
    frac = config.VOLUMES["ordering_customer_fraction"]
    for cust in customers:
        if rng.random() >= frac:
            continue
        signup = datetime.strptime(
            cust["signed_up_at"], "%Y-%m-%d %H:%M:%S+00"
        ).replace(tzinfo=UTC)
        n_orders = 1 + int(rng.poisson(1.8))
        for _ in range(min(n_orders, 8)):
            order_seq += 1
            order_day = _rand_date(rng, signup.date(), config.HISTORY_END)
            ordered_at = _rand_ts(rng, order_day)
            n_lines = 1 + int(item_rng.integers(0, 4))
            chosen = item_rng.choice(
                len(products), size=min(n_lines, len(products)), replace=False
            )
            subtotal = Decimal("0.00")
            order_id = config.entity_uuid("orders", str(order_seq))
            for pidx in sorted(int(x) for x in chosen):
                item_seq += 1
                product = products[pidx]
                qty = 1 + int(item_rng.integers(0, 3))
                unit = Decimal(product["unit_price_eur"])
                line = (unit * qty).quantize(_TWOPLACES, rounding=ROUND_HALF_UP)
                subtotal += line
                items.append(
                    {
                        "order_item_id": config.entity_uuid(
                            "order_items", str(item_seq)
                        ),
                        "order_id": order_id,
                        "product_id": product["product_id"],
                        "quantity": qty,
                        "unit_price_eur": str(unit.quantize(_TWOPLACES)),
                        "line_total_eur": str(line),
                        "created_at": _ts(ordered_at),
                    }
                )
            shipping = (
                Decimal("0.00") if subtotal >= Decimal("50.00") else Decimal("4.95")
            )
            total = subtotal + shipping
            status = _order_status(rng, order_day)
            orders.append(
                {
                    "order_id": order_id,
                    "order_number": f"O{order_seq:08d}",
                    "customer_id": cust["customer_id"],
                    "order_status": status,
                    "ordered_at": _ts(ordered_at),
                    "currency": "EUR",
                    "items_subtotal_eur": str(subtotal),
                    "shipping_fee_eur": str(shipping),
                    "total_eur": str(total),
                    "created_at": _ts(ordered_at),
                    "updated_at": _ts(ordered_at),
                }
            )
    return orders, items


def _order_status(rng, order_day: date) -> str:
    age = (config.HISTORY_END - order_day).days
    if rng.random() < 0.06:
        return "cancelled" if rng.random() < 0.7 else "refunded"
    if age > 21:
        return "delivered"
    if age > 10:
        return "shipped"
    if age > 3:
        return "paid"
    return "placed"
