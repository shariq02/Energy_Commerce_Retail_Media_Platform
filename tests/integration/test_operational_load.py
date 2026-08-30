"""Synthetic operational data -- schema applies and the seed loads cleanly.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Hermetic: builds the schema in a throwaway namespace inside one transaction and
rolls back -- nothing persists. Skips when no PostgreSQL is reachable.
"""

from __future__ import annotations

import re

import pytest

from src.generators import config

pytestmark = pytest.mark.integration

_TEST_SCHEMA = "operational_pytest"
_MIGRATION = (
    config.REPO_ROOT / "postgres" / "migrations" / "0001_initial_operational_schema.sql"
)


def _migration_sql(schema: str) -> str:
    sql = _MIGRATION.read_text(encoding="utf-8")
    sql = re.sub(r"^\s*(BEGIN|COMMIT)\s*;\s*$", "", sql, flags=re.MULTILINE)
    sql = re.sub(
        r"^INSERT INTO public\.schema_migrations.*$", "", sql, flags=re.MULTILINE
    )
    sql = sql.replace("operational", schema)
    return sql


@pytest.fixture
def loaded(pg_conn):
    """Apply the schema and COPY every seed CSV, all inside pg_conn's transaction."""
    if not (config.SEED_DIR / "orders.csv").exists():
        pytest.skip("seed CSVs not generated -- run src/generators/main.py")
    with pg_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {_TEST_SCHEMA} CASCADE")
        cur.execute(_migration_sql(_TEST_SCHEMA))
        for table in config.TABLE_LOAD_ORDER:
            cols = ", ".join(config.TABLE_COLUMNS[table])
            with (config.SEED_DIR / f"{table}.csv").open(encoding="utf-8") as fh:
                cur.copy_expert(
                    f"COPY {_TEST_SCHEMA}.{table} ({cols}) "
                    "FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')",
                    fh,
                )
    return pg_conn


def _count(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_all_ten_tables_created(loaded):
    n = _count(
        loaded,
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{_TEST_SCHEMA}'",
    )
    assert n == 10


@pytest.mark.parametrize("table", config.TABLE_LOAD_ORDER)
def test_row_count_matches_csv(loaded, table):
    csv_rows = (
        sum(1 for _ in (config.SEED_DIR / f"{table}.csv").open(encoding="utf-8")) - 1
    )
    assert _count(loaded, f"SELECT count(*) FROM {_TEST_SCHEMA}.{table}") == csv_rows


def test_no_orphan_rows(loaded):
    assert (
        _count(
            loaded,
            f"SELECT count(*) FROM {_TEST_SCHEMA}.order_items oi "
            f"LEFT JOIN {_TEST_SCHEMA}.orders o USING (order_id) WHERE o.order_id IS NULL",
        )
        == 0
    )
    assert (
        _count(
            loaded,
            f"SELECT count(*) FROM {_TEST_SCHEMA}.meters m "
            f"LEFT JOIN {_TEST_SCHEMA}.customer_contracts c USING (contract_id) "
            "WHERE c.contract_id IS NULL",
        )
        == 0
    )


def test_order_header_matches_line_totals(loaded):
    assert (
        _count(
            loaded,
            f"SELECT count(*) FROM {_TEST_SCHEMA}.orders o JOIN ("
            f"  SELECT order_id, sum(line_total_eur) s FROM {_TEST_SCHEMA}.order_items "
            "  GROUP BY 1) x USING (order_id) "
            "WHERE o.items_subtotal_eur <> x.s OR o.total_eur <> o.items_subtotal_eur + o.shipping_fee_eur",
        )
        == 0
    )


def test_check_constraints_are_enforced(loaded):
    import psycopg2

    with loaded.cursor() as cur:
        cur.execute(f"SELECT customer_id FROM {_TEST_SCHEMA}.customers LIMIT 1")
        customer_id = cur.fetchone()[0]
        cur.execute(f"SELECT tariff_id FROM {_TEST_SCHEMA}.tariffs LIMIT 1")
        tariff_id = cur.fetchone()[0]
        cur.execute("SAVEPOINT s")
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                f"INSERT INTO {_TEST_SCHEMA}.customer_contracts "
                "(contract_id, contract_number, customer_id, tariff_id, start_date, "
                " end_date, status, billing_day, created_at, updated_at) VALUES "
                "(gen_random_uuid(), 'K-BAD', %s, %s, '2025-06-01', '2025-01-01', "
                " 'ended', 5, now(), now())",
                (customer_id, tariff_id),
            )
        cur.execute("ROLLBACK TO s")
