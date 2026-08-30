"""Synthetic operational data -- load the seed CSVs into PostgreSQL.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: truncate the operational tables and COPY the postgres/seed/*.csv files
back in, in FK-safe order. Separate step from generation. Connection settings
come from the environment (POSTGRES_* -> database `ecrmap`, schema `operational`).

Usage:
    PYTHONPATH=. python3 src/generators/load_seed.py
"""

from __future__ import annotations

import os
import sys

import psycopg2
from dotenv import load_dotenv

from src.generators import config as gen

SCHEMA = "operational"

load_dotenv()
DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ecrmap"),
    "user": os.getenv("POSTGRES_USER", ""),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}


def main() -> int:
    missing = [
        gen.SEED_DIR / f"{t}.csv"
        for t in gen.TABLE_LOAD_ORDER
        if not (gen.SEED_DIR / f"{t}.csv").exists()
    ]
    if missing:
        print(
            f"seed files missing: {[p.name for p in missing]} -- run main.py first",
            file=sys.stderr,
        )
        return 2

    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}, public")
            qualified = ", ".join(f"{SCHEMA}.{t}" for t in gen.TABLE_LOAD_ORDER)
            cur.execute(f"TRUNCATE {qualified} RESTART IDENTITY CASCADE")
            for table in gen.TABLE_LOAD_ORDER:
                path = gen.SEED_DIR / f"{table}.csv"
                cols = ", ".join(gen.TABLE_COLUMNS[table])
                with path.open("r", encoding="utf-8") as handle:
                    cur.copy_expert(
                        f"COPY {SCHEMA}.{table} ({cols}) "
                        "FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')",
                        handle,
                    )
                cur.execute(f"SELECT count(*) FROM {SCHEMA}.{table}")
                print(f"  {table:<20} {cur.fetchone()[0]:>7} rows")
        conn.commit()
        print("load complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
