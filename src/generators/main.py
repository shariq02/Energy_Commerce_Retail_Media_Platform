"""Synthetic operational data generator -- entry point.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: build all 7 operational tables deterministically and write them as CSV
to postgres/seed/. Database loading is a separate step (load_seed.py).

Usage:
    PYTHONPATH=. python3 src/generators/main.py
"""

from __future__ import annotations

import csv
import hashlib

from src.generators import build, config


def _write_csv(table: str, rows: list[dict]) -> int:
    columns = config.TABLE_COLUMNS[table]
    path = config.SEED_DIR / f"{table}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[col] for col in columns])
    return path.stat().st_size


def main() -> int:
    config.SEED_DIR.mkdir(parents=True, exist_ok=True)
    tables = build.build_all()

    digest = hashlib.sha256()
    print(f"seed dir: {config.SEED_DIR}")
    for table in config.TABLE_LOAD_ORDER:
        rows = tables[table]
        _write_csv(table, rows)
        digest.update((config.SEED_DIR / f"{table}.csv").read_bytes())
        print(f"  {table:<20} {len(rows):>7} rows")

    total = sum(len(tables[t]) for t in config.TABLE_LOAD_ORDER)
    print(f"  {'TOTAL':<20} {total:>7} rows")
    print(f"seed digest (sha256): {digest.hexdigest()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
