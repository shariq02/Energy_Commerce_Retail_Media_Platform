"""shared_conformed DDL + seed data -- structure and content checks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

No database required -- checks the DDL files and the generated seed CSVs
directly.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pytest

pytestmark = [pytest.mark.schema, pytest.mark.unit]

_DDL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl" / "shared_conformed"
_SEED_DIR = _DDL_DIR / "seed"


@pytest.mark.parametrize(
    "filename",
    [
        "00_dim_date.sql",
        "01_dim_time.sql",
        "02_dim_geography.sql",
        "03_geo_plz_gemeinde_xref.sql",
        "04_dim_weather_context.sql",
    ],
)
def test_ddl_file_exists_and_defines_its_table(filename):
    path = _DDL_DIR / filename
    assert path.exists(), f"missing {filename}"
    sql = path.read_text(encoding="utf-8")
    table_name = filename.split("_", 1)[1].removesuffix(".sql")
    assert f"CREATE TABLE shared_conformed.{table_name}" in sql


def _read_csv(name: str) -> list[dict]:
    path = _SEED_DIR / name
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_dim_date_seed_covers_the_operational_history_window():
    rows = _read_csv("dim_date.csv")
    dates = {dt.date.fromisoformat(r["calendar_date"]) for r in rows}
    # src/generators/config.py HISTORY_START/HISTORY_END -- the operational
    # seed's own date range -- must fall entirely inside dim_date's range.
    assert dt.date(2023, 9, 1) in dates
    assert dt.date(2026, 8, 30) in dates


def test_dim_date_seed_has_no_gaps():
    rows = _read_csv("dim_date.csv")
    dates = sorted(dt.date.fromisoformat(r["calendar_date"]) for r in rows)
    assert dates == [dates[0] + dt.timedelta(days=i) for i in range(len(dates))], (
        "dim_date must be one row per consecutive calendar day, no gaps"
    )


def test_dim_date_date_key_matches_calendar_date():
    rows = _read_csv("dim_date.csv")
    for r in rows[:200]:  # bounded sample, not the full 5,844 rows
        expected_key = r["calendar_date"].replace("-", "")
        assert r["date_key"] == expected_key


def test_dim_time_covers_every_minute_of_day_exactly_once():
    rows = _read_csv("dim_time.csv")
    keys = sorted(int(r["time_key"]) for r in rows)
    assert keys == list(range(24 * 60))


def test_dim_geography_bundeslaender_has_nation_plus_sixteen_states():
    rows = _read_csv("dim_geography_bundeslaender.csv")
    levels = [r["level"] for r in rows]
    assert levels.count("nation") == 1
    assert levels.count("bundesland") == 16
    assert len(rows) == 17


def test_dim_geography_bundeslaender_ags_codes_are_unique_and_zero_padded():
    rows = _read_csv("dim_geography_bundeslaender.csv")
    ags_codes = [r["ags_code"] for r in rows]
    assert len(ags_codes) == len(set(ags_codes))
    for code in ags_codes:
        assert len(code) == 2 and code.isdigit()


def test_dim_geography_bundeslaender_every_state_has_the_nation_as_parent():
    rows = _read_csv("dim_geography_bundeslaender.csv")
    nation = next(r for r in rows if r["level"] == "nation")
    assert nation["parent_ags_code"] == ""
    for r in rows:
        if r["level"] == "bundesland":
            assert r["parent_ags_code"] == nation["ags_code"]
