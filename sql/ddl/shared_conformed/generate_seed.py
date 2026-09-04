"""Generate the shared_conformed seed CSVs -- dim_date, dim_time, dim_geography.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: September 2026

Purpose: deterministic, evidence-independent reference data for the
conformed dimensions. dim_date and dim_time are pure calendar math -- no
acquired source is needed. dim_geography's Bundesland seed is fixed,
official public reference data (the 16 German federal states' AGS 2-digit
Land codes plus the nation row) -- not derived from any ecosystem's acquired
evidence, so populating it here does not cross the "no ecosystem contents"
boundary. Regierungsbezirk / Kreis / Gemeinde rows are deliberately NOT
generated -- see 02_dim_geography.sql's header comment.

Usage:
    PYTHONPATH=. python3 sql/ddl/shared_conformed/generate_seed.py
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent / "seed"

# dim_date range: wide enough to cover the operational history window
# (from 2023-09-01) and a reasonable planning horizon, without generating an
# arbitrarily unbounded table.
DATE_RANGE_START = date(2016, 1, 1)
DATE_RANGE_END = date(2031, 12, 31)

_MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

# The 16 German Bundeslaender, official AGS 2-digit "Land" codes -- a fixed,
# unchanging public administrative fact (not ecosystem evidence).
_BUNDESLAENDER = [
    ("01", "Schleswig-Holstein"),
    ("02", "Hamburg"),
    ("03", "Niedersachsen"),
    ("04", "Bremen"),
    ("05", "Nordrhein-Westfalen"),
    ("06", "Hessen"),
    ("07", "Rheinland-Pfalz"),
    ("08", "Baden-Wuerttemberg"),
    ("09", "Bayern"),
    ("10", "Saarland"),
    ("11", "Berlin"),
    ("12", "Brandenburg"),
    ("13", "Mecklenburg-Vorpommern"),
    ("14", "Sachsen"),
    ("15", "Sachsen-Anhalt"),
    ("16", "Thueringen"),
]


def generate_dim_date(path: Path) -> int:
    rows = []
    d = DATE_RANGE_START
    while d <= DATE_RANGE_END:
        _iso_year, iso_week, iso_weekday = d.isocalendar()
        rows.append(
            {
                "date_key": d.strftime("%Y%m%d"),
                "calendar_date": d.isoformat(),
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "month_name": _MONTH_NAMES[d.month - 1],
                "day_of_month": d.day,
                "day_of_year": d.timetuple().tm_yday,
                "iso_week": iso_week,
                "day_of_week": iso_weekday,
                "day_name": _DAY_NAMES[iso_weekday - 1],
                "is_weekend": iso_weekday in (6, 7),
            }
        )
        d += timedelta(days=1)

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def generate_dim_time(path: Path) -> int:
    rows = []
    for minute_of_day in range(24 * 60):
        hour_24 = minute_of_day // 60
        minute = minute_of_day % 60
        hour_12 = hour_24 % 12
        hour_12 = 12 if hour_12 == 0 else hour_12
        am_pm = "AM" if hour_24 < 12 else "PM"
        quarter_hour = minute // 15
        if 5 <= hour_24 < 12:
            daypart = "morning"
        elif 12 <= hour_24 < 17:
            daypart = "afternoon"
        elif 17 <= hour_24 < 21:
            daypart = "evening"
        else:
            daypart = "night"
        rows.append(
            {
                "time_key": minute_of_day,
                "hour_24": hour_24,
                "minute": minute,
                "hour_12": hour_12,
                "am_pm": am_pm,
                "quarter_hour": quarter_hour,
                "daypart": daypart,
            }
        )

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def generate_dim_geography_bundeslaender(path: Path) -> int:
    rows = [
        {
            "ags_code": "00",
            "ars_code": "00",
            "level": "nation",
            "name": "Deutschland",
            "parent_ags_code": "",
            "nuts_code": "DE",
            "valid_from": "1990-10-03",
            "valid_to": "",
            "source_system": "bkg_destatis_ags",
        }
    ]
    for ags_code, name in _BUNDESLAENDER:
        rows.append(
            {
                "ags_code": ags_code,
                "ars_code": ags_code,
                "level": "bundesland",
                "name": name,
                "parent_ags_code": "00",
                "nuts_code": "",  # NUTS-1 crosswalk: populate from an official
                # BKG/Destatis source later, not asserted
                # here without independent verification
                "valid_from": "1990-10-03",
                "valid_to": "",
                "source_system": "bkg_destatis_ags",
            }
        )

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    n_date = generate_dim_date(OUT_DIR / "dim_date.csv")
    n_time = generate_dim_time(OUT_DIR / "dim_time.csv")
    n_geo = generate_dim_geography_bundeslaender(
        OUT_DIR / "dim_geography_bundeslaender.csv"
    )
    print(f"dim_date.csv: {n_date} rows ({DATE_RANGE_START} .. {DATE_RANGE_END})")
    print(f"dim_time.csv: {n_time} rows (0..1439 minutes)")
    print(f"dim_geography_bundeslaender.csv: {n_geo} rows (nation + 16 Bundeslaender)")


if __name__ == "__main__":
    main()
