# ====================================================================
# DWD Weather Data Download
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Download German weather station data (temperature,
# precipitation, wind, and related categories) from the DWD open-data server.
# ====================================================================

import io
import re
import sys
import unicodedata
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

BASE_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly"
REQUEST_TIMEOUT = 60

# Category -> DWD parameter abbreviation, matching docs/DATA_SOURCES_20260813_v3.md
# Section 3 fields (temperature, precipitation, humidity, pressure, wind, sunshine,
# cloud conditions). Solar radiation is DWD's "solar" category, which uses a
# different directory layout (no recent/historical split, "_row" suffix instead
# of "_akt") and is out of scope for this initial script.
CATEGORIES = {
    "air_temperature": "TU",
    "precipitation": "RR",
    "moisture": "TF",
    "pressure": "P0",
    "wind": "FF",
    "sun": "SD",
    "cloudiness": "N",
}

# Curated 8-station list, per docs/PROJECT_PLAN_20260813_v3.md Section 15.
# Matched against DWD's station name field (substring, case-insensitive,
# diacritics stripped via _normalize() since DWD's own file uses German
# umlauts that this codebase keeps out of source text).
STATIONS = {
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "munich": "Munchen",
    "frankfurt_am_main": "Frankfurt",
    "cologne": "Koln",
    "leipzig": "Leipzig",
    "rostock_warnemuende": "Rostock-Warnemuende",
    "garmisch_partenkirchen": "Garmisch-Partenkirchen",
}

OUTPUT_DIR = DATA_RAW_DIR / "dwd"


def _normalize(text: str) -> str:
    """ASCII-fold text by stripping diacritics (u-umlaut -> u, etc.),
    so station-name matching works without non-ASCII characters in source."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def fetch_active_station_ids(category: str, param: str) -> set[str]:
    """IDs with a currently-published _akt.zip file in {category}/recent/ - the
    full station description file also lists historical/inactive stations, whose
    IDs have no live data file."""
    url = f"{BASE_URL}/{category}/recent/"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return set(re.findall(rf"stundenwerte_{param}_(\d{{5}})_akt\.zip", response.text))


def resolve_station_ids() -> dict[str, str]:
    """Resolve curated station names to DWD station IDs via the air_temperature
    station description file, restricted to stations with a currently-active
    (recent/) data file - physical station IDs are stable across categories."""
    active_ids = fetch_active_station_ids("air_temperature", "TU")

    url = f"{BASE_URL}/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    lines = response.content.decode("latin-1").splitlines()

    resolved: dict[str, str] = {}
    for line in lines[2:]:  # skip header + separator row
        if not line.strip():
            continue
        station_id = line[0:5].strip()
        if station_id not in active_ids:
            continue
        station_name = line[61:].strip() if len(line) > 61 else ""
        normalized_station_name = _normalize(station_name).lower()
        for key, match_name in STATIONS.items():
            if key not in resolved and _normalize(match_name).lower() in normalized_station_name:
                resolved[key] = station_id

    missing = set(STATIONS) - set(resolved)
    if missing:
        logger.warning(f"Could not resolve DWD station IDs for: {sorted(missing)}")
    return resolved


def download_station_category(station_key: str, station_id: str, category: str, param: str) -> None:
    dest_dir = OUTPUT_DIR / station_key / category
    if dest_dir.exists() and any(dest_dir.iterdir()):
        logger.info(f"DWD {station_key}/{category} already downloaded, skipping: {dest_dir}")
        return

    url = f"{BASE_URL}/{category}/recent/stundenwerte_{param}_{station_id}_akt.zip"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        logger.warning(f"No {category} data available for station {station_key} ({station_id}): {url}")
        return
    response.raise_for_status()

    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(dest_dir)

    logger.info(f"Downloaded DWD {category} for {station_key} ({station_id}) -> {dest_dir}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    station_ids = resolve_station_ids()
    for station_key, station_id in station_ids.items():
        for category, param in CATEGORIES.items():
            download_station_category(station_key, station_id, category, param)

    logger.info("DWD download complete.")


if __name__ == "__main__":
    main()
