# DWD weather data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: download German weather station data (temperature, precipitation,
# wind, and related categories) from the DWD open-data server.

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

# Category -> DWD parameter abbreviation. Standard categories share the
# {category}/{recent,historical}/stundenwerte_{param}_{station}_{suffix}.zip
# layout. "solar" is handled separately below: flat directory, no
# recent/historical split, "_row" filename suffix instead of "_akt"/"_hist".
CATEGORIES = {
    "air_temperature": "TU",
    "precipitation": "RR",
    "moisture": "TF",
    "pressure": "P0",
    "wind": "FF",
    "sun": "SD",
    "cloudiness": "N",
    "dew_point": "TD",
    "soil_temperature": "EB",
    "visibility": "VV",
    "cloud_type": "CS",
    "wind_synop": "F",
    "extreme_wind": "FX",
    "weather_phenomena": "WW",
}
SOLAR_PARAM = "ST"

# Germany-wide stratified network (population centres, climate regimes,
# energy/agricultural relevance, historical continuity) -- see groupings
# below. Values matched as substrings against DWD's station-name field after
# _normalize(); compound/trailing-space forms disambiguate stations whose
# names would otherwise collide (e.g. "Nurnberg " vs "Nurnberg-Netzstall").
STATIONS = {
    # Population / economic centres
    "berlin": "Berlin-Tempelhof",
    "hamburg": "Hamburg-Fuhlsbuttel",
    "munich": "Munchen-Flughafen",
    "cologne_bonn": "Koln/Bonn",
    "frankfurt_am_main": "Frankfurt/Main",
    "stuttgart": "Stuttgart",
    "essen": "Essen-Bredeney",
    "leipzig": "Leipzig-Holzhausen",
    "dresden": "Dresden-Klotzsche",
    "nuremberg": "Nurnberg ",
    "hannover": "Hannover",
    "bremen": "Bremen",
    # State capitals / regional anchors not already covered above
    "potsdam": "Potsdam",
    "magdeburg": "Magdeburg",
    "erfurt": "Erfurt-Weimar",
    "trier": "Trier-Petrisberg",
    "saarbruecken": "Saarbrucken-Ensheim",
    "kiel": "Kiel-Holtenau",
    # Maritime / coastal wind-energy regimes
    "rostock_warnemuende": "Rostock-Warnemunde",
    "norderney": "Norderney",
    "sylt": "List auf Sylt",
    # Alpine / highland climate extremes
    "garmisch_partenkirchen": "Garmisch-Partenkirchen",
    "zugspitze": "Zugspitze",
    "hohenpeissenberg": "Hohenpeienberg",
    "feldberg_schwarzwald": "Feldberg/Schwarzwald",
    # Energy (lignite) and continental-climate regions
    "cottbus": "Cottbus",
    "goerlitz": "Gorlitz",
    # Agricultural regions not already covered above
    "braunschweig": "Braunschweig",
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

    url = (
        f"{BASE_URL}/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt"
    )
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
            if (
                key not in resolved
                and _normalize(match_name).lower() in normalized_station_name
            ):
                resolved[key] = station_id

    missing = set(STATIONS) - set(resolved)
    if missing:
        logger.warning(f"Could not resolve DWD station IDs for: {sorted(missing)}")
    return resolved


def _extract_zip(content: bytes, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        archive.extractall(dest_dir)


def download_recent(
    station_key: str, station_id: str, category: str, param: str
) -> None:
    dest_dir = OUTPUT_DIR / station_key / category / "recent"
    if dest_dir.exists() and any(dest_dir.iterdir()):
        logger.info(f"DWD {station_key}/{category}/recent already downloaded, skipping")
        return

    url = f"{BASE_URL}/{category}/recent/stundenwerte_{param}_{station_id}_akt.zip"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        logger.warning(
            f"No {category}/recent data for station {station_key} ({station_id}): {url}"
        )
        return
    response.raise_for_status()
    _extract_zip(response.content, dest_dir)
    logger.info(
        f"Downloaded DWD {category}/recent for {station_key} ({station_id}) -> {dest_dir}"
    )


def download_historical(
    station_key: str, station_id: str, category: str, param: str
) -> None:
    dest_dir = OUTPUT_DIR / station_key / category / "historical"
    if dest_dir.exists() and any(dest_dir.iterdir()):
        logger.info(
            f"DWD {station_key}/{category}/historical already downloaded, skipping"
        )
        return

    index_url = f"{BASE_URL}/{category}/historical/"
    index_response = requests.get(index_url, timeout=REQUEST_TIMEOUT)
    if index_response.status_code == 404:
        logger.warning(f"No {category}/historical index: {index_url}")
        return
    index_response.raise_for_status()

    match = re.search(
        rf"stundenwerte_{param}_{station_id}_\d{{8}}_\d{{8}}_hist\.zip",
        index_response.text,
    )
    if not match:
        logger.warning(
            f"No {category}/historical file for station {station_key} ({station_id})"
        )
        return

    filename = match.group(0)
    response = requests.get(f"{index_url}{filename}", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    _extract_zip(response.content, dest_dir)
    logger.info(
        f"Downloaded DWD {category}/historical for {station_key} ({station_id}) -> {dest_dir}"
    )


def download_solar(station_key: str, station_id: str) -> None:
    dest_dir = OUTPUT_DIR / station_key / "solar"
    if dest_dir.exists() and any(dest_dir.iterdir()):
        logger.info(f"DWD {station_key}/solar already downloaded, skipping")
        return

    url = f"{BASE_URL}/solar/stundenwerte_{SOLAR_PARAM}_{station_id}_row.zip"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        logger.warning(f"No solar data for station {station_key} ({station_id}): {url}")
        return
    response.raise_for_status()
    _extract_zip(response.content, dest_dir)
    logger.info(f"Downloaded DWD solar for {station_key} ({station_id}) -> {dest_dir}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    station_ids = resolve_station_ids()
    for station_key, station_id in station_ids.items():
        for category, param in CATEGORIES.items():
            download_recent(station_key, station_id, category, param)
            download_historical(station_key, station_id, category, param)
        download_solar(station_key, station_id)

    logger.info("DWD download complete.")


if __name__ == "__main__":
    main()
