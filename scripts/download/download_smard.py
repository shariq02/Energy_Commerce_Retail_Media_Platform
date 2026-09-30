# SMARD energy market data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: download German energy market data (day-ahead prices, generation,
# consumption, forecasts) from the SMARD public API, at daily and quarter-hour
# resolution and across the German control-area regions.

import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

BASE_URL = "https://www.smard.de/app/chart_data"
REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.5

# Filter code -> output name.
FILTERS = {
    4169: "day_ahead_prices",
    410: "total_power_consumption",
    4359: "residual_load",
    4387: "pumped_storage_consumption",
    1223: "generation_lignite",
    1224: "generation_nuclear",
    1225: "generation_offshore_wind",
    1226: "generation_hydro",
    1227: "generation_other_conventional",
    1228: "generation_other_renewable",
    4066: "generation_biomass",
    4067: "generation_onshore_wind",
    4068: "generation_photovoltaic",
    4069: "generation_hard_coal",
    4070: "generation_pumped_storage",
    4071: "generation_natural_gas",
}

FORECAST_FILTERS = {
    122: "forecast_generation_total",
    123: "forecast_generation_onshore_wind",
    3791: "forecast_generation_offshore_wind",
    126: "forecast_generation_photovoltaic",
    715: "forecast_generation_other",
    5097: "forecast_generation_wind_and_photovoltaic",
}

# Subset of FILTERS, kept small to bound request count.
CONTROL_AREAS = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]
CONTROL_AREA_FILTERS = {
    410: "total_power_consumption",
    4067: "generation_onshore_wind",
    4068: "generation_photovoltaic",
    1225: "generation_offshore_wind",
}

# Quarterhour paginates in ~weekly chunks (~600/series for full history);
# bounded here to the last ~2 years instead.
RECENT_QUARTERHOUR_CHUNKS = 104

# Not pulled: cross-border flows, intraday prices, Ausgleichsenergie --
# filter IDs not yet confirmed against the live API.

OUTPUT_DIR = DATA_RAW_DIR / "smard"


def fetch_index(filter_id: int, region: str, resolution: str) -> list[int] | None:
    """None if this filter/region combination doesn't exist."""
    url = f"{BASE_URL}/{filter_id}/{region}/index_{resolution}.json"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["timestamps"]


def fetch_series(filter_id: int, region: str, resolution: str, timestamp: int) -> dict:
    url = f"{BASE_URL}/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{timestamp}.json"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def download_series(
    filter_id: int,
    region: str,
    resolution: str,
    name: str,
    dest_subdir: Path,
    limit_chunks: int | None = None,
) -> None:
    dest = dest_subdir / f"{name}.json"
    if dest.exists():
        logger.info(f"SMARD {dest} already downloaded, skipping")
        return

    logger.info(
        f"Fetching SMARD index for {name} (filter {filter_id}, {region}, {resolution})"
    )
    timestamps = fetch_index(filter_id, region, resolution)
    if timestamps is None:
        logger.warning(
            f"No SMARD series for filter {filter_id} in region {region} - skipping"
        )
        return
    if limit_chunks is not None:
        timestamps = timestamps[-limit_chunks:]

    series: list[list] = []
    for timestamp in timestamps:
        payload = fetch_series(filter_id, region, resolution, timestamp)
        series.extend(payload.get("series", []))
        time.sleep(REQUEST_DELAY_SECONDS)

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w") as f:
        json.dump(
            {
                "filter_id": filter_id,
                "region": region,
                "resolution": resolution,
                "series": series,
            },
            f,
        )

    logger.info(f"Wrote {len(series)} records to {dest}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # First-wave series: daily, DE-LU (unchanged, already acquired).
    for filter_id, name in FILTERS.items():
        download_series(filter_id, "DE-LU", "day", name, OUTPUT_DIR)

    # Deepening: forecast generation, daily, DE-LU.
    for filter_id, name in FORECAST_FILTERS.items():
        download_series(filter_id, "DE-LU", "day", name, OUTPUT_DIR)

    # Deepening: control-area breakdown, daily.
    for area in CONTROL_AREAS:
        area_dir = OUTPUT_DIR / "control_area" / area
        for filter_id, name in CONTROL_AREA_FILTERS.items():
            download_series(filter_id, area, "day", name, area_dir)

    # Deepening: quarter-hour resolution, DE-LU, recent window.
    quarterhour_dir = OUTPUT_DIR / "quarterhour"
    for filter_id, name in FILTERS.items():
        download_series(
            filter_id,
            "DE-LU",
            "quarterhour",
            name,
            quarterhour_dir,
            limit_chunks=RECENT_QUARTERHOUR_CHUNKS,
        )

    logger.info("SMARD download complete.")


if __name__ == "__main__":
    main()
