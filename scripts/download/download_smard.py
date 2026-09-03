# SMARD energy market data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: download German energy market data (day-ahead prices,
# generation, consumption) from the SMARD public API.

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

BASE_URL = "https://www.smard.de/app/chart_data"
REGION = "DE-LU"
RESOLUTION = "day"
REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.5

# Filter code -> output name. Day-ahead price (4169) confirmed live against the
# SMARD API; generation/consumption codes per bundesAPI/smard-api documentation.
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

OUTPUT_DIR = DATA_RAW_DIR / "smard"


def fetch_index(filter_id: int) -> list[int]:
    url = f"{BASE_URL}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()["timestamps"]


def fetch_series(filter_id: int, timestamp: int) -> dict:
    url = f"{BASE_URL}/{filter_id}/{REGION}/{filter_id}_{REGION}_{RESOLUTION}_{timestamp}.json"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def download_filter(filter_id: int, name: str) -> None:
    dest = OUTPUT_DIR / f"{name}.json"
    if dest.exists():
        logger.info(
            f"SMARD {name} (filter {filter_id}) already downloaded, skipping: {dest}"
        )
        return

    logger.info(f"Fetching SMARD index for {name} (filter {filter_id})")
    timestamps = fetch_index(filter_id)

    series: list[list] = []
    for timestamp in timestamps:
        payload = fetch_series(filter_id, timestamp)
        series.extend(payload.get("series", []))
        time.sleep(REQUEST_DELAY_SECONDS)

    dest.parent.mkdir(parents=True, exist_ok=True)
    import json

    with open(dest, "w") as f:
        json.dump(
            {
                "filter_id": filter_id,
                "region": REGION,
                "resolution": RESOLUTION,
                "series": series,
            },
            f,
        )

    logger.info(f"Wrote {len(series)} records to {dest}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for filter_id, name in FILTERS.items():
        download_filter(filter_id, name)
    logger.info("SMARD download complete.")


if __name__ == "__main__":
    main()
