# BNetzA power plant list (Kraftwerksliste) data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: download the Bundesnetzagentur power-plant list (Kraftwerksliste)
# and its planned-capacity companion sheet.

import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

BASE_URL = (
    "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/"
    "Versorgungssicherheit/Erzeugungskapazitaeten/Kraftwerksliste/_DL"
)
REQUEST_TIMEOUT = 60

# filename -> URL path
FILES = {
    "Kraftwerksliste.xlsx": "Kraftwerksliste.xlsx?__blob=publicationFile&v=16",
    "Kraftwerksliste_CSV.csv": "Kraftwerksliste_CSV.csv?__blob=publicationFile&v=9",
    "ZuUndRueckbau.xlsx": "ZuUndRueckbau.xlsx?__blob=publicationFile&v=12",
}

OUTPUT_DIR = DATA_RAW_DIR / "kraftwerksliste"


def download_file(filename: str, url_suffix: str) -> None:
    dest = OUTPUT_DIR / filename
    if dest.exists():
        logger.info(f"Kraftwerksliste {filename} already downloaded, skipping: {dest}")
        return

    url = f"{BASE_URL}/{url_suffix}"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    logger.info(f"Downloaded {filename} ({len(response.content)} bytes) -> {dest}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, url_suffix in FILES.items():
        download_file(filename, url_suffix)
    logger.info("Kraftwerksliste download complete.")


if __name__ == "__main__":
    main()
