# Redispatch data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: download the historical redispatch-measures archive (2013-2020)
# from netztransparenz.de.
#
# Not covered: the current publication (~Oct 2023 -> present) sits behind a
# JS date-range picker with no direct endpoint found; needs OAuth WebAPI
# credentials this project doesn't hold, or a picker-request investigation.
#
# Licence: no open-data licence, reproduction needs written TSO consent --
# kept in scope since acquired data stays local-only and is never committed.

import io
import sys
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

ARCHIVE_URL = (
    "https://www.netztransparenz.de/xspproxy/api/staticfiles/ntp-relaunch/dokumente/"
    "systemdienstleistungen/betriebsf%C3%BChrung/redispatch/archiv/"
    "2025-09-17%20redispatch%20export%202013-2020.zip"
)
REQUEST_TIMEOUT = 60

OUTPUT_DIR = DATA_RAW_DIR / "redispatch"


def download_archive() -> None:
    dest_dir = OUTPUT_DIR / "archive_2013_2020"
    if dest_dir.exists() and any(dest_dir.iterdir()):
        logger.info(f"Redispatch archive already downloaded, skipping: {dest_dir}")
        return

    response = requests.get(ARCHIVE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(dest_dir)

    logger.info(
        f"Downloaded Redispatch 2013-2020 archive ({len(response.content)} bytes) -> {dest_dir}"
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    download_archive()
    logger.warning("Current publication (~Oct 2023 -> present) NOT acquired this run.")
    logger.info("Redispatch download complete (archive only).")


if __name__ == "__main__":
    main()
