# MaStR (Marktstammdatenregister) data download
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: download the current Gesamtdatenexport (full data export) of the
# German energy-asset registry from the Bundesnetzagentur.

import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config import DATA_RAW_DIR, get_logger

logger = get_logger(__name__)

DOWNLOAD_PAGE_URL = "https://www.marktstammdatenregister.de/MaStR/Datendownload"
REQUEST_TIMEOUT = 60
STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MB

OUTPUT_DIR = DATA_RAW_DIR / "mastr"


def resolve_current_export_url() -> str:
    """Republished daily under a dated filename -- resolve it rather than hardcode it."""
    response = requests.get(DOWNLOAD_PAGE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    match = re.search(
        r"https://download\.marktstammdatenregister\.de/Gesamtdatenexport_\d{8}_[\d.]+\.zip",
        response.text,
    )
    if not match:
        raise RuntimeError(
            f"Could not find current Gesamtdatenexport link on {DOWNLOAD_PAGE_URL}"
        )
    return match.group(0)


def download_export(url: str) -> Path:
    filename = url.rsplit("/", 1)[-1]
    dest = OUTPUT_DIR / filename

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        expected_bytes = int(response.headers.get("Content-Length", 0))

        if dest.exists() and dest.stat().st_size == expected_bytes:
            logger.info(f"MaStR export already downloaded, skipping: {dest}")
            return dest

        logger.info(
            f"Downloading MaStR Gesamtdatenexport ({expected_bytes / 1024**3:.2f} GB) -> {dest}"
        )
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        written_bytes = 0
        last_log = time.monotonic()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                f.write(chunk)
                written_bytes += len(chunk)
                if time.monotonic() - last_log > 5:
                    pct = written_bytes / expected_bytes * 100 if expected_bytes else 0
                    logger.info(
                        f"MaStR progress: {written_bytes / 1024**3:.2f} / "
                        f"{expected_bytes / 1024**3:.2f} GB ({pct:.0f}%)"
                    )
                    last_log = time.monotonic()

    if expected_bytes and written_bytes != expected_bytes:
        dest.unlink(missing_ok=True)
        raise RuntimeError(
            f"MaStR download incomplete: expected {expected_bytes} bytes, got {written_bytes}"
        )

    logger.info(
        f"Downloaded MaStR Gesamtdatenexport: {written_bytes / 1024**3:.2f} GB -> {dest}"
    )
    return dest


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    export_url = resolve_current_export_url()
    download_export(export_url)
    logger.info("MaStR download complete.")


if __name__ == "__main__":
    main()
