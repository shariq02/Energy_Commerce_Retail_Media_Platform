# KDD Cup 2012 Track 2 staging
# Energy Commerce and Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: stage the single KDD Cup 2012 Track 2 raw CSV in
# data/raw/kddcup2012_track2/ into the logical staging dataset
# (click_prediction), written to data/staging/kddcup2012_track2/. Local
# file operations only -- does not upload anywhere. data/raw/ is
# read-only throughout.
#
# Already one self-contained CSV (399,483 rows, 12 columns, single
# consistent header) -- no splitting, no consolidation.
#
# Memory-safety design: read in 50,000-row chunks and written straight
# to disk; no frames list + pd.concat(). Output stays well under the
# 50 MiB chunking threshold (source is 27.5 MB), so it is written as
# one flat file -- chunking would be artificial here.

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_KDDCUP_DIR = DATA_RAW_DIR / "kddcup2012_track2"
STAGING_KDDCUP_DIR = DATA_STAGING_DIR / "kddcup2012_track2"
ANALYTICAL_DIR = STAGING_KDDCUP_DIR / "analytical"

SOURCE_FILE = RAW_KDDCUP_DIR / "Click_prediction_small.csv"

CHUNK_SIZE = 100_000


def stage_click_prediction(monitor: PeakRSSMonitor) -> tuple[int, list[str], Path, int]:
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Expected KDD Cup 2012 Track 2 source file missing: {SOURCE_FILE}"
        )

    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "click_prediction.csv"

    total_rows = 0
    first_write = True
    columns: list[str] = []

    for chunk in pd.read_csv(SOURCE_FILE, chunksize=CHUNK_SIZE):
        if not columns:
            columns = list(chunk.columns)
        chunk.to_csv(
            out_path,
            mode="w" if first_write else "a",
            header=first_write,
            index=False,
            encoding="utf-8",
        )
        first_write = False
        total_rows += len(chunk)
        del chunk
        monitor.check()

    logger.info(
        f"Staged analytical/click_prediction.csv -- {total_rows} rows, 1 source file"
    )
    return total_rows, columns, out_path, 1


def main() -> None:
    monitor = PeakRSSMonitor()

    total_rows, columns, out_path, files_read = stage_click_prediction(monitor)
    monitor.check()

    logger.info("KDD Cup 2012 Track 2 staging complete.")
    logger.info(
        f"  click_prediction: {total_rows} rows, {len(columns)} columns, "
        f"{files_read} source files -> {out_path}"
    )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )


if __name__ == "__main__":
    main()
