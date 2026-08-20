# ====================================================================
# SMARD Phase 2b Staging
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Consolidate the 16 SMARD raw JSON files in data/raw/smard/
# into the single logical staging dataset defined in PIPELINE_DESIGN.md
# Section 1a (energy_timeseries), written to data/staging/smard/. Local
# file operations only -- does not upload anywhere. data/raw/ is
# read-only throughout.
#
# All 16 files share an identical structure (filter_id, region,
# resolution, series) and the same region/resolution -- confirmed via
# logs/inspections/smard_final_mapping_inspection.txt. The 16 files
# become metric identity within one dataset, not 16 tables. Metric
# identity is currently encoded only in the filename, so it is added
# as an explicit `metric` column.
#
# Memory-safety design: each file's `series` array is small (source
# directory totals ~1.5 MB) so a single file is loaded and converted to
# rows in one pass, then released before the next file is read. No
# frames list + pd.concat() across files -- each file's rows are
# written straight to disk and the in-memory DataFrame is discarded.
# The combined output stays well under the 50 MiB chunking threshold,
# so it is written as one flat file (chunking would be artificial here).
# ====================================================================

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _memory_guard import PeakRSSMonitor
from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_SMARD_DIR = DATA_RAW_DIR / "smard"
STAGING_SMARD_DIR = DATA_STAGING_DIR / "smard"
ANALYTICAL_DIR = STAGING_SMARD_DIR / "analytical"

OUTPUT_COLUMNS = ["metric", "filter_id", "region", "resolution", "timestamp_utc", "value"]


def _rows_for_file(path: Path) -> tuple[pd.DataFrame, int]:
    metric = path.stem
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    filter_id = payload["filter_id"]
    region = payload["region"]
    resolution = payload["resolution"]
    series = payload["series"]

    timestamps = [datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat() for ts_ms, _ in series]
    values = [value for _, value in series]

    df = pd.DataFrame({
        "metric": metric,
        "filter_id": filter_id,
        "region": region,
        "resolution": resolution,
        "timestamp_utc": timestamps,
        "value": values,
    })
    df = df.reindex(columns=OUTPUT_COLUMNS)
    return df, len(series)


def stage_energy_timeseries(monitor: PeakRSSMonitor) -> tuple[int, Path, int]:
    ANALYTICAL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYTICAL_DIR / "energy_timeseries.csv"

    total_rows = 0
    files_read = 0
    first_write = True

    for src_file in sorted(RAW_SMARD_DIR.glob("*.json")):
        files_read += 1
        df, row_count = _rows_for_file(src_file)
        df.to_csv(out_path, mode="w" if first_write else "a", header=first_write,
                   index=False, encoding="utf-8")
        first_write = False
        total_rows += row_count
        del df
        monitor.check()

    if first_write:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Staged analytical/energy_timeseries.csv -- {total_rows} rows, {files_read} source files")
    return total_rows, out_path, files_read


def main() -> None:
    monitor = PeakRSSMonitor()

    total_rows, out_path, files_read = stage_energy_timeseries(monitor)
    monitor.check()

    logger.info("SMARD Phase 2b staging complete.")
    logger.info(f"  energy_timeseries: {total_rows} rows, {len(OUTPUT_COLUMNS)} columns, "
                f"{files_read} source files -> {out_path}")
    logger.info(f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
                f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)")


if __name__ == "__main__":
    main()
