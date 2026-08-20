# ====================================================================
# Honda IoT Phase 2b Staging
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Consolidate Honda IoT raw files in data/raw/honda_iot/ into
# the 7 logical staging datasets defined in PIPELINE_DESIGN.md Section
# 1a, written to data/staging/honda_iot/analytical/. Local file
# operations only -- does not upload anywhere. data/raw/ is read-only
# throughout.
#
# The 1min/15min/1h folders are frequency partitions of the same 7
# datasets, not 21 separate ones -- confirmed schema-identical per
# metric across all three frequencies (logs/inspections/
# honda_iot_phase2b_inspection.txt). Frequency is currently encoded
# only in the folder name, so it is added as an explicit `frequency`
# column.
#
# Known data-quality finding (documented in the inspection report):
# the `weather` dataset's column order is inconsistent across
# frequency files -- 2 of 3 read datetime_utc, Ta, Igm while one reads
# the same two fields reversed. Every source file carries its own CSV
# header row, so pandas already aligns columns by name on read (no
# header=None / positional reads anywhere in this script); each
# DataFrame is additionally reindexed onto its dataset's canonical
# column order by name before being written, so a column-order
# difference between source files can never silently swap field
# values in the staged output.
#
# Memory-safety design: each frequency file is read gzip-compressed in
# 50,000-row chunks and handed straight to a ChunkedCSVWriter -- no
# frames list + pd.concat() across files or frequencies. The largest
# per-dataset consolidated output (electricity_P/W, weather; ~3.4M rows
# across all 3 frequencies) exceeds the 50 MiB physical chunk limit, so
# every dataset uses the chunked writer for consistency, even where a
# smaller dataset happens to end up as a single chunk.
# ====================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor
from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_HONDA_DIR = DATA_RAW_DIR / "honda_iot"
STAGING_HONDA_DIR = DATA_STAGING_DIR / "honda_iot"
ANALYTICAL_DIR = STAGING_HONDA_DIR / "analytical"

CHUNK_SIZE = 100_000

FREQUENCIES = ["1min", "15min", "1h"]

# Canonical output column order per dataset, by name -- source column
# order is never trusted positionally (see weather note above).
DATASET_COLUMNS = {
    "electricity_P": ["frequency", "datetime_utc", "total", "PV", "CHP"],
    "electricity_W": ["frequency", "datetime_utc", "total", "PV", "CHP"],
    "heating_P": ["frequency", "datetime_utc", "total", "CHP_heat", "CHP_elec"],
    "heating_W": ["frequency", "datetime_utc", "total", "CHP_heat", "CHP_elec"],
    "cooling_P": ["frequency", "datetime_utc", "total", "cool_elec"],
    "cooling_W": ["frequency", "datetime_utc", "total", "cool_elec"],
    "weather": ["frequency", "datetime_utc", "WeatherStation.Weather.Ta", "WeatherStation.Weather.Igm"],
}

DATASETS = list(DATASET_COLUMNS.keys())


def stage_dataset(dataset: str, monitor: PeakRSSMonitor) -> tuple[int, Path, int, int]:
    out_dir = ANALYTICAL_DIR / dataset
    writer = ChunkedCSVWriter(out_dir, source="honda_iot", dataset=dataset)
    columns = DATASET_COLUMNS[dataset]
    files_read = 0

    for frequency in FREQUENCIES:
        src_file = RAW_HONDA_DIR / frequency / f"{dataset}.csv.gz"
        if not src_file.exists():
            raise FileNotFoundError(f"Expected Honda IoT source file missing: {src_file}")
        files_read += 1

        for chunk in pd.read_csv(src_file, compression="gzip", chunksize=CHUNK_SIZE):
            chunk.insert(0, "frequency", frequency)
            chunk = chunk.reindex(columns=columns)
            writer.write(chunk)
            del chunk
            monitor.check()

    writer.close()
    logger.info(f"Staged analytical/{dataset}/ -- {writer.total_rows} rows, {files_read} source files, "
                f"{len(writer.chunk_paths)} chunks")
    return writer.total_rows, out_dir, files_read, len(writer.chunk_paths)


def main() -> None:
    monitor = PeakRSSMonitor()
    results = {}

    for dataset in DATASETS:
        rows, out_dir, files_read, chunk_count = stage_dataset(dataset, monitor)
        results[dataset] = (rows, out_dir, files_read, chunk_count)

    logger.info("Honda IoT Phase 2b staging complete.")
    for dataset, (rows, out_dir, files_read, chunk_count) in results.items():
        logger.info(f"  {dataset}: {rows} rows, {files_read} source files, "
                    f"{chunk_count} chunks -> {out_dir}")
    logger.info(f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
                f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)")


if __name__ == "__main__":
    main()
