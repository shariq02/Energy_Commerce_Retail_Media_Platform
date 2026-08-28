# ====================================================================
# Search Visibility Phase 2b Staging
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Consolidate Search Visibility raw files in
# data/raw/search_visibility/ into the 2 logical staging datasets
# defined in PIPELINE_DESIGN.md Section 1a (search_visibility_events,
# repository_info), written to data/staging/search_visibility/. Local
# file operations only -- does not upload anywhere. data/raw/ is
# read-only throughout.
#
# The 12 monthly .zip archives are monthly partitions of one event
# dataset, not 12 tables -- confirmed identical 11-column header across
# all 12 (logs/inspections/search_visibility_final_mapping_check.txt),
# including repository_id as an in-row join key to the separate
# repository_info reference dataset. README.txt remains documentation,
# not data, and is not staged.
#
# Month column decision (FINAL, per PIPELINE_DESIGN.md): no separate
# `month` column is added here -- the existing in-row `date` field
# already carries this information, and monthly archive provenance is
# preserved through `date` rather than a synthetic column. February
# 2017's row-count ceiling (1,048,575 rows) is a source condition and
# is preserved as-is, not repaired.
#
# Archive extraction: each .zip holds exactly one CSV. Archives are
# extracted by streaming the CSV entry directly out of the .zip via
# zipfile + pandas chunked reads -- no intermediate extracted file is
# ever written to data/raw/ or elsewhere on disk.
#
# Memory-safety design: each monthly CSV entry is streamed in
# 50,000-row chunks straight into a ChunkedCSVWriter, which writes and
# closes each physical chunk immediately; no frames list +
# pd.concat(). The reference file is small (<1 MB) and is read/written
# in one pass.
# ====================================================================

import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_SEARCH_VISIBILITY_DIR = DATA_RAW_DIR / "search_visibility"
STAGING_SEARCH_VISIBILITY_DIR = DATA_STAGING_DIR / "search_visibility"
EVENTS_DIR = STAGING_SEARCH_VISIBILITY_DIR / "events"
REFERENCE_DIR = STAGING_SEARCH_VISIBILITY_DIR / "reference"

REPOSITORY_INFO_FILE = RAW_SEARCH_VISIBILITY_DIR / "RAMP_repository_info.csv"

CHUNK_SIZE = 100_000

EVENT_COLUMNS = [
    "citableContent",
    "clickThrough",
    "clicks",
    "country",
    "date",
    "device",
    "impressions",
    "index",
    "position",
    "url",
    "repository_id",
]


def stage_events(monitor: PeakRSSMonitor) -> tuple[int, Path, int, int]:
    writer = ChunkedCSVWriter(EVENTS_DIR, source="search_visibility", dataset="events")
    files_read = 0

    for zip_path in sorted(RAW_SEARCH_VISIBILITY_DIR.glob("*.zip")):
        files_read += 1
        with zipfile.ZipFile(zip_path) as zf:
            entries = zf.namelist()
            if len(entries) != 1:
                raise ValueError(
                    f"Expected exactly one entry in {zip_path}, found {len(entries)}: {entries}"
                )
            with zf.open(entries[0]) as f:
                for chunk in pd.read_csv(f, chunksize=CHUNK_SIZE):
                    chunk = chunk.reindex(columns=EVENT_COLUMNS)
                    writer.write(chunk)
                    del chunk
                    monitor.check()

    writer.close()
    logger.info(
        f"Staged events/ -- {writer.total_rows} rows, {files_read} source files, "
        f"{len(writer.chunk_paths)} chunks"
    )
    return writer.total_rows, EVENTS_DIR, files_read, len(writer.chunk_paths)


def stage_repository_info() -> tuple[int, list[str], Path]:
    if not REPOSITORY_INFO_FILE.exists():
        raise FileNotFoundError(
            f"Expected Search Visibility reference file missing: {REPOSITORY_INFO_FILE}"
        )

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REFERENCE_DIR / "repository_info.csv"

    df = pd.read_csv(REPOSITORY_INFO_FILE)
    df.to_csv(out_path, index=False, encoding="utf-8")
    rows, columns = len(df), list(df.columns)
    del df

    logger.info(f"Staged reference/repository_info.csv -- {rows} rows, 1 source file")
    return rows, columns, out_path


def main() -> None:
    monitor = PeakRSSMonitor()

    event_rows, events_dir, event_files, chunk_count = stage_events(monitor)
    ref_rows, ref_columns, ref_path = stage_repository_info()
    monitor.check()

    logger.info("Search Visibility Phase 2b staging complete.")
    logger.info(
        f"  events: {event_rows} rows, {len(EVENT_COLUMNS)} columns, "
        f"{event_files} source files, {chunk_count} chunks -> {events_dir}"
    )
    logger.info(
        f"  reference/repository_info: {ref_rows} rows, {len(ref_columns)} columns -> {ref_path}"
    )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )


if __name__ == "__main__":
    main()
