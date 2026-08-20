# ====================================================================
# Criteo Attribution Phase 2b Staging
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Stage the single Criteo Attribution raw TSV in
# data/raw/criteo_attribution/ into the logical staging dataset defined
# in PIPELINE_DESIGN.md Section 1a (attribution_events), written to
# data/staging/criteo_attribution/attribution_events/. Local file
# operations only -- does not upload anywhere. data/raw/ is read-only
# throughout.
#
# Already one self-contained TSV (16,468,028 rows, 22 columns) -- no
# splitting, no consolidation, all columns passed through unchanged.
# At ~2.47 GB it requires lossless physical chunking for upload
# practicality (PIPELINE_DESIGN.md Section 1b); chunking never creates
# a second logical dataset -- all chunks stay one Volume upload unit.
#
# Memory-safety design: read in 50,000-row chunks directly from the
# source TSV and handed straight to a ChunkedCSVWriter, which writes
# and closes each physical chunk immediately; no frames list +
# pd.concat().
# ====================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor
from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_CRITEO_DIR = DATA_RAW_DIR / "criteo_attribution"
STAGING_CRITEO_DIR = DATA_STAGING_DIR / "criteo_attribution"

SOURCE_FILE = RAW_CRITEO_DIR / "pcb_dataset_final.tsv"

CHUNK_SIZE = 100_000


def stage_attribution_events(monitor: PeakRSSMonitor) -> tuple[int, list[str], Path, int, int]:
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Expected Criteo Attribution source file missing: {SOURCE_FILE}")

    out_dir = STAGING_CRITEO_DIR / "attribution_events"
    writer = ChunkedCSVWriter(out_dir, source="criteo", dataset="attribution", extension="tsv", sep="\t")

    columns: list[str] = []
    for chunk in pd.read_csv(SOURCE_FILE, sep="\t", chunksize=CHUNK_SIZE):
        if not columns:
            columns = list(chunk.columns)
        writer.write(chunk)
        del chunk
        monitor.check()

    writer.close()
    logger.info(f"Staged attribution_events/ -- {writer.total_rows} rows, 1 source file, "
                f"{len(writer.chunk_paths)} chunks")
    return writer.total_rows, columns, out_dir, 1, len(writer.chunk_paths)


def main() -> None:
    monitor = PeakRSSMonitor()

    total_rows, columns, out_dir, files_read, chunk_count = stage_attribution_events(monitor)
    monitor.check()

    logger.info("Criteo Attribution Phase 2b staging complete.")
    logger.info(f"  attribution_events: {total_rows} rows, {len(columns)} columns, "
                f"{files_read} source files, {chunk_count} chunks -> {out_dir}")
    logger.info(f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
                f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)")


if __name__ == "__main__":
    main()
