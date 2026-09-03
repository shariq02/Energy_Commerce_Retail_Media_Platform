# REES46 staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Purpose: consolidate the REES46 raw CSVs in data/raw/rees46/ into the
# single logical staging dataset (events), written to
# data/staging/rees46/events/. Local file operations only -- does not
# upload anywhere. data/raw/ is read-only throughout.
#
# 2019-Oct.csv (42,448,764 rows) and 2019-Nov.csv (67,501,979 rows)
# share an identical 9-column header, and are monthly
# partitions of one events dataset; view/cart/purchase event types
# remain together, not split into separate datasets. The existing
# in-row event_time field already carries month provenance, so no
# synthetic column is added.
#
# At ~14 GB combined, this requires lossless physical chunking for
# upload practicality; chunking never creates a second logical dataset
# -- all chunks stay one Volume upload unit.
#
# Memory-safety design: each source file is read in 50,000-row chunks
# and handed straight to a ChunkedCSVWriter, which writes and closes
# each physical chunk immediately; no frames list + pd.concat().

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_REES46_DIR = DATA_RAW_DIR / "rees46"
STAGING_REES46_DIR = DATA_STAGING_DIR / "rees46"

CHUNK_SIZE = 300_000

SOURCE_FILES = ["2019-Oct.csv", "2019-Nov.csv"]

EVENT_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


def stage_events(monitor: PeakRSSMonitor) -> tuple[int, Path, int, int]:
    out_dir = STAGING_REES46_DIR / "events"
    writer = ChunkedCSVWriter(out_dir, source="rees46", dataset="events")
    files_read = 0

    for filename in SOURCE_FILES:
        src_file = RAW_REES46_DIR / filename
        if not src_file.exists():
            raise FileNotFoundError(f"Expected REES46 source file missing: {src_file}")
        files_read += 1

        for chunk in pd.read_csv(src_file, chunksize=CHUNK_SIZE):
            chunk = chunk.reindex(columns=EVENT_COLUMNS)
            writer.write(chunk)
            del chunk
            monitor.check()

    writer.close()
    logger.info(
        f"Staged events/ -- {writer.total_rows} rows, {files_read} source files, "
        f"{len(writer.chunk_paths)} chunks"
    )
    return writer.total_rows, out_dir, files_read, len(writer.chunk_paths)


def main() -> None:
    monitor = PeakRSSMonitor()

    total_rows, out_dir, files_read, chunk_count = stage_events(monitor)
    monitor.check()

    logger.info("REES46 staging complete.")
    logger.info(
        f"  events: {total_rows} rows, {len(EVENT_COLUMNS)} columns, "
        f"{files_read} source files, {chunk_count} chunks -> {out_dir}"
    )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )


if __name__ == "__main__":
    main()
