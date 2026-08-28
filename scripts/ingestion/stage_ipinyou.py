# ====================================================================
# iPinYou Phase 2b Staging
# Energy Commerce & Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
# ====================================================================
# Purpose: Consolidate iPinYou raw files in data/raw/ipinyou/ into the
# 3 logical staging datasets defined in PIPELINE_DESIGN.md Section 1a
# (training_data, leaderboard_data, reference_data), written to
# data/staging/ipinyou/. Local file operations only -- does not upload
# anywhere. data/raw/ is read-only throughout.
#
# Season 1 (training1st/testing1st) log rows carry fewer fields than
# Season 2/3 -- they do not carry AdvertiserID or UserTags. This is
# confirmed both by the README ("the second and third season data
# contains the user tags column while the first season data does not")
# and by direct column-count inspection (imp/clk/conv: 22 vs 24,
# leaderboard: 24 vs 26 -- a 2-column gap). Rows are reconciled onto one
# superset schema per logical dataset; Season 1 rows carry NULL in the
# columns that season never populated.
#
# The bid-request logs (training{1,2,3}/bid.*) are deliberately NOT
# staged: they are ~2/3 of the raw volume and redundant for this
# platform's analysis -- the impression log already carries every won
# auction with its paying price, and no use case needs raw RTB
# bid-landscape data. See PIPELINE_DESIGN.md Section 1a and CHANGELOG
# Entry 013. training_data therefore consists of impression / click /
# conversion events only.
# ====================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedCSVWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_IPINYOU_DIR = DATA_RAW_DIR / "ipinyou"
STAGING_IPINYOU_DIR = DATA_STAGING_DIR / "ipinyou"
ANALYTICAL_DIR = STAGING_IPINYOU_DIR / "analytical"
REFERENCE_DIR = STAGING_IPINYOU_DIR / "reference"

# Source logs are plain TSV with no header row, bzip2-compressed. Read
# in chunks -- the largest single source file (training2nd impression
# logs) decompresses far past what fits in memory on this machine at once.
# Capped at 50,000 rows/chunk under the Phase 2b 1 GB memory-safety design
# (scripts/ingestion/_memory_guard.py).
CHUNK_SIZE = 300_000

# ---- Canonical field lists, in on-disk column order -----------------

EVENT_FIELDS_S1 = [
    "bid_id", "timestamp", "log_type", "ipinyou_id", "user_agent", "ip",
    "region", "city", "ad_exchange", "domain", "url", "anonymous_url_id",
    "ad_slot_id", "ad_slot_width", "ad_slot_height", "ad_slot_visibility",
    "ad_slot_format", "ad_slot_floor_price", "creative_id", "bidding_price",
    "paying_price", "keypage_url",
]
EVENT_FIELDS_S23 = EVENT_FIELDS_S1 + ["advertiser_id", "user_tags"]

LEADERBOARD_FIELDS_S1 = EVENT_FIELDS_S1 + ["related_clicks_count", "has_conversion"]
LEADERBOARD_FIELDS_S23 = EVENT_FIELDS_S23 + ["related_clicks_count", "has_conversion"]

# Superset output schemas -- Season 1 rows get NULL in the columns their
# season's log format never carried (advertiser_id, user_tags). This is
# schema reconciliation, not missing data.
TRAINING_OUTPUT_COLUMNS = [
    "season", "event_type", "bid_id", "timestamp", "log_type",
    "ipinyou_id", "user_agent", "ip", "region", "city", "ad_exchange",
    "domain", "url", "anonymous_url_id", "ad_slot_id", "ad_slot_width",
    "ad_slot_height", "ad_slot_visibility", "ad_slot_format",
    "ad_slot_floor_price", "creative_id", "bidding_price", "paying_price",
    "keypage_url", "advertiser_id", "user_tags",
]
LEADERBOARD_OUTPUT_COLUMNS = [
    "season", "bid_id", "timestamp", "log_type", "ipinyou_id",
    "user_agent", "ip", "region", "city", "ad_exchange", "domain", "url",
    "anonymous_url_id", "ad_slot_id", "ad_slot_width", "ad_slot_height",
    "ad_slot_visibility", "ad_slot_format", "ad_slot_floor_price",
    "creative_id", "bidding_price", "paying_price", "keypage_url",
    "advertiser_id", "user_tags", "related_clicks_count", "has_conversion",
]

# Impression / click / conversion logs only -- bid-request logs are not
# staged (see module docstring). Season 1 uses the shorter field list;
# Season 2/3 add advertiser_id + user_tags.
TRAINING_SOURCES = [
    ("training1st", "clk", EVENT_FIELDS_S1),
    ("training1st", "conv", EVENT_FIELDS_S1),
    ("training1st", "imp", EVENT_FIELDS_S1),
    ("training2nd", "clk", EVENT_FIELDS_S23),
    ("training2nd", "conv", EVENT_FIELDS_S23),
    ("training2nd", "imp", EVENT_FIELDS_S23),
    ("training3rd", "clk", EVENT_FIELDS_S23),
    ("training3rd", "conv", EVENT_FIELDS_S23),
    ("training3rd", "imp", EVENT_FIELDS_S23),
]

LEADERBOARD_SOURCES = [
    ("testing1st", LEADERBOARD_FIELDS_S1),
    ("testing2nd", LEADERBOARD_FIELDS_S23),
    ("testing3rd", LEADERBOARD_FIELDS_S23),
]


def _remove_obsolete_flat_file(path: Path) -> None:
    """Pre-chunking runs of this script wrote one growing CSV per dataset
    directly under analytical/. Once the new chunked output for that
    dataset is fully written, the obsolete flat file (if any) is removed
    so it isn't left sitting beside the chunk directory as stale/ambiguous
    data."""
    if path.exists() and path.is_file():
        path.unlink()
        logger.info(f"Removed obsolete pre-chunking output: {path}")


def stage_training_data(monitor: PeakRSSMonitor) -> tuple[int, list[str], Path, int]:
    out_dir = ANALYTICAL_DIR / "training_data"
    writer = ChunkedCSVWriter(out_dir, source="ipinyou", dataset="training")
    files_read = 0

    for season, event_type, fields in TRAINING_SOURCES:
        season_dir = RAW_IPINYOU_DIR / season
        for src_file in sorted(season_dir.glob(f"{event_type}.*.txt.bz2")):
            files_read += 1
            for chunk in pd.read_csv(
                src_file, sep="\t", header=None, names=fields,
                compression="bz2", chunksize=CHUNK_SIZE,
            ):
                chunk.insert(0, "event_type", event_type)
                chunk.insert(0, "season", season)
                chunk = chunk.reindex(columns=TRAINING_OUTPUT_COLUMNS)
                writer.write(chunk)
                del chunk
                monitor.check()

    writer.close()
    _remove_obsolete_flat_file(ANALYTICAL_DIR / "training_data.csv")
    logger.info(f"Staged training_data/ -- {writer.total_rows} rows, {files_read} source files, "
                f"{len(writer.chunk_paths)} chunks")
    return writer.total_rows, TRAINING_OUTPUT_COLUMNS, out_dir, files_read


def stage_leaderboard_data(monitor: PeakRSSMonitor) -> tuple[int, list[str], Path, int]:
    out_dir = ANALYTICAL_DIR / "leaderboard_data"
    writer = ChunkedCSVWriter(out_dir, source="ipinyou", dataset="leaderboard")
    files_read = 0

    for season, fields in LEADERBOARD_SOURCES:
        season_dir = RAW_IPINYOU_DIR / season
        for src_file in sorted(season_dir.glob("leaderboard.test.data.*.txt.bz2")):
            files_read += 1
            for chunk in pd.read_csv(
                src_file, sep="\t", header=None, names=fields,
                compression="bz2", chunksize=CHUNK_SIZE,
            ):
                chunk.insert(0, "season", season)
                chunk = chunk.reindex(columns=LEADERBOARD_OUTPUT_COLUMNS)
                writer.write(chunk)
                del chunk
                monitor.check()

    writer.close()
    _remove_obsolete_flat_file(ANALYTICAL_DIR / "leaderboard_data.csv")
    logger.info(f"Staged leaderboard_data/ -- {writer.total_rows} rows, {files_read} source files, "
                f"{len(writer.chunk_paths)} chunks")
    return writer.total_rows, LEADERBOARD_OUTPUT_COLUMNS, out_dir, files_read


def _read_id_name_file(path: Path) -> pd.DataFrame:
    """City/region/user-profile-tag lookup files are 'id<whitespace>name'
    per line, CRLF-terminated. Almost all rows are tab-separated, but the
    id-0 row in city/region files uses a literal space instead of a tab
    -- split on any whitespace run to handle both consistently."""
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                rows.append((int(parts[0]), parts[1]))
    return pd.DataFrame(rows, columns=["id", "name"])


def _stage_lookup_pair(name: str, en_file: str, cn_file: str, id_col: str) -> tuple[int, list[str], Path]:
    en_df = _read_id_name_file(RAW_IPINYOU_DIR / en_file).rename(columns={"id": id_col, "name": "name_en"})
    cn_df = _read_id_name_file(RAW_IPINYOU_DIR / cn_file).rename(columns={"id": id_col, "name": "name_cn"})
    combined = en_df.merge(cn_df, on=id_col, how="outer").sort_values(id_col).reset_index(drop=True)

    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REFERENCE_DIR / f"{name}.csv"
    combined.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"Staged reference/{name}.csv -- {len(combined)} rows")
    return len(combined), list(combined.columns), out_path


def stage_reference_data() -> dict:
    results = {}
    rows, columns, path = _stage_lookup_pair("city", "city.en.txt", "city.cn.txt", "city_id")
    results["city"] = (rows, columns, path)

    rows, columns, path = _stage_lookup_pair("region", "region.en.txt", "region.cn.txt", "region_id")
    results["region"] = (rows, columns, path)

    rows, columns, path = _stage_lookup_pair(
        "user_profile_tags", "user.profile.tags.en.txt", "user.profile.tags.cn.txt", "tag_id",
    )
    results["user_profile_tags"] = (rows, columns, path)
    return results


def main() -> None:
    monitor = PeakRSSMonitor()

    training_rows, training_columns, training_path, training_files = stage_training_data(monitor)
    leaderboard_rows, leaderboard_columns, leaderboard_path, leaderboard_files = stage_leaderboard_data(monitor)
    reference_results = stage_reference_data()
    monitor.check()

    logger.info("iPinYou Phase 2b staging complete.")
    logger.info(f"  training_data: {training_rows} rows, {len(training_columns)} columns, "
                f"{training_files} source files -> {training_path}")
    logger.info(f"  leaderboard_data: {leaderboard_rows} rows, {len(leaderboard_columns)} columns, "
                f"{leaderboard_files} source files -> {leaderboard_path}")
    for name, (rows, columns, path) in reference_results.items():
        logger.info(f"  reference/{name}: {rows} rows, {len(columns)} columns -> {path}")
    logger.info(f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
                f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)")


if __name__ == "__main__":
    main()
