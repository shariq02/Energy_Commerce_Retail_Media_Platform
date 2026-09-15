# GA4 staging
# ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
# Author: Sharique Mohammad
# Date: September 2026
#
# Purpose: filter and project data/raw/ga4/*.jsonl (92 daily files, one row
# per event) down to the retained event/field allowlists below, written to
# data/staging/ga4/events/. Read-only against data/raw/, no upload.
#
# `refund` is never emitted -- it has zero observed rows in this archive.
# One raw event produces at most one staged record. geo.country is
# flattened to geo_country.

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from _chunk_writer import ChunkedJSONLWriter
from _memory_guard import PeakRSSMonitor

from config import DATA_RAW_DIR, DATA_STAGING_DIR, get_logger

logger = get_logger(__name__)

RAW_GA4_DIR = DATA_RAW_DIR / "ga4"
STAGING_GA4_DIR = DATA_STAGING_DIR / "ga4"

BATCH_SIZE = 50_000

# Retained funnel events -- page_view/user_engagement/scroll/session_start/
# first_visit/click carry no item/ecommerce signal, excluded.
RETAINED_EVENTS = frozenset(
    {
        "view_item",
        "view_item_list",
        "select_item",
        "view_promotion",
        "select_promotion",
        "view_search_results",
        "add_to_cart",
        "begin_checkout",
        "add_shipping_info",
        "add_payment_info",
        "purchase",
    }
)

# Retained event_params keys -- the rest (debug/engagement flags,
# marketing attribution, outbound-link tracking) is dropped.
RETAINED_PARAM_KEYS = frozenset(
    {
        "ga_session_id",
        "ga_session_number",
        "page_location",
        "page_title",
        "search_term",
        "unique_search_term",
    }
)

RETAINED_ECOMMERCE_FIELDS = (
    "transaction_id",
    "purchase_revenue",
    "unique_items",
    "total_item_quantity",
)

RETAINED_ITEM_FIELDS = (
    "item_id",
    "item_name",
    "item_category",
    "price",
    "quantity",
    "item_revenue",
)

ALLOWED_TOP_LEVEL_FIELDS = frozenset(
    {
        "event_date",
        "event_timestamp",
        "event_name",
        "user_pseudo_id",
        "event_params",
        "ecommerce",
        "items",
        "geo_country",
    }
)

EXPECTED_RETAINED_ROWS = 779_485  # from the prior full-archive measurement pass


def project_event(row: dict) -> dict | None:
    """Filter + project one raw GA4 event row. Returns None for any
    event_name outside RETAINED_EVENTS -- the row is dropped, not
    transformed."""
    event_name = row.get("event_name")
    if event_name not in RETAINED_EVENTS:
        return None

    projected = {
        "event_date": row.get("event_date"),
        "event_timestamp": row.get("event_timestamp"),
        "event_name": event_name,
        "user_pseudo_id": row.get("user_pseudo_id"),
    }

    kept_params = [
        p
        for p in (row.get("event_params") or [])
        if p.get("key") in RETAINED_PARAM_KEYS
    ]
    if kept_params:
        projected["event_params"] = kept_params

    ecommerce = row.get("ecommerce") or {}
    kept_ecommerce = {
        field: ecommerce[field]
        for field in RETAINED_ECOMMERCE_FIELDS
        if ecommerce.get(field) is not None
    }
    if kept_ecommerce:
        projected["ecommerce"] = kept_ecommerce

    kept_items = []
    for item in row.get("items") or []:
        kept_item = {
            field: item[field]
            for field in RETAINED_ITEM_FIELDS
            if item.get(field) is not None
        }
        if kept_item:
            kept_items.append(kept_item)
    if kept_items:
        projected["items"] = kept_items

    geo = row.get("geo") or {}
    country = geo.get("country")
    if country is not None:
        projected["geo_country"] = country

    return projected


def stage_events(monitor: PeakRSSMonitor) -> tuple[int, int, int, Path, int, int]:
    source_files = sorted(RAW_GA4_DIR.glob("events_*.jsonl"))
    if not source_files:
        raise FileNotFoundError(f"No GA4 raw files found under {RAW_GA4_DIR}")

    out_dir = STAGING_GA4_DIR / "events"
    writer = ChunkedJSONLWriter(out_dir, source="ga4", dataset="events")

    lines_read = 0
    malformed_lines = 0
    retained_rows = 0
    batch: list[dict] = []

    for src_file in source_files:
        with open(src_file, encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                lines_read += 1
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    malformed_lines += 1
                    logger.warning(
                        f"{src_file.name}: malformed JSON line, skipped ({exc})"
                    )
                    continue

                projected = project_event(row)
                if projected is None:
                    continue
                batch.append(projected)
                retained_rows += 1

                if len(batch) >= BATCH_SIZE:
                    writer.write(batch)
                    batch = []
                    monitor.check()

        logger.info(
            f"Staged {src_file.name} -- {lines_read} lines read so far, "
            f"{retained_rows} retained so far"
        )

    if batch:
        writer.write(batch)
        monitor.check()

    writer.close()
    return (
        lines_read,
        malformed_lines,
        retained_rows,
        out_dir,
        len(writer.chunk_paths),
        len(source_files),
    )


def validate_staged_output(out_dir: Path, expected_rows: int) -> None:
    """Read back the (much smaller) staged output itself -- never the raw
    archive -- and confirm the retained event/field allowlists actually
    held throughout."""
    seen_events: set[str] = set()
    seen_param_keys: set[str] = set()
    seen_top_level_keys: set[str] = set()
    seen_item_keys: set[str] = set()
    row_count = 0
    total_bytes = 0

    chunk_paths = sorted(out_dir.glob("*.jsonl"))
    for chunk_path in chunk_paths:
        total_bytes += chunk_path.stat().st_size
        with open(chunk_path, encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                row_count += 1
                record = json.loads(line)
                seen_top_level_keys.update(record.keys())
                seen_events.add(record.get("event_name"))
                for p in record.get("event_params", []):
                    seen_param_keys.add(p.get("key"))
                for it in record.get("items", []):
                    seen_item_keys.update(it.keys())

    unapproved_events = seen_events - RETAINED_EVENTS
    unapproved_top_level = seen_top_level_keys - ALLOWED_TOP_LEVEL_FIELDS
    unapproved_params = seen_param_keys - RETAINED_PARAM_KEYS
    unapproved_items = seen_item_keys - set(RETAINED_ITEM_FIELDS)

    logger.info("=== Staged output validation ===")
    logger.info(f"Chunks read back: {len(chunk_paths)}")
    logger.info(
        f"Staged rows: {row_count} (previously measured expected count: {expected_rows})"
    )
    if row_count != expected_rows:
        logger.warning(
            f"Staged row count ({row_count}) differs from the previously measured "
            f"expected count ({expected_rows}) -- the raw archive may have changed "
            "since that measurement; re-check before treating this as an error."
        )
    logger.info(f"Distinct event_name values present: {sorted(seen_events)}")
    if "refund" in seen_events:
        logger.warning(
            "'refund' present in staged output -- should never occur, investigate."
        )
    if unapproved_events:
        logger.warning(f"Unapproved event_name values present: {unapproved_events}")
    if unapproved_top_level:
        logger.warning(f"Unapproved top-level fields present: {unapproved_top_level}")
    if unapproved_params:
        logger.warning(f"Unapproved event_params keys present: {unapproved_params}")
    if unapproved_items:
        logger.warning(f"Unapproved item fields present: {unapproved_items}")
    if not (
        unapproved_events
        or unapproved_top_level
        or unapproved_params
        or unapproved_items
        or "refund" in seen_events
    ):
        logger.info("All retained fields/events are within the approved allowlists.")
    logger.info(
        f"Total staged output size: {total_bytes} bytes ({total_bytes / 1e9:.4f} GB)"
    )


def main() -> None:
    monitor = PeakRSSMonitor()

    lines_read, malformed_lines, retained_rows, out_dir, chunk_count, files_read = (
        stage_events(monitor)
    )
    monitor.check()

    logger.info("GA4 staging complete.")
    logger.info(
        f"  events: {retained_rows} rows retained of {lines_read} read "
        f"({100 * retained_rows / lines_read:.2f}% kept), "
        f"{malformed_lines} malformed lines skipped, "
        f"{files_read} source files, {chunk_count} chunks -> {out_dir}"
    )
    logger.info(
        f"Peak RSS observed: {monitor.peak_rss_mb:.1f} MB "
        f"(safety threshold {monitor.safety_threshold_bytes / 1024 / 1024:.0f} MB)"
    )

    validate_staged_output(out_dir, expected_rows=EXPECTED_RETAINED_ROWS)


if __name__ == "__main__":
    main()
