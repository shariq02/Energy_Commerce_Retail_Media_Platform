"""CDC pipeline -- local landing writer.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: append accepted change events to per-table JSON Lines files under
data/cdc/landing/<table>/. Each line is one event carrying the full before/after
images plus the CDC metadata (op, LSN, source and event timestamps, primary
key) that the Databricks batch step needs to build the history and current-state
tables.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from src.ingestion.cdc.debezium import ChangeEvent

# Column names for the metadata Debezium/PostgreSQL provide, kept stable so the
# Databricks schema can rely on them.
META_COLUMNS = (
    "_table",
    "_op",
    "_op_name",
    "_row_key",
    "_lsn",
    "_event_ms",
    "_event_ts",
    "_source_ts_ms",
    "_ingested_ts",
    "_deleted",
)


class LandingWriter:
    """One JSONL file per table per run; appends, never rewrites."""

    def __init__(self, base_dir: Path, run_id: str) -> None:
        self._base = base_dir
        self._run_id = run_id
        self._handles: dict[str, object] = {}
        self._counts: dict[str, int] = {}

    def _handle(self, table: str):
        if table not in self._handles:
            target = self._base / table
            target.mkdir(parents=True, exist_ok=True)
            path = target / f"{table}__{self._run_id}.jsonl"
            self._handles[table] = path.open("a", encoding="utf-8")
            self._counts.setdefault(table, 0)
        return self._handles[table]

    def write(self, event: ChangeEvent) -> None:
        record = {
            "_table": event.table,
            "_op": event.op,
            "_op_name": event.op_name,
            "_row_key": event.row_key,
            "_lsn": event.lsn,
            "_event_ms": event.event_ms,
            "_event_ts": event.event_iso,
            "_source_ts_ms": event.source.get("ts_ms"),
            "_ingested_ts": datetime.now(tz=UTC).isoformat(),
            "_deleted": event.is_delete,
            "before": event.before,
            "after": event.after,
        }
        handle = self._handle(event.table)
        handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True))
        handle.write("\n")
        self._counts[event.table] = self._counts.get(event.table, 0) + 1

    def flush(self) -> None:
        for handle in self._handles.values():
            handle.flush()

    def close(self) -> dict[str, int]:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()
        return dict(self._counts)
