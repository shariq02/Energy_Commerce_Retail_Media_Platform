"""CDC pipeline -- consumer state store.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: a small SQLite store that makes the local consumer resumable and
idempotent -- it remembers the highest log position (LSN) already applied for
every (table, primary key) so replays and duplicates are dropped, and it holds
one processing watermark per table for late-event reporting.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS applied_position (
    table_name  TEXT NOT NULL,
    row_key     TEXT NOT NULL,
    last_lsn    INTEGER NOT NULL,
    last_op     TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (table_name, row_key)
);

CREATE TABLE IF NOT EXISTS watermark (
    table_name       TEXT PRIMARY KEY,
    max_event_ms     INTEGER NOT NULL,
    events_seen      INTEGER NOT NULL DEFAULT 0,
    events_applied   INTEGER NOT NULL DEFAULT 0,
    events_stale     INTEGER NOT NULL DEFAULT 0,
    events_late      INTEGER NOT NULL DEFAULT 0
);
"""


class ConsumerState:
    """Resumable dedup / ordering state for the CDC consumer."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self):
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def last_lsn(self, table: str, row_key: str) -> int | None:
        row = self._conn.execute(
            "SELECT last_lsn FROM applied_position WHERE table_name = ? AND row_key = ?",
            (table, row_key),
        ).fetchone()
        return None if row is None else int(row[0])

    def record_applied(
        self, table: str, row_key: str, lsn: int, op: str, event_iso: str
    ) -> None:
        self._conn.execute(
            "INSERT INTO applied_position (table_name, row_key, last_lsn, last_op, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(table_name, row_key) DO UPDATE SET "
            "last_lsn = excluded.last_lsn, last_op = excluded.last_op, "
            "updated_at = excluded.updated_at",
            (table, row_key, lsn, op, event_iso),
        )

    def bump_watermark(
        self,
        table: str,
        event_ms: int,
        *,
        applied: bool,
        stale: bool,
        late: bool,
    ) -> None:
        self._conn.execute(
            "INSERT INTO watermark (table_name, max_event_ms, events_seen, "
            "events_applied, events_stale, events_late) VALUES (?, ?, 1, ?, ?, ?) "
            "ON CONFLICT(table_name) DO UPDATE SET "
            "max_event_ms = MAX(max_event_ms, excluded.max_event_ms), "
            "events_seen = events_seen + 1, "
            "events_applied = events_applied + excluded.events_applied, "
            "events_stale = events_stale + excluded.events_stale, "
            "events_late = events_late + excluded.events_late",
            (table, event_ms, int(applied), int(stale), int(late)),
        )

    def watermark_ms(self, table: str) -> int:
        row = self._conn.execute(
            "SELECT max_event_ms FROM watermark WHERE table_name = ?", (table,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def summary(self) -> list[dict]:
        cur = self._conn.execute(
            "SELECT table_name, max_event_ms, events_seen, events_applied, "
            "events_stale, events_late FROM watermark ORDER BY table_name"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
