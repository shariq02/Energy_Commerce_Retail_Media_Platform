"""CDC pipeline -- source PostgreSQL lifetime identity.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: derive a stable token for the current source database lifetime. The
consumer stores this token alongside its dedup state; when it changes -- a
restored, re-initialised or otherwise rebuilt source whose write-ahead log
positions no longer continue the old sequence -- the consumer knows its recorded
log positions are from a previous lifetime and must not be used to judge new
events as already-seen. Pure read; no schema, no writes.
"""

from __future__ import annotations

import psycopg2

from src.ingestion.cdc import config


class SourceUnavailable(RuntimeError):
    """The source database could not be reached to confirm its identity."""


def current_epoch() -> str:
    """A token that changes only when the source database lifetime changes.

    ``system_identifier`` is assigned by ``initdb`` and preserved by physical
    replication / base backups, so it is stable across restarts and failover but
    distinct for a freshly initialised or differently-restored cluster -- exactly
    the case where log-sequence numbers restart lower and stale dedup state would
    wrongly suppress valid events.
    """
    try:
        conn = psycopg2.connect(connect_timeout=10, **config.POSTGRES)
    except psycopg2.Error as exc:  # pragma: no cover - network failure path
        raise SourceUnavailable(
            f"cannot reach the source database to confirm its identity: {exc}"
        ) from exc
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT system_identifier::text FROM pg_control_system()")
            system_identifier = cur.fetchone()[0]
    finally:
        conn.close()
    return f"pg-system-{system_identifier}"
