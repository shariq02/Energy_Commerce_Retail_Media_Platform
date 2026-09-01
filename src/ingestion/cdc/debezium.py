"""CDC pipeline -- Debezium envelope parsing.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: turn a raw Debezium PostgreSQL change message (key + value JSON, with
or without the converter's schema wrapper) into a flat, typed record the
consumer can dedup, order, and land. Pure functions -- no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

# Debezium op codes: create, read (snapshot), update, delete.
_OP_NAMES = {"c": "create", "r": "read", "u": "update", "d": "delete"}


class MalformedEvent(ValueError):
    """A message that does not look like a Debezium change event."""


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: str
    row_key: str
    lsn: int
    event_ms: int
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: dict[str, Any] = field(default_factory=dict)

    @property
    def is_delete(self) -> bool:
        return self.op == "d"

    @property
    def op_name(self) -> str:
        return _OP_NAMES.get(self.op, self.op)

    @property
    def event_iso(self) -> str:
        return datetime.fromtimestamp(self.event_ms / 1000, tz=UTC).isoformat()

    @property
    def state_row(self) -> dict[str, Any] | None:
        """The row image that represents current state after this event."""
        return None if self.is_delete else self.after


def _unwrap(obj: Any) -> Any:
    """Return the payload, whether or not the converter added a schema wrapper."""
    if isinstance(obj, dict) and "payload" in obj and "schema" in obj:
        return obj["payload"]
    return obj


def row_key_from_key(key_payload: Any, pk_column: str) -> str | None:
    payload = _unwrap(key_payload)
    if isinstance(payload, dict):
        if pk_column in payload:
            return str(payload[pk_column])
        if len(payload) == 1:
            return str(next(iter(payload.values())))
    return None


def parse_value(
    value_payload: Any,
    *,
    table: str,
    pk_column: str,
    key_payload: Any = None,
) -> ChangeEvent:
    payload = _unwrap(value_payload)
    if not isinstance(payload, dict) or "op" not in payload:
        raise MalformedEvent(f"{table}: value is not a Debezium envelope")

    op = payload["op"]
    if op not in _OP_NAMES:
        raise MalformedEvent(f"{table}: unknown op {op!r}")

    before = payload.get("before")
    after = payload.get("after")
    source = payload.get("source") or {}

    image = after if after is not None else before
    row_key = None
    if isinstance(image, dict) and pk_column in image:
        row_key = str(image[pk_column])
    if row_key is None:
        row_key = row_key_from_key(key_payload, pk_column)
    if row_key is None:
        raise MalformedEvent(f"{table}: cannot determine {pk_column}")

    lsn = source.get("lsn")
    if lsn is None:
        # Every event -- snapshot reads included -- must carry a real log
        # position. Inventing one (a zero sentinel) silently breaks ordering and
        # dedup, so fail closed instead.
        raise MalformedEvent(
            f"{table}: {op!r} event has no source.lsn -- cannot order or dedup it"
        )
    event_ms = source.get("ts_ms") or payload.get("ts_ms") or 0

    return ChangeEvent(
        table=table,
        op=op,
        row_key=row_key,
        lsn=int(lsn),
        event_ms=int(event_ms),
        before=before if isinstance(before, dict) else None,
        after=after if isinstance(after, dict) else None,
        source=source,
    )
