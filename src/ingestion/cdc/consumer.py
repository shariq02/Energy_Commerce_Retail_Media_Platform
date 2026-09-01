"""CDC pipeline -- local Redpanda consumer.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: read the 10 Debezium CDC topics from the local Redpanda broker and land
every accepted change event as JSON Lines under data/cdc/landing/. Owns the
streaming concerns: consumer-group offsets for resumability, LSN-based ordering
and deduplication, watermark / late-event accounting, and a fail-closed stop on
malformed events. Databricks does the batch build of the Bronze tables from the
landed files -- this process never talks to Databricks.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.ingestion.cdc import config
from src.ingestion.cdc.debezium import ChangeEvent, MalformedEvent, parse_value
from src.ingestion.cdc.landing import LandingWriter
from src.ingestion.cdc.state import ConsumerState


class FailClosed(RuntimeError):
    """A data-level fault that must stop the consumer, not be retried."""


def _decode(raw: bytes | None):
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8"))


def _table_from_topic(topic: str) -> str:
    return topic.rsplit(".", 1)[-1]


def _end_offsets(messages: list) -> list[TopicPartition]:
    """Next offset to consume for every topic/partition in a processed batch.

    Committing these explicitly (rather than a bare commit of the implicit
    offset store) keeps offset tracking correct across a rebalance and covers
    every partition the batch touched, not just the last message.
    """
    highest: dict[tuple[str, int], int] = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        offset = msg.offset()
        if offset > highest.get(key, -1):
            highest[key] = offset
    return [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in highest.items()
    ]


class CdcConsumer:
    def __init__(self, *, idle_timeout: float = 15.0, from_beginning: bool = False):
        config.ensure_dirs()
        self._idle_timeout = idle_timeout
        self._run_id = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        self._state = ConsumerState(config.STATE_DB)
        self._writer = LandingWriter(config.LANDING_DIR, self._run_id)
        self._pk = config.PRIMARY_KEY
        self._applied = 0
        self._stale = 0
        self._late = 0
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "group.id": config.CONSUMER_GROUP,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "enable.partition.eof": False,
            }
        )

    # -- lifecycle ---------------------------------------------------------

    def run(self) -> dict:
        topics = config.all_topics()
        self._consumer.subscribe(topics)
        last_message_at = time.monotonic()
        try:
            while True:
                batch = self._poll_batch()
                if batch:
                    self._apply_batch(batch)
                    self._consumer.commit(
                        offsets=_end_offsets(batch),
                        asynchronous=False,
                    )
                    last_message_at = time.monotonic()
                elif time.monotonic() - last_message_at > self._idle_timeout:
                    break
        finally:
            self._writer.flush()
            self._consumer.close()
        return self._finish()

    def _poll_batch(self, max_records: int = 500, window: float = 2.0) -> list:
        """Collect up to max_records messages, then sort by (table, lsn)."""
        records = []
        deadline = time.monotonic() + window
        while len(records) < max_records and time.monotonic() < deadline:
            msg = self._consumer.poll(0.5)
            if msg is None:
                if records:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            records.append(msg)
        return records

    # -- apply -----------------------------------------------------------

    def _apply_batch(self, messages: list) -> None:
        parsed: list[ChangeEvent] = []
        for msg in messages:
            value = _decode(msg.value())
            if value is None:
                # Debezium delete tombstone -- the op='d' event carries the data.
                continue
            table = _table_from_topic(msg.topic())
            pk_column = self._pk.get(table)
            if pk_column is None:
                raise FailClosed(f"message on unexpected topic {msg.topic()}")
            try:
                event = parse_value(
                    value,
                    table=table,
                    pk_column=pk_column,
                    key_payload=_decode(msg.key()),
                )
            except MalformedEvent as exc:
                raise FailClosed(str(exc)) from exc
            parsed.append(event)

        parsed.sort(key=lambda e: (e.table, e.lsn))
        with self._state.transaction():
            for event in parsed:
                self._apply_event(event)

    def _apply_event(self, event: ChangeEvent) -> None:
        prior = self._state.last_lsn(event.table, event.row_key)
        stale = prior is not None and event.lsn <= prior
        late = event.event_ms < self._state.watermark_ms(event.table)

        if stale:
            self._stale += 1
            self._state.bump_watermark(
                event.table, event.event_ms, applied=False, stale=True, late=late
            )
            return

        self._writer.write(event)
        self._state.record_applied(
            event.table, event.row_key, event.lsn, event.op, event.event_iso
        )
        self._state.bump_watermark(
            event.table, event.event_ms, applied=True, stale=False, late=late
        )
        self._applied += 1
        if late:
            self._late += 1

    # -- reporting ------------------------------------------------------

    def _finish(self) -> dict:
        counts = self._writer.close()
        self._state.close()
        manifest = {
            "run_id": self._run_id,
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "events_applied": self._applied,
            "events_stale": self._stale,
            "events_late": self._late,
            "landed_files_by_table": counts,
        }
        manifest_path = config.LANDING_DIR / f"_manifest__{self._run_id}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Local Redpanda CDC consumer")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=15.0,
        help="stop after this many seconds with no new messages",
    )
    args = parser.parse_args()

    consumer = CdcConsumer(idle_timeout=args.idle_timeout)
    try:
        manifest = consumer.run()
    except FailClosed as exc:
        print(f"FAIL  consumer stopped (fail-closed): {exc}")
        return 1
    print("=" * 70)
    print("CDC CONSUMER RUN SUMMARY")
    print("=" * 70)
    print(f"run id          : {manifest['run_id']}")
    print(f"events applied  : {manifest['events_applied']}")
    print(f"events stale    : {manifest['events_stale']}  (older LSN, skipped)")
    print(f"events late     : {manifest['events_late']}  (out-of-order, applied)")
    for table, n in sorted(manifest["landed_files_by_table"].items()):
        print(f"  {table:<22} {n} event(s) landed")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
