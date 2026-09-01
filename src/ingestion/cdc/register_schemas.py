"""CDC pipeline -- register the CDC event schemas.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: build one JSON Schema per operational table describing a landed CDC
event (the CDC metadata plus nullable before / after row images taken from the
generated contract schemas) and register it in the Schema Registry as
``<topic>-value`` under the configured compatibility level. A later change to an
operational table that breaks that level is then refused at registration time.
"""

from __future__ import annotations

import argparse
import json

from src.ingestion.cdc import config
from src.ingestion.cdc.landing import META_COLUMNS
from src.ingestion.cdc.schema_registry import SchemaRegistry, SchemaRegistryError

_GENERATED = config.REPO_ROOT / "src" / "schemas" / "contracts" / "generated"

_META_SCHEMA = {
    "_table": {"type": "string"},
    "_op": {"type": "string", "enum": ["c", "r", "u", "d"]},
    "_op_name": {"type": "string"},
    "_row_key": {"type": "string"},
    "_lsn": {"type": "integer"},
    "_event_ms": {"type": "integer"},
    "_event_ts": {"type": "string", "format": "date-time"},
    "_source_ts_ms": {"type": ["integer", "null"]},
    "_ingested_ts": {"type": "string", "format": "date-time"},
    "_deleted": {"type": "boolean"},
}


def _row_schema(table: str) -> dict:
    path = _GENERATED / f"{table}.schema.json"
    row = json.loads(path.read_text(encoding="utf-8"))
    return {
        "type": ["object", "null"],
        "properties": row.get("properties", {}),
        "additionalProperties": True,
    }


def event_schema(table: str) -> dict:
    row = _row_schema(table)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"CDC event: operational.{table}",
        "type": "object",
        "properties": {
            **_META_SCHEMA,
            "before": row,
            "after": row,
        },
        "required": list(META_COLUMNS),
        "additionalProperties": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Register CDC event schemas")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    registry = SchemaRegistry(config.SCHEMA_REGISTRY_URL, config.SCHEMA_COMPATIBILITY)
    if not args.dry_run and not registry.ping():
        print(f"FAIL  schema registry unreachable at {config.SCHEMA_REGISTRY_URL}")
        return 1

    if not args.dry_run:
        registry.set_global_compatibility()

    failures = []
    for table in config.TABLES:
        subject = f"{config.topic_for(table)}-value"
        schema = event_schema(table)
        if args.dry_run:
            jsonschema_ok = "properties" in schema and "before" in schema["properties"]
            print(f"{subject}: {len(json.dumps(schema))} bytes  valid={jsonschema_ok}")
            continue
        try:
            registry.set_subject_compatibility(subject)
            schema_id = registry.register(subject, schema)
            print(f"OK  {subject} -> schema id {schema_id}")
        except SchemaRegistryError as exc:
            print(f"FAIL  {subject}: {exc}")
            failures.append(subject)

    if args.dry_run:
        print(f"OK  {len(config.TABLES)} CDC event schemas built (dry run, not sent)")
        return 0
    if failures:
        print(f"FAIL  {len(failures)} subject(s) not registered: {failures}")
        return 1
    print(
        f"OK  {len(config.TABLES)} CDC event schemas registered "
        f"({config.SCHEMA_COMPATIBILITY})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
