"""Generate JSON Schema files from the authored YAML data contracts.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: for each ``src/schemas/contracts/<source>.yml`` write a JSON Schema
(draft 2020-12) per Bronze table into ``src/schemas/contracts/generated/`` --
``<source>.<table>.schema.json`` -- describing a single record of that table,
derived from the table's ``columns`` list. A contract carries a top-level
``tables:`` list; each table needs ``name`` and ``columns``. The YAML contract
stays the source of truth; the generated JSON Schema is what ingestion-time
validation loads.

Usage:
    python3 src/schemas/_generate_jsonschema.py            # regenerate all
    python3 src/schemas/_generate_jsonschema.py --source smard
    python3 src/schemas/_generate_jsonschema.py --check    # fail if any file is stale
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

CONTRACTS_DIR = Path(__file__).resolve().parent / "contracts"
GENERATED_DIR = CONTRACTS_DIR / "generated"
SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

# contract column type -> (json type, extra keywords)
_TYPE_MAP: dict[str, dict[str, object]] = {
    "string": {"type": "string"},
    "int": {"type": "integer"},
    "integer": {"type": "integer"},
    "long": {"type": "integer"},
    "double": {"type": "number"},
    "float": {"type": "number"},
    "decimal": {"type": "number"},
    "boolean": {"type": "boolean"},
    "date": {"type": "string", "format": "date"},
    "timestamp": {"type": "string", "format": "date-time"},
}


def _column_schema(column: dict) -> dict:
    """Build the JSON Schema fragment for one contract column."""
    raw_type = column["type"]
    if raw_type not in _TYPE_MAP:
        message = f"column {column['name']!r}: unknown type {raw_type!r}"
        raise ValueError(message)

    base = dict(_TYPE_MAP[raw_type])
    if column.get("nullable", False):
        base["type"] = [base["type"], "null"]

    allowed = column.get("allowed_values")
    if allowed is not None:
        enum_values = list(allowed)
        if column.get("nullable", False):
            enum_values.append(None)
        base["enum"] = enum_values

    notes = column.get("notes")
    if notes is not None:
        base["description"] = " ".join(str(notes).split())

    return base


def build_table_schema(source: str, table: dict, contract_name: str) -> dict:
    """Turn one table entry of a parsed contract into a JSON Schema document."""
    columns = table["columns"]
    properties = {col["name"]: _column_schema(col) for col in columns}
    required = [col["name"] for col in columns if col.get("required", False)]

    description = (
        f"Generated from {contract_name} by _generate_jsonschema.py -- do not "
        "edit by hand. Types are logical; the Bronze load stores every column as "
        "string (inferSchema=false)."
    )
    return {
        "$schema": SCHEMA_DIALECT,
        "$id": f"https://ecrmap/schemas/contracts/{table['name']}.schema.json",
        "$comment": "ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform. "
        "Author: Sharique Mohammad. Generated file -- do not edit by hand.",
        "title": f"{source}: {table['name']} record",
        "description": description,
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _tables(contract: dict) -> list[dict]:
    """Return the contract's table list, accepting the legacy single-table form."""
    if "tables" in contract:
        return [t for t in contract["tables"] if "columns" in t]
    if "columns" in contract:
        name = contract.get("bronze_table", contract["source"]).split(".")[-1]
        return [{"name": name, "columns": contract["columns"]}]
    return []


def _render(schema: dict) -> str:
    return json.dumps(schema, indent=2, ensure_ascii=False) + "\n"


def generate(source: str | None, *, check: bool) -> int:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    contracts = sorted(CONTRACTS_DIR.glob("*.yml"))
    if source is not None:
        contracts = [p for p in contracts if p.stem == source]
        if not contracts:
            print(f"no contract found for source {source!r}", file=sys.stderr)
            return 2

    stale: list[str] = []
    for contract_path in contracts:
        contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
        for table in _tables(contract):
            schema_text = _render(
                build_table_schema(contract["source"], table, contract_path.name)
            )
            out_path = GENERATED_DIR / f"{table['name']}.schema.json"

            current = (
                out_path.read_text(encoding="utf-8") if out_path.exists() else None
            )
            if current == schema_text:
                continue
            if check:
                stale.append(out_path.name)
                continue
            out_path.write_text(schema_text, encoding="utf-8")
            print(f"wrote {out_path.relative_to(CONTRACTS_DIR.parent.parent)}")

    if check and stale:
        joined = ", ".join(stale)
        print(
            f"stale generated schema(s): {joined} -- run _generate_jsonschema.py",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="regenerate only this source's schema")
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit non-zero if any generated schema is out of date",
    )
    args = parser.parse_args()
    return generate(args.source, check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
