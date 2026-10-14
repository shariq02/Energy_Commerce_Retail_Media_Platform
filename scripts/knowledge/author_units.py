"""Build the governance, pipeline and glossary units from written bodies.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Bodies are scripts/knowledge/authored/<unit_id>.md, listed in authored_units.yml.
Adds the header with the source hashes (from local_sources.yml) and rebuilds the
manifest. Units stay pending until approved. It prints the plan first and asks
before writing (y or yes); --apply writes without asking, --dry-run never writes.

Usage (repository root):
    python -m scripts.knowledge.author_units [--apply | --dry-run]
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import yaml

from scripts.knowledge import _unit_common as uc

AUTHORED_DIR = uc.ROOT / "scripts" / "knowledge" / "authored"
INDEX_NAME = "authored_units.yml"
AUTHORED_KINDS = ("governance", "pipeline", "glossary")
ECOSYSTEMS = ("energy", "commerce", "shared")
_ENTRY_KEYS = {"unit_id", "kind", "ecosystem", "sources"}


def read_index(path: Path) -> list[dict]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = data.get("units")
    if not isinstance(entries, list):
        raise TypeError(f"{path}: the index needs a 'units' list")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != _ENTRY_KEYS:
            raise ValueError(f"{path}: a unit entry has the keys {sorted(_ENTRY_KEYS)}")
        unit_id = str(entry["unit_id"])
        uc.validate_unit_id(unit_id, str(path))
        if entry["kind"] not in AUTHORED_KINDS:
            raise ValueError(f"{unit_id}: kind must be one of {AUTHORED_KINDS}")
        if not unit_id.startswith(f"{entry['kind']}."):
            raise ValueError(f"{unit_id}: the id must start with '{entry['kind']}.'")
        if entry["ecosystem"] not in ECOSYSTEMS:
            raise ValueError(f"{unit_id}: ecosystem must be one of {ECOSYSTEMS}")
        names = entry["sources"]
        if not isinstance(names, list) or not names:
            raise ValueError(f"{unit_id}: sources must be a non-empty list of names")
        if unit_id in seen:
            raise ValueError(f"duplicate unit id: {unit_id}")
        seen.add(unit_id)
    return entries


def authored_units(authored_dir: Path, local_sources: dict[str, Path]) -> list[dict]:
    """One dict per authored unit: unit_id, kind, ecosystem, sources, body."""
    units = []
    for entry in read_index(authored_dir / INDEX_NAME):
        body_path = authored_dir / f"{entry['unit_id']}.md"
        if not body_path.is_file():
            raise FileNotFoundError(f"{entry['unit_id']}: {body_path} not found")
        body = body_path.read_text(encoding="utf-8").strip()
        if not body:
            raise ValueError(f"{entry['unit_id']}: the body is empty")
        units.append(
            {
                "unit_id": entry["unit_id"],
                "kind": entry["kind"],
                "ecosystem": entry["ecosystem"],
                "sources": [
                    uc.named_source_entry(str(name), local_sources)
                    for name in entry["sources"]
                ],
                "body": body,
            }
        )
    return units


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--authored-dir", type=Path, default=AUTHORED_DIR)
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--local-sources", type=Path, default=uc.LOCAL_SOURCES_FILE)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    today = datetime.now(UTC).date().isoformat()

    try:
        local_sources = uc.load_local_sources(args.local_sources)
        units = authored_units(args.authored_dir, local_sources)
    except (FileNotFoundError, KeyError, TypeError, ValueError) as error:
        print(f"ERROR: {error}")
        return 2
    plans = [
        uc.plan_unit(
            args.corpus_dir,
            unit["unit_id"],
            unit["kind"],
            unit["ecosystem"],
            unit["sources"],
            unit["body"],
            today,
        )
        for unit in units
    ]
    uc.print_plans(plans, AUTHORED_KINDS)
    produced = {unit["unit_id"] for unit in units}
    orphans = [
        unit.unit_id
        for unit in uc.scan_units(args.corpus_dir)
        if unit.kind in AUTHORED_KINDS and unit.unit_id not in produced
    ]
    if orphans:
        print(f"WARNING: {len(orphans)} unit(s) have no body any more (kept):")
        for unit_id in orphans:
            print(f"  {unit_id}")
    return uc.write_plans(plans, args.corpus_dir, args.apply, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
