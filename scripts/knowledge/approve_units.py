"""Approve knowledge units.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Purpose: set a unit's approval to ``approved`` with the approver, the date and
the approved version, then rebuild the manifest. This is the only command that
sets ``approved``. The approval stores the hash of the approved body. A unit
whose body no longer matches that hash is ``stale`` and waits for approval
again. A unit that is approved at its current version and body is skipped.

Dry run by default. Choose what to approve with --kind, --unit-id or
--all-pending.

Usage (from the repository root):
    python -m scripts.knowledge.approve_units --approver NAME --kind metric
    python -m scripts.knowledge.approve_units --approver NAME --unit-id contract.smard --apply
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from scripts.knowledge import _unit_common as uc


def needs_approval(unit: uc.Unit) -> bool:
    return (
        unit.approval_status != "approved"
        or unit.header["approval"]["approved_version"] != unit.header["version"]
    )


def select_units(
    units: list[uc.Unit],
    kinds: list[str],
    unit_ids: list[str],
    all_pending: bool,
) -> list[uc.Unit]:
    known = {unit.unit_id for unit in units}
    unknown = sorted(set(unit_ids) - known)
    if unknown:
        raise ValueError(f"unknown unit id(s): {unknown}")
    chosen = [
        unit
        for unit in units
        if all_pending or unit.kind in kinds or unit.unit_id in unit_ids
    ]
    return [unit for unit in chosen if needs_approval(unit)]


def approved_text(unit: uc.Unit, approver: str, today: str) -> str:
    header = dict(unit.header)
    header["approval"] = {
        "status": "approved",
        "approver": approver,
        "approved_at": today,
        "approved_version": unit.header["version"],
        "approved_content_hash": unit.content_hash,
    }
    return uc.render_unit(header, unit.body)


def approve_chosen(corpus_dir: Path, chosen: list[uc.Unit], approver: str) -> dict:
    today = datetime.now(UTC).date().isoformat()
    for unit in chosen:
        uc.write_text(unit.path, approved_text(unit, approver, today))
    return uc.write_manifest(corpus_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--approver", required=True)
    parser.add_argument("--kind", action="append", default=[], choices=uc.KIND_DIRS)
    parser.add_argument("--unit-id", action="append", default=[])
    parser.add_argument("--all-pending", action="store_true")
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--apply", action="store_true", help="write the approvals")
    args = parser.parse_args(argv)
    if not args.approver.strip():
        parser.error("--approver must not be empty")
    if not (args.kind or args.unit_id or args.all_pending):
        parser.error("choose --kind, --unit-id or --all-pending")

    chosen = select_units(
        uc.scan_units(args.corpus_dir), args.kind, args.unit_id, args.all_pending
    )
    for kind, count in sorted(Counter(unit.kind for unit in chosen).items()):
        print(f"{kind}: {count} unit(s) to approve")
    print(f"Total: {len(chosen)}")
    if not args.apply:
        print("Dry run: nothing written. Use --apply to write.")
        return 0
    manifest = approve_chosen(args.corpus_dir, chosen, args.approver.strip())
    print(f"Manifest approval: {manifest['approval']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
