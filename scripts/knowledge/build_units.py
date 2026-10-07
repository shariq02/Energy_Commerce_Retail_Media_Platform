"""Build the derived knowledge units from their sources.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Purpose: convert the metric definitions, the contract descriptions and the
quality rules into Markdown units in ``ai/knowledge_corpus/``, then rebuild the
manifest. Only the included fields are written: metric key, definition and
grain; rule id, expression and severity; the contract description. Every unit is
written as ``pending``; only ``approve_units`` sets ``approved``.

Dry run by default. A unit whose body changed gets a new version and loses its
approval; the dry run lists those units.

Usage (from the repository root):
    python -m scripts.knowledge.build_units            # dry run
    python -m scripts.knowledge.build_units --apply    # write units and manifest
"""

from __future__ import annotations

import argparse
import ast
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import yaml

from scripts.knowledge import _unit_common as uc

METRIC_FILES = {
    "energy": Path("databricks/analytics/energy/semantic/01_metric_definitions.py"),
    "commerce": Path("databricks/analytics/commerce/semantic/01_metric_definitions.py"),
}
CONTRACTS_DIR = Path("src/schemas/contracts")
ECOSYSTEM_MAP = Path("src/schemas/reference/source_ecosystem_map.yml")


def read_metrics(path: Path) -> list[dict]:
    """Read the METRICS list from a notebook file without running it."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else []
        if any(isinstance(t, ast.Name) and t.id == "METRICS" for t in targets):
            rows = ast.literal_eval(node.value)
            break
    else:
        raise ValueError(f"no METRICS list in {path}")
    metrics = []
    for row in rows:
        if len(row) < 5:
            raise ValueError(f"{path}: a metric row has fewer than 5 fields")
        metrics.append({"metric_key": row[0], "definition": row[1], "grain": row[4]})
    return metrics


def read_ecosystems(path: Path) -> dict[str, str]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        entry["source_system"]: entry["ecosystem"]
        for entry in data["mappings"]
        if isinstance(entry, dict) and "ecosystem" in entry
    }


def clean_text(text: str) -> str:
    paragraphs = [" ".join(part.split()) for part in str(text).strip().split("\n\n")]
    return "\n\n".join(part for part in paragraphs if part)


def metric_body(metric: dict) -> str:
    return (
        f"# Metric: {metric['metric_key']}\n\n"
        f"**Definition:** {clean_text(metric['definition'])}\n\n"
        f"**Grain:** {clean_text(metric['grain'])}"
    )


def contract_body(source: str, description: str) -> str:
    return f"# Contract: {source}\n\n{clean_text(description)}"


def rule_body(source: str, rule: dict) -> str:
    return (
        f"# Rule: {rule['id']} ({source})\n\n"
        f"**Expression:** {clean_text(rule['expr'])}\n\n"
        f"**Severity:** {rule['severity']}"
    )


def source_entry(root: Path, relative: Path) -> dict:
    return {"path": relative.as_posix(), "sha256": uc.sha256_file(root / relative)}


def derived_units(root: Path) -> list[dict]:
    """One dict per derived unit: unit_id, kind, ecosystem, sources, body."""
    units = []
    for ecosystem, relative in METRIC_FILES.items():
        sources = [source_entry(root, relative)]
        for metric in read_metrics(root / relative):
            units.append(
                {
                    "unit_id": f"metric.{metric['metric_key']}",
                    "kind": "metric",
                    "ecosystem": ecosystem,
                    "sources": sources,
                    "body": metric_body(metric),
                }
            )
    ecosystems = read_ecosystems(root / ECOSYSTEM_MAP)
    for path in sorted((root / CONTRACTS_DIR).glob("*.yml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(contract, dict) or "source" not in contract:
            continue
        source = contract["source"]
        ecosystem = ecosystems[contract["source_system"]]
        sources = [source_entry(root, CONTRACTS_DIR / path.name)]
        units.append(
            {
                "unit_id": f"contract.{source}",
                "kind": "contract",
                "ecosystem": ecosystem,
                "sources": sources,
                "body": contract_body(source, contract["description"]),
            }
        )
        for rule in contract.get("quality_rules") or []:
            units.append(
                {
                    "unit_id": f"rule.{source}.{rule['id']}",
                    "kind": "rule",
                    "ecosystem": ecosystem,
                    "sources": sources,
                    "body": rule_body(source, rule),
                }
            )
    ids = [unit["unit_id"] for unit in units]
    duplicates = sorted(i for i, n in Counter(ids).items() if n > 1)
    if duplicates:
        raise ValueError(f"duplicate unit ids: {duplicates}")
    return units


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=Path, default=uc.ROOT)
    parser.add_argument("--corpus-dir", type=Path, default=None)
    parser.add_argument("--apply", action="store_true", help="write the units")
    args = parser.parse_args(argv)
    corpus_dir = args.corpus_dir or args.root / "ai" / "knowledge_corpus"
    today = datetime.now(UTC).date().isoformat()

    units = derived_units(args.root)
    plans = [
        uc.plan_unit(
            corpus_dir,
            unit["unit_id"],
            unit["kind"],
            unit["ecosystem"],
            unit["sources"],
            unit["body"],
            today,
        )
        for unit in units
    ]
    counts = Counter((plan.kind, plan.action) for plan in plans)
    for kind in uc.DERIVED_KINDS:
        line = ", ".join(
            f"{action} {counts[(kind, action)]}"
            for action in ("created", "revised", "refreshed", "unchanged")
        )
        print(f"{kind}: {line}")
    reset = [p.unit_id for p in plans if p.action == "revised" and p.was_approved]
    if reset:
        print(f"WARNING: {len(reset)} approved unit(s) lose their approval:")
        for unit_id in reset:
            print(f"  {unit_id}")
    produced = {unit["unit_id"] for unit in units}
    orphans = [
        unit.unit_id
        for unit in uc.scan_units(corpus_dir)
        if unit.kind in uc.DERIVED_KINDS and unit.unit_id not in produced
    ]
    if orphans:
        print(f"WARNING: {len(orphans)} unit(s) have no source any more (kept):")
        for unit_id in orphans:
            print(f"  {unit_id}")
    if not args.apply:
        print("Dry run: nothing written. Use --apply to write.")
        return 0
    for plan in plans:
        if plan.text is not None:
            uc.write_text(plan.path, plan.text)
    manifest = uc.write_manifest(corpus_dir)
    print(
        f"Manifest: corpus_version {manifest['corpus_version']}, "
        f"{manifest['unit_count']} unit(s), approval {manifest['approval']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
