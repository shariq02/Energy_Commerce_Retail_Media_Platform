"""Build the metric, contract, rule, model card and approval rule units.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Writes pending units and rebuilds the manifest. It prints the plan first and asks
before writing (y or yes); --apply writes without asking, --dry-run never writes.

Usage (repository root):
    python -m scripts.knowledge.build_units [--apply | --dry-run]
"""

from __future__ import annotations

import argparse
import ast
import re
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
MODEL_CARDS_DIR = Path("src/model_cards")
APPROVAL_RULES_FILE = Path("databricks/models/lib/_approval_rules.py")
# Sections of a model card that become part of its unit, in card order. The sections
# Training and Data (run ids, parameters, row counts, libraries, processor) stay out.
CARD_SECTIONS = (
    "Decision",
    "Purpose and task",
    "Intended use",
    "Out of scope",
    "Flags",
    "Conditions and known limits",
    "Disclosures",
    "Revalidation trigger",
    "Baseline fallback",
)
CARD_METRIC_ROWS = (
    "skill_primary",
    "skill_primary_ci_low",
    "skill_primary_ci_high",
)
_CARD_TITLE = re.compile(r"^# Model card: (\S+)", re.MULTILINE)
_CARD_MODEL = re.compile(
    r"^_model `([^`]+)`.*?primary metric `([^`]+)` \(([^)]*)\)", re.MULTILINE
)


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


def card_sections(text: str) -> dict[str, str]:
    """The text of each `## ` section of a model card, keyed by its heading."""
    parts = re.split(r"^## (.+)$", text, flags=re.MULTILINE)
    return {parts[i].strip(): parts[i + 1].strip() for i in range(1, len(parts) - 1, 2)}


def card_metric_lines(section: str, primary: str) -> list[str]:
    """One line per row of the primary metric and of its skill and interval, from
    the first metric table of the card."""
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in section.splitlines()
        if line.startswith("|")
    ]
    if len(rows) < 3:
        return []
    header = rows[0]
    lines = []
    for row in rows[2:]:
        if row[0] not in (primary, *CARD_METRIC_ROWS):
            continue
        cells = [f"{header[i]} {row[i]}" for i in range(1, len(row)) if row[i]]
        lines.append(f"- {row[0]}: " + "; ".join(cells))
    return lines


def model_card_body(text: str) -> tuple[str, str]:
    """(task id, unit body) of one model card file."""
    title = _CARD_TITLE.search(text)
    meta = _CARD_MODEL.search(text)
    if not title or not meta:
        raise ValueError("a model card has no title or no model line")
    task, (model, primary, direction) = title.group(1), meta.groups()
    sections = card_sections(text)
    paragraphs = [
        f"# Model card: {task}",
        f"_model `{model}`, primary metric `{primary}` ({direction})_",
    ]
    for name in CARD_SECTIONS[:4]:
        if name in sections:
            paragraphs.append(f"## {name}\n{sections[name]}")
    metrics = card_metric_lines(sections.get("Metrics", ""), primary)
    if metrics:
        paragraphs.append("## Primary metric\n" + "\n".join(metrics))
    for name in CARD_SECTIONS[4:]:
        if name in sections:
            paragraphs.append(f"## {name}\n{sections[name]}")
    return task, "\n\n".join(paragraphs)


def read_approval_rules(path: Path) -> list[dict]:
    """The rules of APPROVAL_RULES read without running the notebook library: one
    dict per rule id, with the class, the description and the parameters."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else []
        if any(isinstance(t, ast.Name) and t.id == "APPROVAL_RULES" for t in targets):
            rows = ast.literal_eval(node.value)
            break
    else:
        raise ValueError(f"no APPROVAL_RULES in {path}")
    rules: dict[str, dict] = {}
    for rule_id, rule_class, description, parameter, value in rows:
        rule = rules.setdefault(
            rule_id,
            {
                "rule_id": rule_id,
                "class": rule_class,
                "description": description,
                "parameters": [],
            },
        )
        if parameter:
            rule["parameters"].append(f"{parameter} {value}")
    return list(rules.values())


def approval_rule_body(rule: dict) -> str:
    body = (
        f"# Approval rule: {rule['rule_id']}\n\n"
        f"**Class:** {rule['class']}\n\n"
        f"**Description:** {clean_text(rule['description'])}"
    )
    if rule["parameters"]:
        body += "\n\n**Parameters:** " + ", ".join(rule["parameters"])
    return body


def model_card_units(root: Path) -> list[dict]:
    units = []
    for path in sorted((root / MODEL_CARDS_DIR).glob("*/*.md")):
        task, body = model_card_body(path.read_text(encoding="utf-8"))
        units.append(
            {
                "unit_id": f"model_card.{task}",
                "kind": "model_card",
                "ecosystem": path.parent.name,
                "sources": [source_entry(root, path.relative_to(root))],
                "body": body,
            }
        )
    return units


def approval_rule_units(root: Path) -> list[dict]:
    sources = [source_entry(root, APPROVAL_RULES_FILE)]
    return [
        {
            "unit_id": f"approval_rule.{rule['rule_id'].lower()}",
            "kind": "approval_rule",
            "ecosystem": "shared",
            "sources": sources,
            "body": approval_rule_body(rule),
        }
        for rule in read_approval_rules(root / APPROVAL_RULES_FILE)
    ]


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
    units += model_card_units(root) + approval_rule_units(root)
    ids = [unit["unit_id"] for unit in units]
    duplicates = sorted(i for i, n in Counter(ids).items() if n > 1)
    if duplicates:
        raise ValueError(f"duplicate unit ids: {duplicates}")
    return units


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", type=Path, default=uc.ROOT)
    parser.add_argument("--corpus-dir", type=Path, default=None)
    uc.add_write_mode(parser)
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
    uc.print_plans(plans, uc.DERIVED_KINDS)
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
    return uc.write_plans(plans, corpus_dir, args.apply, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
