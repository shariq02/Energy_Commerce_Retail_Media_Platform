"""Knowledge graph of the corpora.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Curated corpus: one node per unit; edges follow from the unit headers by rule, with
no model. `rule_of` runs from a rule to the contract of its source. `same_source`
joins units that share a source entry. `same_grain` joins metrics with the same
grain line. The graph is written as two text files, one record per line, sorted.
Wikipedia corpus: the edge file of the corpus is the graph; only a record is made.
Library only.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from scripts.knowledge import _unit_common as uc

NODES_FILE = "nodes.jsonl"
EDGES_FILE = "edges.jsonl"
_GRAIN = re.compile(r"^\*\*Grain:\*\*\s*(.+?)\s*$", re.MULTILINE)


def curated_graph(units: list[uc.Unit]) -> tuple[list[dict], list[dict], list[str]]:
    """Return (nodes, edges, rules without a contract)."""
    ids = {unit.unit_id for unit in units}
    edges: set[tuple[str, str, str]] = set()
    orphans: list[str] = []
    by_source: dict[str, list[str]] = defaultdict(list)
    by_grain: dict[str, list[str]] = defaultdict(list)
    for unit in units:
        for entry in unit.header["source"]:
            by_source[str(entry.get("path") or entry["name"])].append(unit.unit_id)
        if unit.kind == "rule":
            contract = f"contract.{unit.unit_id.split('.')[1]}"
            if contract in ids:
                edges.add((unit.unit_id, contract, "rule_of"))
            else:
                orphans.append(unit.unit_id)
        if unit.kind == "metric":
            for grain in _GRAIN.findall(unit.body):
                by_grain[grain].append(unit.unit_id)
    for edge_type, groups in (("same_source", by_source), ("same_grain", by_grain)):
        for members in groups.values():
            for first, second in combinations(sorted(set(members)), 2):
                edges.add((first, second, edge_type))
    nodes = [
        {
            "node_id": unit.unit_id,
            "kind": unit.kind,
            "ecosystem": unit.header["ecosystem"],
            "version": int(unit.header["version"]),
            "content_hash": unit.content_hash,
        }
        for unit in units
    ]
    edge_records = [
        {"source": a, "target": b, "type": kind} for a, b, kind in sorted(edges)
    ]
    return nodes, edge_records, sorted(orphans)


def _lines(records: list[dict]) -> str:
    return "".join(
        json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n"
        for record in records
    )


def graph_version(directory: Path) -> str:
    nodes = (directory / NODES_FILE).read_text(encoding="utf-8")
    edges = (directory / EDGES_FILE).read_text(encoding="utf-8")
    return uc.sha256_text(nodes + "\n" + edges)


def write_graph(directory: Path, nodes: list[dict], edges: list[dict]) -> dict:
    uc.write_text(directory / NODES_FILE, _lines(nodes))
    uc.write_text(directory / EDGES_FILE, _lines(edges))
    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "directory": directory.name,
        "version": graph_version(directory),
    }


def wikipedia_graph_record(article_count: int, edges: dict) -> dict:
    """The edge file of the corpus is the graph; its SHA-256 is the version."""
    return {
        "nodes": article_count,
        "edges": edges["edges"],
        "file": edges["file"],
        "version": edges["sha256"],
    }
