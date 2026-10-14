"""Graph retriever: units linked one hop to the top hits.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Links are read from the knowledge graph files and followed in both directions.
Neighbours are ordered by seed (the fused order of the top hits), then link type,
then weight (higher first), then unit id. The graph is only read. Library only.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from ai.embeddings import knowledge_graph as kg
from scripts.knowledge import wikipedia_prepare as wp

# Lower comes first; a rule belongs to its contract before it shares a source.
TYPE_ORDER = {"rule_of": 0, "link": 0, "same_grain": 1, "same_source": 2}


class Graph:
    def __init__(self, links: dict[str, list[tuple[str, str, float]]], unit_ids: set):
        # unit id -> [(neighbour unit id, link type, weight)]
        self.links = links
        self.unit_ids = unit_ids

    def neighbours(self, seeds: list[str], exclude: set[str]) -> list[tuple]:
        """Return [(unit id, seed unit, link type, weight)], each unit once."""
        found, seen = [], set(exclude)
        for seed in seeds:
            ordered = sorted(
                self.links.get(seed, ()),
                key=lambda link: (TYPE_ORDER.get(link[1], 9), -link[2], link[0]),
            )
            for unit_id, kind, weight in ordered:
                if unit_id not in seen:
                    seen.add(unit_id)
                    found.append((unit_id, seed, kind, weight))
        return found


def _add(links, first: str, second: str, kind: str, weight: float) -> None:
    links[first].append((second, kind, weight))
    links[second].append((first, kind, weight))


def read_curated(directory: Path) -> Graph:
    """The units and typed links of the curated graph files."""
    nodes = [
        json.loads(line)
        for line in (directory / kg.NODES_FILE).read_text(encoding="utf-8").splitlines()
    ]
    links: dict = defaultdict(list)
    for line in (directory / kg.EDGES_FILE).read_text(encoding="utf-8").splitlines():
        edge = json.loads(line)
        _add(links, edge["source"], edge["target"], edge["type"], 1.0)
    return Graph(links, {node["node_id"] for node in nodes})


def read_wikipedia(edge_file: Path) -> Graph:
    """The click links between selected articles; the click count is the weight."""
    links: dict = defaultdict(list)
    for line in edge_file.read_text(encoding="utf-8").splitlines():
        edge = json.loads(line)
        first = f"{wp.UNIT_PREFIX}{int(edge['source_id'])}"
        second = f"{wp.UNIT_PREFIX}{int(edge['target_id'])}"
        _add(links, first, second, "link", float(edge["click_count"]))
    return Graph(links, set(links))
