"""Wikipedia article units for the knowledge corpus (pure Python, no Spark).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loads the selection terms and builds, hashes, checks and writes the article units
that the local commands handle. The text cleaning and splitting run in the
Databricks notebook library `_wikipedia_text`. Library only.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from scripts.knowledge._unit_common import sha256_bytes, sha256_text

KIND = "wikipedia_article"
UNIT_PREFIX = "wikipedia."
_UNIT_ID = re.compile(r"^wikipedia\.\d+$")
_ECOSYSTEM = re.compile(r"^[a-z_]+$")
REQUIRED_KEYS = (
    "unit_id",
    "kind",
    "version",
    "title",
    "revision_timestamp",
    "snapshot",
    "ecosystem",
    "selection_term",
    "rule_version",
    "sections",
    "content_hash",
)


@dataclass(frozen=True)
class Terms:
    """The selection rule: ordered (ecosystem, term) pairs, a cap per term."""

    version: int
    cap_per_term: int
    pairs: tuple[tuple[str, str], ...]
    file_hash: str


def load_terms(path: Path) -> Terms:
    # The notebook library _wikipedia_text has the same function; both hash the
    # file with line endings normalised, so the two hashes agree.
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    data = yaml.safe_load(raw.decode("utf-8")) or {}
    version, cap = data.get("version"), data.get("cap_per_term")
    if not isinstance(version, int) or not isinstance(cap, int) or cap < 1:
        raise ValueError(f"{path}: version and cap_per_term must be positive integers")
    groups = data.get("terms")
    if not isinstance(groups, dict) or not groups:
        raise ValueError(f"{path}: terms must be a mapping of ecosystem to term list")
    pairs, seen = [], set()
    for ecosystem, terms in groups.items():
        if not _ECOSYSTEM.match(str(ecosystem)) or not isinstance(terms, list):
            raise ValueError(f"{path}: bad ecosystem entry {ecosystem!r}")
        for term in terms:
            key = str(term).strip().lower()
            if not key or key in seen:
                raise ValueError(f"{path}: empty or repeated term {term!r}")
            seen.add(key)
            pairs.append((str(ecosystem), str(term).strip()))
    return Terms(version, cap, tuple(pairs), sha256_bytes(raw))


def unit_hash(title: str, sections: list[dict]) -> str:
    payload = json.dumps(
        {"title": title, "sections": sections},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return sha256_text(payload)


def article_unit(
    row: dict,
    sections: list[dict],
    ecosystem: str,
    term: str,
    terms: Terms,
    snapshot: str,
) -> dict:
    """Build the article unit from its cleaned sections."""
    title = str(row["title"])
    stamp = row["revisionTimestamp"]
    stamp = stamp.isoformat() if hasattr(stamp, "isoformat") else str(stamp)
    return {
        "unit_id": f"{UNIT_PREFIX}{int(row['id'])}",
        "kind": KIND,
        "version": int(row["revisionId"]),
        "title": title,
        "revision_timestamp": stamp,
        "snapshot": snapshot,
        "ecosystem": ecosystem,
        "selection_term": term,
        "rule_version": terms.version,
        "sections": sections,
        "content_hash": unit_hash(title, sections),
    }


def validate_article(unit: object, where: str) -> None:
    """Check one article unit read from a shard; a bad unit stops the build."""
    if not isinstance(unit, dict) or [k for k in REQUIRED_KEYS if k not in unit]:
        raise ValueError(f"{where}: article lacks required keys")
    if unit["kind"] != KIND or not _UNIT_ID.fullmatch(str(unit["unit_id"])):
        raise ValueError(f"{where}: bad kind or unit id {unit['unit_id']!r}")
    sections = unit["sections"]
    if not isinstance(sections, list) or not sections:
        raise ValueError(f"{where}: article has no sections")
    for section in sections:
        if not str(section.get("text", "")).strip():
            raise ValueError(f"{where}: empty section in {unit['unit_id']}")
    if unit_hash(unit["title"], sections) != unit["content_hash"]:
        raise ValueError(f"{where}: content hash differs for {unit['unit_id']}")


def shard_name(number: int) -> str:
    return f"shard_{number:05d}.jsonl"


def write_shard(path: Path, units: list[dict]) -> str:
    """Write one shard, one article per line; returns the file SHA-256."""
    text = "".join(
        json.dumps(unit, ensure_ascii=False, sort_keys=True) + "\n" for unit in units
    )
    path.write_text(text, encoding="utf-8", newline="\n")
    return sha256_text(text)
