"""Freshness of the curated units: has a source file changed since approval?

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The header of a unit holds the SHA-256 of each source file as it was when the unit
was written. This re-reads the sources and lists every unit whose source changed as
`stale`. A source named by a logical name is read through the local source map; one
that cannot be read here is listed as `unchecked`. Nothing is changed: approval of a
stale unit is the owner's decision. The Wikipedia corpus has no source to compare
with, so only its snapshot is reported. Library only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ai.embeddings.chunks import CURATED_ID
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc


@dataclass
class Freshness:
    corpus_id: str
    checked: int = 0
    stale: list[dict] = field(default_factory=list)
    unchecked: list[dict] = field(default_factory=list)
    snapshot: str | None = None


def _source_file(entry: dict, root: Path, local: dict[str, Path] | None):
    """The file of a source entry and why it cannot be read (None when it can)."""
    if "path" in entry:
        return root / entry["path"], None
    if local is None:
        return None, "no local source map"
    if entry["name"] not in local:
        return None, "the name is not in the local source map"
    return local[entry["name"]], None


def check(
    corpus_id: str,
    corpus_dir: Path,
    root: Path = uc.ROOT,
    local_sources: Path = uc.LOCAL_SOURCES_FILE,
) -> Freshness:
    result = Freshness(corpus_id)
    if corpus_id != CURATED_ID:
        manifest = wc.read_previous(corpus_dir / corpus_id)
        result.snapshot = manifest.get("snapshot")
        return result
    try:
        local = uc.load_local_sources(local_sources, root)
    except FileNotFoundError:
        local = None
    for unit in uc.scan_units(corpus_dir):
        for entry in unit.header["source"]:
            label = str(entry.get("path") or entry["name"])
            file, reason = _source_file(entry, root, local)
            if reason is None and not file.is_file():
                reason = "the source file was not found"
            if reason:
                result.unchecked.append(
                    {"unit_id": unit.unit_id, "source": label, "reason": reason}
                )
                continue
            result.checked += 1
            current = uc.sha256_file(file)
            if current != entry["sha256"]:
                result.stale.append(
                    {
                        "unit_id": unit.unit_id,
                        "source": label,
                        "recorded": entry["sha256"],
                        "current": current,
                    }
                )
    return result
