"""Result and citation records of a retrieval.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A citation names the chunk, its unit and version, the section, the source and the
content hash. The text form is `[unit_id@version §section]`. A hit without a
complete citation is an error. Library only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

CITATION_FIELDS = (
    "corpus",
    "unit_id",
    "section",
    "version",
    "source",
    "last_updated",
    "content_hash",
    "chunk_id",
)
DIRECT = "direct"
RELATED = "related"
OK = "ok"
NO_MATCH = "no_match"


@dataclass(frozen=True)
class Citation:
    corpus: str
    unit_id: str
    section: str
    version: int
    source: str
    last_updated: str
    content_hash: str
    chunk_id: str
    # Stays empty until the licence of a corpus is read.
    licence: str = ""

    def text(self) -> str:
        return f"[{self.unit_id}@{self.version} §{self.section}]"

    def validate(self) -> None:
        for name in CITATION_FIELDS:
            if getattr(self, name) in (None, ""):
                raise ValueError(
                    f"{self.chunk_id or '?'}: citation field {name} is empty"
                )


def citation_from(chunk_id: str, metadata: dict) -> Citation:
    """The citation of a chunk, read from its stored metadata; it must be complete."""
    citation = Citation(
        corpus=metadata.get("corpus", ""),
        unit_id=metadata.get("unit_id", ""),
        section=metadata.get("section", ""),
        version=metadata.get("version"),
        source=metadata.get("source", ""),
        last_updated=metadata.get("last_updated", ""),
        content_hash=metadata.get("content_hash", ""),
        chunk_id=chunk_id,
    )
    citation.validate()
    return citation


@dataclass(frozen=True)
class Hit:
    chunk_id: str
    text: str
    citation: Citation
    role: str
    piece: int
    fused_score: float | None = None
    # Rank (from 1) and score of the chunk in each retriever that returned it.
    ranks: dict = field(default_factory=dict)
    scores: dict = field(default_factory=dict)
    # Related hits only: the unit that led here and the link.
    via_unit: str | None = None
    link_type: str | None = None
    link_weight: float | None = None


@dataclass(frozen=True)
class Segment:
    chunk_id: str
    text_hash: str
    text: str
    citation: str


@dataclass(frozen=True)
class Block:
    unit_id: str
    role: str
    segments: tuple[Segment, ...]

    @property
    def citation(self) -> str:
        """The citations of the segments, each once, in segment order."""
        return " ".join(dict.fromkeys(s.citation for s in self.segments))


@dataclass
class Result:
    query: str
    processed: dict
    filters: dict
    corpus: str
    mode: str
    build_id: str
    collection: str
    status: str
    retrievers: dict
    config: dict
    hits: list[Hit] = field(default_factory=list)
    blocks: list[Block] = field(default_factory=list)
    context: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
