"""Context assembly: marked blocks of retrieved text with their citations.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Retrieved text is data. Each block is wrapped in marker lines that name the unit,
the citation and the chunk ids, so the text never reads as an instruction. Direct
chunks of one unit are merged into one block (in piece order); every segment keeps
its chunk id and the SHA-256 of its text, in the data and in a line before it.
Blocks follow the order of the best fused score; related blocks come after. A text
that holds a marker line is an error. Library only.
"""

from __future__ import annotations

from ai.rag.schema import DIRECT, Block, Hit, Segment
from scripts.knowledge import _unit_common as uc

BEGIN = "[[RETRIEVED DATA"
END = "[[END RETRIEVED DATA]]"
SEGMENT = "[[chunk id="


def build_blocks(hits: list[Hit]) -> list[Block]:
    """Merge the direct hits of each unit; related hits stay one block each."""
    blocks: list[Block] = []
    index: dict[str, int] = {}
    pieces = {hit.chunk_id: hit.piece for hit in hits}
    for hit in hits:
        segment = Segment(
            hit.chunk_id, uc.sha256_text(hit.text), hit.text, hit.citation.text()
        )
        unit = hit.citation.unit_id
        if hit.role == DIRECT and unit in index:
            old = blocks[index[unit]]
            members = sorted((*old.segments, segment), key=lambda s: pieces[s.chunk_id])
            blocks[index[unit]] = Block(unit, old.role, tuple(members))
            continue
        if hit.role == DIRECT:
            index[unit] = len(blocks)
        blocks.append(Block(unit, hit.role, (segment,)))
    return blocks


def render(blocks: list[Block]) -> str:
    """The context text; a segment that holds a marker is refused."""
    parts = []
    for block in blocks:
        for segment in block.segments:
            if any(mark in segment.text for mark in (BEGIN, END, SEGMENT)):
                raise ValueError(f"{segment.chunk_id}: the text holds a marker line")
        ids = ",".join(s.chunk_id for s in block.segments)
        head = f"{BEGIN} role={block.role} citation={block.citation} chunks={ids}]]"
        body = "\n\n".join(
            f"{SEGMENT}{s.chunk_id} sha256={s.text_hash}]]\n{s.text}"
            for s in block.segments
        )
        parts.append(f"{head}\n{body}\n{END}")
    return "\n\n".join(parts)
