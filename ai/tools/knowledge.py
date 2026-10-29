"""Knowledge tool: cited chunks of the designated curated build.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The tool calls `ai.rag.retrieve()` in production mode, so it reads only the curated
corpus at its designated build. It uses the retrieval config of the designation and
changes only the number of results. The text of a hit is platform-owned and comes
inside the marked blocks of `context`. The retrieval libraries load on the first
call, not at import. Library only.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from pydantic import BaseModel, ConfigDict

from ai.tools import common as cm

SERVER = "knowledge_tools"
SCOPE = cm.load_scopes()[SERVER]
QUERY_MAX = 500
K_MAX = 10


class KnowledgeHit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chunk_id: str
    unit_id: str
    section: str
    version: int
    citation: str
    role: str
    text: str


class KnowledgeData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    corpus: str
    build_id: str
    hits: list[KnowledgeHit]
    context: str


class KnowledgeResult(cm.ToolResult):
    data: KnowledgeData


def designated_config(k: int) -> Any:
    """The retrieval config of the designated build, with `k` results."""
    from ai.knowledge_quality import state as qstate
    from ai.rag import store as st
    from ai.rag.config import RetrievalConfig
    from scripts.knowledge import _unit_common as uc

    event = qstate.designated_event(uc.CORPUS_DIR.parent, st.CURATED_ID)
    if event is None:
        raise cm.ToolFailure(
            "internal_error",
            "no curated build is designated",
            "run the acceptance gate",
        )
    return replace(RetrievalConfig.from_dict(event["config"]), final_k=k)


def run_retrieve(query: str, filters: dict | None, config: Any) -> Any:
    from ai.rag import retrieve as rt

    return rt.retrieve(query, filters=filters, config=config, production=True)


@cm.guarded
def retrieve(
    query: str, k: int = 5, filters: dict[str, str] | None = None
) -> KnowledgeResult:
    text = query.strip()
    if not 1 <= len(text) <= QUERY_MAX:
        raise cm.ToolFailure(
            "invalid_argument", f"the query needs 1 to {QUERY_MAX} characters", ""
        )
    if not 1 <= k <= K_MAX:
        raise cm.ToolFailure("invalid_argument", f"k is outside 1 to {K_MAX}", "")
    try:
        result = run_retrieve(text, filters, designated_config(k))
    except ValueError as exc:
        raise cm.ToolFailure(
            "invalid_argument", cm.clean_text(exc), "filters: kind, ecosystem, source"
        ) from exc
    hits = [
        KnowledgeHit(
            chunk_id=hit.chunk_id,
            unit_id=hit.citation.unit_id,
            section=hit.citation.section,
            version=int(hit.citation.version),
            citation=hit.citation.text(),
            role=hit.role,
            text=hit.text,
        )
        for hit in result.hits
    ]
    dates = [hit.citation.last_updated for hit in result.hits]
    notes = [] if hits else ["no unit matched the query"]
    return KnowledgeResult(
        data=KnowledgeData(
            status=result.status,
            corpus=result.corpus,
            build_id=result.build_id,
            hits=hits,
            context=result.context,
        ),
        source_tables=[f"knowledge:{result.corpus}:{result.build_id}"],
        as_of=max(dates) if dates else None,
        row_count=len(hits),
        warnings=notes,
    )
