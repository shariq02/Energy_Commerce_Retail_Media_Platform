"""retrieve(): cited chunks for a question, from one build of one corpus.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The query is processed by rule, then searched by the dense and the keyword
retriever with the same filter. Reciprocal Rank Fusion gives the direct hits; the
graph adds linked units of the top hits as `related`. The context is assembled in
marked blocks. The result holds the query, filters, hits, scores, the status of
each retriever, the config and the build id. Nothing is written. The same query on
the same build gives the same result. Library only.
"""

from __future__ import annotations

from pathlib import Path

from ai.embeddings import embedder as emb
from ai.rag import assembler as asm
from ai.rag import filters as flt
from ai.rag import fusion
from ai.rag import query as qy
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from ai.rag.schema import DIRECT, NO_MATCH, OK, RELATED, Hit, Result, citation_from
from scripts.knowledge import _unit_common as uc

DENSE = "dense"
KEYWORD = "keyword"
GRAPH = "graph"


def _fetch(store: st.Store, chunk_ids: list[str]) -> dict[str, tuple[str, dict]]:
    """Text and metadata of the chunks, by chunk id."""
    if not chunk_ids:
        return {}
    page = store.collection.get(ids=chunk_ids, include=["documents", "metadatas"])
    return {
        chunk_id: (text, meta)
        for chunk_id, text, meta in zip(
            page["ids"], page["documents"], page["metadatas"], strict=True
        )
    }


def _first_chunk(store: st.Store, unit_id: str, where: dict | None):
    """The first piece of a unit that passes the filter, as (id, text, metadata)."""
    unit = {"unit_id": unit_id}
    page = store.collection.get(
        where={"$and": [unit, where]} if where else unit,
        include=["documents", "metadatas"],
    )
    rows = sorted(
        zip(page["ids"], page["documents"], page["metadatas"], strict=True),
        key=lambda row: (int(row[2]["piece"]), row[0]),
    )
    return rows[0] if rows else None


def _direct_hits(store, fused, dense_scores, keyword_scores) -> list[Hit]:
    rows = _fetch(store, [chunk_id for chunk_id, _, _ in fused])
    scores = {DENSE: dense_scores, KEYWORD: keyword_scores}
    hits = []
    for chunk_id, fused_score, ranks in fused:
        text, meta = rows[chunk_id]
        found = {n: scores[n][chunk_id] for n in ranks}
        citation = citation_from(chunk_id, meta)
        piece = int(meta["piece"])
        hits.append(
            Hit(chunk_id, text, citation, DIRECT, piece, fused_score, ranks, found)
        )
    return hits


def _related_hits(store, direct, where, limit) -> list[Hit]:
    seeds = list(dict.fromkeys(hit.citation.unit_id for hit in direct))
    related: list[Hit] = []
    for unit_id, seed, kind, weight in store.graph.neighbours(seeds, set(seeds)):
        if len(related) >= limit:
            break
        row = _first_chunk(store, unit_id, where)
        if row is None:
            continue
        chunk_id, text, meta = row
        related.append(
            Hit(
                chunk_id,
                text,
                citation_from(chunk_id, meta),
                RELATED,
                int(meta["piece"]),
                via_unit=seed,
                link_type=kind,
                link_weight=weight,
            )
        )
    return related


def retrieve(
    query: str,
    *,
    corpus: str = st.CURATED_ID,
    build_id: str | None = None,
    filters: dict | None = None,
    config: RetrievalConfig | None = None,
    production: bool = False,
    corpus_dir: Path | None = None,
    model_dir: Path | None = None,
) -> Result:
    config = config or RetrievalConfig()
    normalised = flt.normalise(filters, corpus)
    store = st.load(
        corpus,
        corpus_dir or uc.CORPUS_DIR,
        model_dir or emb.DEFAULT_MODEL_DIR,
        build_id,
    )
    if production and store.mode == "development":
        raise ValueError(f"{corpus} is a development corpus: refused in production")
    processed = qy.process(
        query, store.unit_ids, store.glossary, config.expansion_terms_max
    )
    where = flt.to_where(normalised)
    dense_status, dense = store.dense.search(
        processed.text, where, config.dense_k, config.dense_min_score
    )
    keyword_status, keyword = store.keyword.search(
        processed.terms, normalised, config.keyword_k, config.keyword_min_score
    )
    fused = fusion.fuse(
        {DENSE: [c for c, _ in dense], KEYWORD: [c for c, _ in keyword]},
        config.rrf_constant,
        config.final_k,
    )
    direct = _direct_hits(store, fused, dict(dense), dict(keyword))
    graph_status = "disabled" if config.related_max == 0 else "no_seeds"
    related: list[Hit] = []
    if direct and config.related_max:
        graph_status = OK
        related = _related_hits(store, direct, where, config.related_max)
    hits = direct + related
    blocks = asm.build_blocks(hits)
    return Result(
        query=query,
        processed=processed.as_dict(),
        filters=normalised,
        corpus=corpus,
        mode=store.mode,
        build_id=store.record["build_id"],
        collection=store.collection_name,
        status=OK if direct else NO_MATCH,
        retrievers={
            DENSE: {"status": dense_status, "hits": len(dense)},
            KEYWORD: {"status": keyword_status, "hits": len(keyword)},
            GRAPH: {"status": graph_status, "hits": len(related)},
        },
        config=config.as_dict(),
        hits=hits,
        blocks=blocks,
        context=asm.render(blocks),
    )
