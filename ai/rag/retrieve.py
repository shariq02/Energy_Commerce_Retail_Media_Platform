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

from dataclasses import dataclass
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


@dataclass
class Searched:
    """What the dense and the keyword retriever returned for one query."""

    processed: qy.ProcessedQuery
    where: dict | None
    filters: dict
    dense_status: str
    dense: list[tuple[str, float]]
    keyword_status: str
    keyword: list[tuple[str, float]]


def search_all(
    store: st.Store, query: str, filters: dict, config: RetrievalConfig
) -> Searched:
    processed = qy.process(
        query, store.unit_ids, store.glossary, config.expansion_terms_max
    )
    where = flt.to_where(filters)
    dense_status, dense = store.dense.search(
        processed.text, where, config.dense_k, config.dense_min_score
    )
    keyword_status, keyword = store.keyword.search(
        processed.terms, filters, config.keyword_k, config.keyword_min_score
    )
    return Searched(
        processed, where, filters, dense_status, dense, keyword_status, keyword
    )


def serve(
    store: st.Store, searched: Searched, config: RetrievalConfig
) -> tuple[list[Hit], list[Hit], str]:
    """The direct hits (fused top chunks), the related hits, and the graph status.

    A result is served only when every retriever that ran found something; one
    that ran and found nothing (a query term in no chunk, or every score under its
    threshold) means no match. A retriever that did not run (an empty query, a
    query over the model limit) does not count.
    """
    ran = (
        (searched.dense_status, searched.dense),
        (searched.keyword_status, searched.keyword),
    )
    if any(status == "ok" and not hits for status, hits in ran):
        return [], [], "no_seeds"
    fused = fusion.fuse(
        {
            DENSE: [c for c, _ in searched.dense],
            KEYWORD: [c for c, _ in searched.keyword],
        },
        config.rrf_constant,
        config.final_k,
    )
    direct = _direct_hits(store, fused, dict(searched.dense), dict(searched.keyword))
    graph_status = "disabled" if config.related_max == 0 else "no_seeds"
    related: list[Hit] = []
    if direct and config.related_max:
        graph_status = OK
        related = _related_hits(store, direct, searched.where, config.related_max)
    return direct, related, graph_status


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
    """Cited chunks for `query`. Without a build id, the designated build is used."""
    corpus_dir = corpus_dir or uc.CORPUS_DIR
    model_dir = model_dir or emb.DEFAULT_MODEL_DIR
    normalised = flt.normalise(filters, corpus)
    if production:
        if st.read_record(corpus, corpus_dir)["mode"] == "development":
            raise ValueError(f"{corpus} is a development corpus: refused in production")
        if build_id is not None:
            raise ValueError("production uses the designated build, not a build id")
    if build_id is None:
        store, event = st.load_designated(corpus, corpus_dir, model_dir)
        config = config or RetrievalConfig.from_dict(event["config"])
    else:
        store = st.load(corpus, corpus_dir, model_dir, build_id)
    config = config or RetrievalConfig()
    searched = search_all(store, query, normalised, config)
    direct, related, graph_status = serve(store, searched, config)
    hits = direct + related
    blocks = asm.build_blocks(hits)
    return Result(
        query=query,
        processed=searched.processed.as_dict(),
        filters=normalised,
        corpus=corpus,
        mode=store.mode,
        build_id=store.record["build_id"],
        collection=store.collection_name,
        status=OK if direct else NO_MATCH,
        retrievers={
            DENSE: {"status": searched.dense_status, "hits": len(searched.dense)},
            KEYWORD: {
                "status": searched.keyword_status,
                "hits": len(searched.keyword),
            },
            GRAPH: {"status": graph_status, "hits": len(related)},
        },
        config=config.as_dict(),
        hits=hits,
        blocks=blocks,
        context=asm.render(blocks),
    )
