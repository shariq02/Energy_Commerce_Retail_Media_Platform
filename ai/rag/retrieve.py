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

from dataclasses import dataclass, field
from pathlib import Path

from ai.embeddings import embedder as emb
from ai.judge import factory
from ai.knowledge_quality import state as qstate
from ai.rag import assembler as asm
from ai.rag import filters as flt
from ai.rag import fusion
from ai.rag import query as qy
from ai.rag import rerank as rr
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


def extra_name(slug: str) -> str:
    """The name of an extra dense retriever in ranks, scores and results."""
    return f"{DENSE}:{slug}"


def _direct_hits(store, fused, scores) -> list[Hit]:
    rows = _fetch(store, [chunk_id for chunk_id, _, _ in fused])
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
    # Extra dense retrievers: slug -> (status, hits). Notes: what serve() recorded.
    extra: dict = field(default_factory=dict)
    notes: dict = field(default_factory=dict)


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
    extra = {}
    for slug in config.embedders:
        if slug not in store.extra:
            raise ValueError(f"the store was opened without the {slug} collection")
        threshold = (config.extra_min_scores or {}).get(slug)
        extra[slug] = store.extra[slug].search(
            processed.text, where, config.dense_k, threshold
        )
    return Searched(
        processed, where, filters, dense_status, dense, keyword_status, keyword, extra
    )


def serve(
    store: st.Store, searched: Searched, config: RetrievalConfig, scores=None
) -> tuple[list[Hit], list[Hit], str]:
    """The direct hits (fused top chunks), the related hits, and the graph status.

    A result is served only when every retriever that ran found something; one
    that ran and found nothing (a query term in no chunk, or every score under its
    threshold) means no match. A retriever that did not run (an empty query, a
    query over the model limit) does not count. With `config.rerank` the fused top
    `rerank_candidates` are reordered by the cached cross-encoder scores `scores`;
    the status goes to `searched.notes`.
    """
    ran = [
        (searched.dense_status, searched.dense),
        (searched.keyword_status, searched.keyword),
        *searched.extra.values(),
    ]
    if any(status == "ok" and not hits for status, hits in ran):
        return [], [], "no_seeds"
    rankings = {
        DENSE: [c for c, _ in searched.dense],
        KEYWORD: [c for c, _ in searched.keyword],
    }
    found = {DENSE: dict(searched.dense), KEYWORD: dict(searched.keyword)}
    for slug, (_status, hits) in searched.extra.items():
        rankings[extra_name(slug)] = [c for c, _ in hits]
        found[extra_name(slug)] = dict(hits)
    reranking = config.rerank
    if reranking and scores is None:
        raise ValueError("rerank is on, but no score cache was given")
    fused = fusion.fuse(
        rankings,
        config.rrf_constant,
        config.rerank_candidates if reranking else config.final_k,
    )
    if reranking:
        texts = {
            c: row[0] for c, row in _fetch(store, [c for c, _, _ in fused]).items()
        }
        fused, status, missing = rr.reorder(
            config.rerank_model,
            searched.processed.text,
            fused,
            texts,
            scores,
            config.final_k,
        )
        note = {"status": status, "missing": missing}
        if status == rr.RERANKED:
            note["scores"] = {
                c: scores[
                    rr.pair_key(config.rerank_model, searched.processed.text, texts[c])
                ]
                for c, _, _ in fused
            }
        searched.notes["rerank"] = note
    direct = _direct_hits(store, fused, found)
    graph_status = "disabled" if config.related_max == 0 else "no_seeds"
    related: list[Hit] = []
    if direct and config.related_max:
        graph_status = OK
        related = _related_hits(store, direct, searched.where, config.related_max)
    return direct, related, graph_status


def top_scores(searched: Searched) -> dict:
    """The best score of each retriever that ran, by retriever name."""
    found = {DENSE: searched.dense, KEYWORD: searched.keyword}
    for slug, (_status, hits) in searched.extra.items():
        found[extra_name(slug)] = hits
    return {
        name: max((s for _, s in pairs), default=None) for name, pairs in found.items()
    }


def judge_filters(judge, config: RetrievalConfig, query: str, filters: dict):
    """The filters with the domain filter of the judge added when it is on and has an
    answer; returns (filters, notes)."""
    if not config.judge_filter or "ecosystem" in filters:
        return filters, {}
    if judge is None:
        return filters, {"judge_filter": {"status": "skipped: no judge"}}
    values = judge.area_filter(query, config.judge_filter_min)
    note = {"status": "applied" if values else "no answer", "values": values}
    return ({**filters, "ecosystem": values} if values else filters), {
        "judge_filter": note
    }


def judge_after(
    judge,
    config: RetrievalConfig,
    query: str,
    direct: list,
    related: list,
    searched=None,
):
    """Apply the relevance and the gate of the judge to the served hits; returns
    (direct, related, notes). A step without an answer keeps the hits."""
    notes: dict = {}
    if judge is None:
        if config.judge_relevance or config.judge_gate:
            notes["judge"] = {"status": "skipped: no judge"}
        return direct, related, notes
    reranked = (searched.notes.get("rerank", {}).get("scores", {})) if searched else {}
    if config.judge_relevance and direct:
        kept = []
        for hit in direct:
            value = judge.relevance(query, hit.text, reranked.get(hit.chunk_id))
            if value is None or value >= config.judge_relevance_min:
                kept.append(hit)
        notes["judge_relevance"] = {"kept": len(kept), "of": len(direct)}
        direct = kept
    if config.judge_gate and direct:
        context = "\n\n".join(hit.text for hit in direct)
        value = judge.answerable(
            query, context, top_scores(searched) if searched else {}
        )
        blocked = value is not None and value < config.judge_gate_min
        notes["judge_gate"] = {"value": value, "blocked": blocked}
        if blocked:
            direct, related = [], []
    return direct, related, notes


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
    judge=None,
) -> Result:
    """Cited chunks for `query`. Without a build id, the designated build is used.

    The switches of the config (second dense retriever, reranker, local judge) are off by
    default. A judge step needs a judge; without one the step is skipped and the
    result says so. `judge` is a judge object; without one, and with a switch on, a
    local judge is made from the parameters fitted on the build.
    """
    corpus_dir = corpus_dir or uc.CORPUS_DIR
    model_dir = model_dir or emb.DEFAULT_MODEL_DIR
    normalised = flt.normalise(filters, corpus)
    if production:
        if st.read_record(corpus, corpus_dir)["mode"] == "development":
            raise ValueError(f"{corpus} is a development corpus: refused in production")
        if build_id is not None:
            raise ValueError("production uses the designated build, not a build id")
    if build_id is None:
        event = qstate.designated_event(corpus_dir.parent, corpus)
        if event is None:
            raise ValueError(
                f"no build of {corpus} is designated: run the acceptance gate"
            )
        config = config or RetrievalConfig.from_dict(event["config"])
        store, _event = st.load_designated(
            corpus, corpus_dir, model_dir, config.embedders
        )
    else:
        config = config or RetrievalConfig()
        store = st.load(corpus, corpus_dir, model_dir, build_id, config.embedders)
    if judge is None and factory.switches_on(config):
        embedder = store.dense.embedder
        judge = factory.make_judge(
            config,
            corpus_dir.parent,
            corpus,
            store.record["build_id"],
            getattr(embedder, "embed_queries", embedder.embed),
        )
    normalised, notes = judge_filters(judge, config, query, normalised)
    scores = None
    if config.rerank:
        scores = rr.load_scores(rr.cache_path(corpus_dir.parent))
    searched = search_all(store, query, normalised, config)
    direct, related, graph_status = serve(store, searched, config, scores)
    direct, related, after = judge_after(
        judge, config, query, direct, related, searched
    )
    notes.update(after)
    hits = direct + related
    blocks = asm.build_blocks(hits)
    retrievers = {
        DENSE: {"status": searched.dense_status, "hits": len(searched.dense)},
        KEYWORD: {"status": searched.keyword_status, "hits": len(searched.keyword)},
        GRAPH: {"status": graph_status, "hits": len(related)},
    }
    for slug, (status, found) in searched.extra.items():
        retrievers[extra_name(slug)] = {"status": status, "hits": len(found)}
    retrievers.update({**searched.notes, **notes})
    if judge is not None:
        retrievers["judge"] = judge.summary()
    return Result(
        query=query,
        processed=searched.processed.as_dict(),
        filters=normalised,
        corpus=corpus,
        mode=store.mode,
        build_id=store.record["build_id"],
        collection=store.collection_name,
        status=OK if direct else NO_MATCH,
        retrievers=retrievers,
        config=config.as_dict(),
        hits=hits,
        blocks=blocks,
        context=asm.render(blocks),
    )
