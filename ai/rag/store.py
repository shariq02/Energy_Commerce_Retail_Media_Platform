"""Open the stored indexes of one build and check them against its record.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The collection, keyword index, graph copy and build record of a build are kept in
its build folder, so a call may name any kept build. The model files must equal the
recorded hashes, and the keyword index and the graph must equal the recorded versions.
Everything is opened for reading; nothing is created or changed. A loaded store is
kept for the next call of the same build. Library only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import keyword_index as ki
from ai.embeddings import knowledge_graph as kg
from ai.knowledge_quality import integrity
from ai.knowledge_quality import state as qstate
from ai.rag import graph as gr
from ai.rag import keyword_fts as fts
from ai.rag import query as qy
from ai.rag.dense import DenseRetriever
from ai.rag.keyword import Bm25Keyword
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc

CURATED_ID = "curated"
WIKIPEDIA_ID = wc.CORPUS_ID
PINNED = ("model_onnx_sha256", "tokenizer_sha256")
_CACHE: dict[tuple, Store] = {}


@dataclass
class Store:
    corpus_id: str
    mode: str
    record: dict
    collection_name: str
    collection: object
    dense: DenseRetriever
    keyword: Bm25Keyword | fts.FtsKeyword
    graph: gr.Graph
    glossary: list
    unit_ids: set
    # Curated only: chunk id -> metadata, so the filter runs before ranking.
    metadata: dict


def manifest_path(corpus_id: str, corpus_dir: Path) -> Path:
    if corpus_id == CURATED_ID:
        return corpus_dir / uc.MANIFEST_NAME
    if corpus_id == WIKIPEDIA_ID:
        return corpus_dir / WIKIPEDIA_ID / wc.MANIFEST_NAME
    raise ValueError(f"unknown corpus {corpus_id!r}")


def read_manifest(corpus_id: str, corpus_dir: Path) -> dict:
    path = manifest_path(corpus_id, corpus_dir)
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found: run the corpus build first")
    return json.loads(path.read_text(encoding="utf-8"))


def read_record(corpus_id: str, corpus_dir: Path) -> dict:
    record = read_manifest(corpus_id, corpus_dir).get(br.RECORD_KEY)
    if not record:
        raise ValueError(f"{corpus_id}: no build record: run the embedding build")
    return record


def _open(corpus_id: str, corpus_dir: Path, model_dir: Path, record: dict) -> Store:
    ai_dir = corpus_dir.parent
    hashes = emb.model_hashes(model_dir)
    changed = [k for k in PINNED if record["model"].get(k) != hashes[k]]
    if changed:
        raise ValueError(f"model files differ from the build record: {changed}")
    chroma = br.chroma_dir(ai_dir)
    if not chroma.is_dir():
        raise FileNotFoundError(f"{chroma} not found: run the embedding build")
    try:
        collection = coll.open_client(chroma).get_collection(
            record["collection"], embedding_function=None
        )
    except Exception as error:
        raise ValueError(
            f"collection {record['collection']} cannot be opened"
        ) from error
    dense = DenseRetriever(
        collection, emb.Embedder(model_dir), emb.token_counter(model_dir)
    )
    keyword_file = ai_dir / record["keyword_index"]["file"]
    metadata: dict = {}
    glossary: list = []
    if corpus_id == CURATED_ID:
        if emb.file_sha256(keyword_file) != record["keyword_index"]["version"]:
            raise ValueError("the keyword index file differs from the build record")
        chunk_ids, bm25 = ki.load_index(keyword_file)
        page = collection.get(include=["metadatas", "documents"])
        metadata = dict(zip(page["ids"], page["metadatas"], strict=True))
        keyword = Bm25Keyword(chunk_ids, bm25, metadata)
        directory = br.build_graph_dir(ai_dir, corpus_id, record["build_id"])
        if kg.graph_version(directory) != record["graph"]["version"]:
            raise ValueError("the graph differs from the build record")
        graph = gr.read_curated(directory)
        texts = [
            doc
            for doc, meta in zip(page["documents"], page["metadatas"], strict=True)
            if meta.get("kind") == "glossary"
        ]
        glossary = qy.glossary_entries(texts)
    else:
        index = fts.index_path(ai_dir, record["build_id"])
        problem = fts.check_index(index, record, record["keyword_index"]["version"])
        if problem:
            raise ValueError(problem)
        keyword = fts.FtsKeyword(index)
        edge_file = (
            br.build_graph_dir(ai_dir, corpus_id, record["build_id"]) / kg.EDGES_FILE
        )
        if uc.sha256_file(edge_file) != record["graph"]["version"]:
            raise ValueError("the graph differs from the build record")
        graph = gr.read_wikipedia(edge_file)
    return Store(
        corpus_id,
        record["mode"],
        record,
        record["collection"],
        collection,
        dense,
        keyword,
        graph,
        glossary,
        graph.unit_ids if corpus_id == CURATED_ID else set(),
        metadata,
    )


def read_build_record(corpus_id: str, corpus_dir: Path, build_id: str) -> dict:
    """The record of a kept build, from its build folder."""
    record = br.read_build_record(corpus_dir.parent, corpus_id, build_id)
    if record is None:
        raise ValueError(
            f"build {build_id} of {corpus_id} is not kept (no build record): "
            "run python -m ai.embeddings.build migrate, or build it again"
        )
    return record


def load(
    corpus_id: str, corpus_dir: Path, model_dir: Path, build_id: str | None
) -> Store:
    """The store of the kept build named by `build_id`."""
    if build_id is None:
        raise ValueError("a build id is required: no collection is designated yet")
    record = read_build_record(corpus_id, corpus_dir, build_id)
    key = (corpus_id, str(corpus_dir), str(model_dir), build_id)
    if key not in _CACHE:
        _CACHE[key] = _open(corpus_id, corpus_dir, model_dir, record)
    return _CACHE[key]


def load_designated(
    corpus_id: str, corpus_dir: Path, model_dir: Path
) -> tuple[Store, dict]:
    """The store of the designated build, after an integrity check at the first open.

    The designated build may differ from the recorded one; its build folder is read.
    """
    event = qstate.designated_event(corpus_dir.parent, corpus_id)
    if event is None:
        raise ValueError(
            f"no build of {corpus_id} is designated: run the acceptance gate"
        )
    record = read_build_record(corpus_id, corpus_dir, event["build_id"])
    key = (
        "designated",
        corpus_id,
        str(corpus_dir),
        str(model_dir),
        event["event_hash"],
    )
    if key not in _CACHE:
        recorded = read_record(corpus_id, corpus_dir)["build_id"]
        if event["build_id"] == recorded:
            report = integrity.check(corpus_id, corpus_dir, model_dir)
        else:
            report = integrity.check_kept(
                corpus_id, corpus_dir, model_dir, event["build_id"]
            )
        if not report.ok:
            raise ValueError(
                "integrity check failed: " + "; ".join(report.problems[:3])
            )
        _CACHE[key] = _open(corpus_id, corpus_dir, model_dir, record)
    return _CACHE[key], event


def clear_cache() -> None:
    for store in _CACHE.values():
        close = getattr(store.keyword, "close", None)
        if close:
            close()
    _CACHE.clear()
