"""Open the stored indexes of one build and check them against its record.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The collection, keyword index and graph exist for the build in the corpus manifest
only, so a call must name that build. The model files must equal the recorded
hashes, and the keyword index and the graph must equal the recorded versions.
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


def read_record(corpus_id: str, corpus_dir: Path) -> dict:
    path = manifest_path(corpus_id, corpus_dir)
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found: run the corpus build first")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    record = manifest.get(br.RECORD_KEY)
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
        directory = br.graph_dir(ai_dir)
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
        problem = fts.check_index(
            fts.index_path(ai_dir), record, record["keyword_index"]["version"]
        )
        if problem:
            raise ValueError(problem)
        keyword = fts.FtsKeyword(fts.index_path(ai_dir))
        edge_file = corpus_dir / WIKIPEDIA_ID / record["graph"]["file"]
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


def load(
    corpus_id: str, corpus_dir: Path, model_dir: Path, build_id: str | None
) -> Store:
    """The store of the build named by `build_id`, which must be the recorded one."""
    record = read_record(corpus_id, corpus_dir)
    if build_id is None:
        raise ValueError("a build id is required: no collection is designated yet")
    if build_id != record["build_id"]:
        raise ValueError(
            f"build {build_id} is not the recorded build {record['build_id']}: "
            "the keyword index and the graph exist for the recorded build only"
        )
    key = (corpus_id, str(corpus_dir), str(model_dir), build_id)
    if key not in _CACHE:
        _CACHE[key] = _open(corpus_id, corpus_dir, model_dir, record)
    return _CACHE[key]


def clear_cache() -> None:
    for store in _CACHE.values():
        close = getattr(store.keyword, "close", None)
        if close:
            close()
    _CACHE.clear()
