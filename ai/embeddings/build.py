"""Build and verify the dense collection, keyword index and graph of a corpus.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

`build` reads an approved corpus and its chunks, fills a new versioned collection
with their vectors, writes the keyword index and the graph, records the build in
the corpus manifest and keeps the previous collection as last-known-good. It prints
the plan first and asks before writing (y or yes); --apply writes without asking,
--dry-run never writes and does not load the model. The curated chunks are cut and
embedded here. The Wikipedia chunks and vectors are made in Databricks and fetched;
a sample is embedded here and must agree with them. An unapproved corpus, a
mismatch with the manifest or changed model files stop the build. `verify` compares
the manifest record with the corpus, the model files and the stored indexes.

Usage (repository root):
    python -m ai.embeddings.build build [--corpus curated|wikipedia|all]
    python -m ai.embeddings.build verify [--corpus curated|wikipedia|all]
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import keyword_index as ki
from ai.embeddings import knowledge_graph as kg
from ai.embeddings import progress
from ai.embeddings.chunks import (
    CURATED_ID,
    WIKIPEDIA_ID,
    Corpus,
    StoredVectors,
    read_curated,
    read_wikipedia,
    stored_sample,
)
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc

CORPORA = (CURATED_ID, WIKIPEDIA_ID)
PINNED_FILES = ("model_onnx_sha256", "tokenizer_sha256")
PARITY_SAMPLE = 50
PARITY_MIN_COSINE = 0.999


@dataclass
class Prepared:
    corpus: Corpus
    hashes: dict
    new_id: str
    unchanged: bool
    graph: tuple | None


def not_ready(name: str, corpus_dir: Path) -> str | None:
    """Why this corpus cannot be built yet (it waits for approval), else None."""
    if name == CURATED_ID:
        units = uc.scan_units(corpus_dir)
        if not units:
            return "there are no units"
        if any(unit.approval_status != "approved" for unit in units):
            return "units wait for approval"
        return None
    manifest = wc.read_previous(corpus_dir / WIKIPEDIA_ID)
    if not manifest:
        return "there is no Wikipedia manifest"
    if manifest["approval"]["status"] != "approved":
        return "the Wikipedia corpus is not approved"
    if not (corpus_dir / WIKIPEDIA_ID / wc.CHUNKS_NAME).is_file():
        return "its chunks are not fetched yet (run notebook 05, then run_all)"
    return None


def read_corpus(name: str, corpus_dir: Path, model_dir: Path, say=None) -> Corpus:
    if name == CURATED_ID:
        return read_curated(corpus_dir, emb.token_counter(model_dir))
    return read_wikipedia(corpus_dir / WIKIPEDIA_ID, say)


def check_model_files(corpus: Corpus, hashes: dict, new_model: bool) -> None:
    """Changed model files stop the build unless the owner asks for --new-model."""
    record = corpus.manifest.get(br.RECORD_KEY)
    if not record or new_model:
        return
    changed = [k for k in PINNED_FILES if record["model"].get(k) != hashes[k]]
    if changed:
        raise ValueError(
            f"model files differ from the manifest ({', '.join(changed)}); "
            "use --new-model to rebuild with them"
        )


def prepare(name: str, args: argparse.Namespace, ai_dir: Path) -> Prepared:
    progress.show(f"Preparing corpus {name}")
    corpus = read_corpus(name, args.corpus_dir, args.model_dir, progress.show)
    progress.show("  step: hash the model files")
    hashes = emb.model_hashes(args.model_dir)
    check_model_files(corpus, hashes, args.new_model)
    if corpus.vector_model and corpus.vector_model != {
        k: hashes[k] for k in PINNED_FILES
    }:
        raise ValueError("the fetched vectors come from other model files than here")
    new_id = br.build_id(corpus, hashes)
    chroma = br.chroma_dir(ai_dir)
    unchanged = False
    if chroma.exists():
        record = corpus.manifest.get(br.RECORD_KEY) or {}
        if record.get("build_id") == new_id:
            progress.show("  step: the build id is unchanged; verify the stored build")
            unchanged = not br.verify(
                corpus.corpus_id,
                br.input_from_corpus(corpus, ai_dir),
                hashes,
                coll.open_client(chroma),
                ai_dir,
                progress.show,
            )
    graph = kg.curated_graph(corpus.units) if corpus.units is not None else None
    return Prepared(corpus, hashes, new_id, unchanged, graph)


def print_plan(item: Prepared) -> None:
    corpus = item.corpus
    print(
        f"Corpus {corpus.corpus_id} ({corpus.mode}), "
        f"corpus_version {corpus.manifest['corpus_version']}"
    )
    print(
        f"  units {corpus.unit_count}, chunks {len(corpus.chunks)}, "
        f"units split {corpus.split_units}, sentences left out "
        + (
            "(counted in Databricks)"
            if corpus.skipped is None
            else str(len(corpus.skipped))
        )
    )
    made = "fetched from Databricks" if corpus.vector_file else "embedded here"
    print(f"  vectors: {made}")
    if corpus.without_chunks:
        print(
            f"  articles without a chunk (all text left out): {corpus.without_chunks}"
        )
    if item.graph is not None:
        nodes, edges, orphans = item.graph
        print(f"  graph: {len(nodes)} nodes, {len(edges)} edges")
        if orphans:
            print(f"  rules without a contract ({len(orphans)}): {orphans[:5]}")
    elif corpus.edges is not None:
        print(f"  graph: {corpus.unit_count} nodes, {corpus.edges['edges']} edges")
    name = coll.collection_name(corpus.corpus_id, item.new_id)
    print(f"  model {emb.MODEL_ID}, build id {item.new_id}, collection {name}")
    status = "unchanged: the build record verifies" if item.unchanged else "new build"
    print(f"  {status}")


def parity(corpus: Corpus, model_dir: Path) -> float:
    """Embed a sample of the fetched chunks here; the stored vectors must agree."""
    progress.show(f"  step: parity check, {PARITY_SAMPLE} chunks embedded here")
    step = max(1, len(corpus.chunks) // PARITY_SAMPLE)
    sample = corpus.chunks[::step][:PARITY_SAMPLE]
    stored = stored_sample(corpus.vector_file, [chunk.chunk_id for chunk in sample])
    missing = [chunk.chunk_id for chunk in sample if chunk.chunk_id not in stored]
    if missing:
        raise ValueError(f"the chunk file has no vector for {missing[:3]}")
    local = emb.Embedder(model_dir).embed([chunk.text for chunk in sample])
    cosines = [
        float(np.dot(stored[c.chunk_id], v)) for c, v in zip(sample, local, strict=True)
    ]
    lowest = min(cosines)
    if lowest < PARITY_MIN_COSINE:
        raise ValueError(
            f"fetched vectors disagree with the local model: lowest cosine {lowest:.5f}"
        )
    print(f"  parity: {len(sample)} chunks, lowest cosine {lowest:.5f}")
    return lowest


def build(item: Prepared, args: argparse.Namespace, ai_dir: Path) -> None:
    corpus = item.corpus
    if corpus.vector_file:
        lowest = parity(corpus, args.model_dir)
        source = StoredVectors(corpus.vector_file)
        vectors = {
            "made_in": "databricks",
            "file": corpus.vector_file.name,
            "sha256": corpus.vector_digest,
            "parity_lowest_cosine": round(lowest, 6),
        }
    else:
        source = emb.Embedder(args.model_dir)
        vectors = {"made_in": "local"}
    client = coll.open_client(br.chroma_dir(ai_dir))
    name = coll.collection_name(corpus.corpus_id, item.new_id)
    if name in coll.names(client, corpus.corpus_id):
        client.delete_collection(name)
    print(f"Building {name}")
    coll.fill(
        client,
        name,
        corpus.chunks,
        source,
        label=corpus.corpus_id,
    )
    progress.show("  step: write the keyword file")
    keyword_version = ki.write_index(
        br.keyword_path(ai_dir, corpus.corpus_id), corpus.chunks, progress.show
    )
    progress.show("  step: graph and build record")
    if item.graph is not None:
        nodes, edges, _ = item.graph
        graph = kg.write_graph(br.graph_dir(ai_dir), nodes, edges)
    else:
        graph = kg.wikipedia_graph_record(corpus.unit_count, corpus.edges)
    today = datetime.now(UTC).date().isoformat()
    record = br.make_record(
        corpus,
        item.hashes,
        item.new_id,
        corpus.manifest.get(br.RECORD_KEY) or {},
        keyword_version,
        graph,
        today,
        vectors,
    )
    br.write_record(corpus, record)
    keep = {record["collection"], record["last_known_good"]} - {None}
    removed = coll.prune(client, corpus.corpus_id, keep)
    print(f"Record written: {corpus.manifest_path}")
    print(
        f"  current {record['collection']}, last-known-good {record['last_known_good']}"
    )
    if removed:
        print(f"  removed older collections: {removed}")


def verify_all(names: tuple[str, ...], args: argparse.Namespace, ai_dir: Path) -> int:
    progress.show("Hashing the model files")
    hashes = emb.model_hashes(args.model_dir)
    client = coll.open_client(br.chroma_dir(ai_dir))
    failed = 0
    for name in names:
        progress.show(f"Verifying corpus {name}")
        try:
            if name == CURATED_ID:
                corpus = read_corpus(name, args.corpus_dir, args.model_dir)
                data = br.input_from_corpus(corpus, ai_dir)
            else:
                data = br.wikipedia_input(args.corpus_dir / WIKIPEDIA_ID, progress.show)
        except (FileNotFoundError, ValueError) as error:
            problems = [f"the corpus cannot be read: {error}"]
        else:
            problems = br.verify(name, data, hashes, client, ai_dir, progress.show)
        print(f"{name}: " + ("OK" if not problems else f"{len(problems)} problem(s)"))
        for problem in problems:
            print(f"  FAIL {problem}")
        failed += bool(problems)
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("build", "verify"))
    parser.add_argument("--corpus", choices=(*CORPORA, "all"), default="all")
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--model-dir", type=Path, default=emb.DEFAULT_MODEL_DIR)
    parser.add_argument(
        "--new-model",
        action="store_true",
        help="build with model files that differ from the manifest",
    )
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    ai_dir = args.corpus_dir.parent
    names = CORPORA if args.corpus == "all" else (args.corpus,)
    if args.command == "verify":
        return verify_all(names, args, ai_dir)

    items = [prepare(name, args, ai_dir) for name in names]
    for item in items:
        print_plan(item)
    todo = [item for item in items if not item.unchanged]
    if not todo:
        print("Nothing to build.")
        return 0
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    for item in todo:
        build(item, args, ai_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
