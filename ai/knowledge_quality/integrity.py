"""Integrity check of a corpus and every representation built from it.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Compares the build record with the corpus, the model files, the collection, the
keyword index and the graph (the 13b verifier), checks every curated chunk text and
metadata against the units, checks the SQLite index of the Wikipedia corpus, and
checks the designation chain. Units added, removed or changed since the designated
build are reported; they explain a difference and are not a failure by themselves.
`check_kept` checks a kept build that is not the recorded one from its build folder.
Nothing is written. Library only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ai.embeddings import build_record as br
from ai.embeddings import chunks as ch
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.knowledge_quality import state
from ai.rag import keyword_fts as fts

CURATED_ID = ch.CURATED_ID
WIKIPEDIA_ID = ch.WIKIPEDIA_ID


@dataclass
class IntegrityReport:
    corpus_id: str
    problems: list[str] = field(default_factory=list)
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


def unit_snapshot(manifest: dict) -> list[list]:
    """(unit id, version, content hash) of every curated unit, from the manifest."""
    return [
        [unit["unit_id"], unit["version"], unit["content_hash"]]
        for unit in manifest["units"]
    ]


def diff_units(old: list[list], new: list[list]) -> tuple[list, list, list]:
    """Return (added, removed, changed) unit ids between two snapshots."""
    before = {row[0]: tuple(row[1:]) for row in old}
    after = {row[0]: tuple(row[1:]) for row in new}
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(u for u in set(before) & set(after) if before[u] != after[u])
    return added, removed, changed


def _curated_chunks(client, record: dict, corpus: ch.Corpus) -> list[str]:
    """Every chunk of the collection must have the text and metadata of its unit."""
    try:
        collection = client.get_collection(
            record["collection"], embedding_function=None
        )
    except Exception:  # verify reports the missing collection
        return []
    got = collection.get(
        ids=[chunk.chunk_id for chunk in corpus.chunks],
        include=["documents", "metadatas"],
    )
    found = dict(
        zip(
            got["ids"],
            zip(got["documents"], got["metadatas"], strict=True),
            strict=True,
        )
    )
    bad = [
        chunk.chunk_id
        for chunk in corpus.chunks
        if found.get(chunk.chunk_id) != (chunk.text, chunk.metadata)
    ]
    if bad:
        return [
            f"{len(bad)} chunk(s) differ from their units in the collection: {bad[:3]}"
        ]
    return []


def check_kept(
    corpus_id: str, corpus_dir: Path, model_dir: Path, build_id: str, say=None
) -> IntegrityReport:
    """Check a kept build that is not the recorded one, from its own build folder.

    The corpus has moved on, so the chunks are not compared with it; the model
    files, the collection, the keyword file, the graph copy and the FTS index are.
    """
    say = say or (lambda line: None)
    ai_dir = corpus_dir.parent
    report = IntegrityReport(corpus_id)
    record = br.read_build_record(ai_dir, corpus_id, build_id)
    if record is None:
        report.problems.append(f"build {build_id} is not kept (no build record)")
        return report
    report.notes.append(
        f"build {build_id} is not the recorded build: chunks are not compared "
        "with the corpus"
    )
    for key, value in emb.model_hashes(model_dir).items():
        if record["model"].get(key) != value:
            report.problems.append(f"model file hash differs from the record: {key}")
    chroma = br.chroma_dir(ai_dir)
    if not chroma.is_dir():
        report.problems.append("the vector store folder is missing")
        return report
    name = record["collection"]
    try:
        collection = coll.open_client(chroma).get_collection(
            name, embedding_function=None
        )
    except Exception:  # any client error means the collection is unusable
        report.problems.append(f"collection {name} is missing")
    else:
        if collection.count() != record["chunk_count"]:
            report.problems.append(
                f"collection {name} holds {collection.count()} chunks, "
                f"not {record['chunk_count']}"
            )
    report.problems += br.index_problems(corpus_id, record, ai_dir, say)
    if corpus_id == WIKIPEDIA_ID:
        problem = fts.check_index(
            fts.index_path(ai_dir, build_id),
            record,
            record["keyword_index"]["version"],
        )
        if problem:
            report.problems.append(problem)
    report.problems += state.verify(ai_dir, corpus_id)
    return report


def check(
    corpus_id: str, corpus_dir: Path, model_dir: Path, say=None
) -> IntegrityReport:
    say = say or (lambda line: None)
    ai_dir = corpus_dir.parent
    report = IntegrityReport(corpus_id)
    chroma = br.chroma_dir(ai_dir)
    if not chroma.is_dir():
        report.problems.append("the vector store folder is missing")
        return report
    hashes = emb.model_hashes(model_dir)
    client = coll.open_client(chroma)
    manifest, corpus = {}, None
    try:
        if corpus_id == CURATED_ID:
            corpus = ch.read_curated(corpus_dir, emb.token_counter(model_dir))
            manifest = corpus.manifest
            data = br.input_from_corpus(corpus, ai_dir)
        else:
            data = br.wikipedia_input(corpus_dir / WIKIPEDIA_ID, say)
            manifest = data.manifest
    except (FileNotFoundError, ValueError) as error:
        report.problems.append(f"the corpus cannot be read: {error}")
    else:
        report.problems += br.verify(corpus_id, data, hashes, client, ai_dir, say)
        record = manifest.get(br.RECORD_KEY) or {}
        if corpus is not None and record:
            report.problems += _curated_chunks(client, record, corpus)
        if corpus is None and record:
            problem = fts.check_index(
                fts.index_path(ai_dir, record["build_id"]),
                record,
                record["keyword_index"]["version"],
            )
            if problem:
                report.problems.append(problem)
    report.problems += state.verify(ai_dir, corpus_id)
    event = state.designated_event(ai_dir, corpus_id)
    if event is None:
        report.notes.append("no build is designated yet")
    elif corpus_id == CURATED_ID and manifest.get("units"):
        report.added, report.removed, report.changed = diff_units(
            event["unit_snapshot"], unit_snapshot(manifest)
        )
    elif manifest.get("fingerprint") != event["corpus_fingerprint"]:
        report.notes.append("the article set or its text changed since the designation")
    return report
