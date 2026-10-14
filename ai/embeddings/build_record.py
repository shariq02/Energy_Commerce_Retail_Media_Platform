"""Build record in the corpus manifest, and its verifier.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The key `embedding` of a corpus manifest says which collection, keyword index and
graph were built from which corpus version with which model files. The verifier
hashes the files as a stream (nothing is held in memory), compares the record with
the corpus, the model files and the stored indexes, and checks a sample of the
chunks in the collection. Every step prints a line. Library only.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import knowledge_graph as kg
from ai.embeddings.chunks import Chunk, Corpus
from ai.embeddings.progress import progress_line, show
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_prepare as wp

RECORD_KEY = "embedding"
SAMPLE_SIZE = 1000
SAMPLE_BATCH = 250
HASH_BLOCK = 8 * 1024 * 1024


def chroma_dir(ai_dir: Path) -> Path:
    return ai_dir / "vector_store" / "data" / "chroma"


def keyword_path(ai_dir: Path, corpus_id: str) -> Path:
    return ai_dir / "vector_store" / "data" / "keyword" / f"{corpus_id}.json"


def graph_dir(ai_dir: Path) -> Path:
    return ai_dir / "knowledge_graph"


def chunks_hash_from_pairs(pairs) -> str:
    """Hash of the (chunk id, text SHA-256) pairs, in id order."""
    lines = sorted(f"{chunk_id}\t{text_hash}" for chunk_id, text_hash in pairs)
    return uc.sha256_text("\n".join(lines))


def chunks_hash(chunks: list[Chunk]) -> str:
    return chunks_hash_from_pairs((c.chunk_id, uc.sha256_text(c.text)) for c in chunks)


def make_build_id(
    fingerprint: str, hashes: dict, chunks_digest: str, vector_digest: str | None
) -> str:
    parts = [
        fingerprint,
        hashes["model_onnx_sha256"],
        hashes["tokenizer_sha256"],
        chunks_digest,
        vector_digest or "",
    ]
    return uc.sha256_text("|".join(parts))[:12]


def build_id(corpus: Corpus, hashes: dict) -> str:
    """The same corpus, model files and chunks always give the same build id."""
    return make_build_id(
        corpus.manifest["fingerprint"],
        hashes,
        chunks_hash(corpus.chunks),
        corpus.vector_digest,
    )


def make_record(
    corpus: Corpus,
    hashes: dict,
    new_id: str,
    previous: dict,
    keyword_version: str,
    graph: dict,
    today: str,
    vectors: dict,
) -> dict:
    name = coll.collection_name(corpus.corpus_id, new_id)
    old = previous.get("collection")
    good = old if old and old != name else previous.get("last_known_good")
    return {
        "build_id": new_id,
        "built_on": today,
        "collection": name,
        "last_known_good": good,
        "corpus_id": corpus.corpus_id,
        "mode": corpus.mode,
        "corpus_version": corpus.manifest["corpus_version"],
        "corpus_fingerprint": corpus.manifest["fingerprint"],
        "model": {
            "id": emb.MODEL_ID,
            "dimension": emb.DIMENSION,
            "max_tokens": emb.MAX_TOKENS,
            "similarity": "cosine",
            **hashes,
        },
        "unit_count": corpus.unit_count,
        "chunk_count": len(corpus.chunks),
        "split_units": corpus.split_units,
        "skipped_sentences": None if corpus.skipped is None else len(corpus.skipped),
        "articles_without_chunks": corpus.without_chunks,
        "vectors": vectors,
        "keyword_index": {
            "file": f"vector_store/data/keyword/{corpus.corpus_id}.json",
            "version": keyword_version,
        },
        "graph": graph,
    }


def write_record(corpus: Corpus, record: dict) -> None:
    """Set the `embedding` key in the manifest file; the other keys stay."""
    manifest = json.loads(corpus.manifest_path.read_text(encoding="utf-8"))
    manifest[RECORD_KEY] = record
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    uc.write_text(corpus.manifest_path, text)


def file_sha256_progress(path: Path, label: str, say=show) -> str:
    """SHA-256 of a file read in blocks, with a progress line; nothing is kept."""
    digest = hashlib.sha256()
    total = max(1, round(path.stat().st_size / 1e6))
    started, done, next_report = time.monotonic(), 0, 0
    with path.open("rb") as handle:
        while block := handle.read(HASH_BLOCK):
            digest.update(block)
            done += len(block)
            if done >= next_report or done == path.stat().st_size:
                say(progress_line(label, round(done / 1e6), total, started, "MB"))
                next_report = done + 64 * 1024 * 1024
    return digest.hexdigest()


def sample_positions(total: int, size: int = SAMPLE_SIZE) -> list[int]:
    """Evenly spaced positions; the same every run. All of them for a small corpus."""
    if total <= size:
        return list(range(total))
    step = total / size
    return [int(number * step) for number in range(size)]


@dataclass
class VerifyInput:
    """What the verifier compares with the record."""

    manifest: dict
    chunk_total: int
    chunks_digest: str
    vector_digest: str | None
    # (chunk id, SHA-256 of the text, unit id) of the sampled chunks
    sample: list[tuple[str, str, str]]
    graph_version: str | None
    problems: list[str] = field(default_factory=list)


def input_from_corpus(corpus: Corpus, ai_dir: Path) -> VerifyInput:
    """From a corpus already in memory (the build has read it anyway)."""
    sample = [
        (c.chunk_id, uc.sha256_text(c.text), c.metadata["unit_id"])
        for c in (corpus.chunks[i] for i in sample_positions(len(corpus.chunks)))
    ]
    if corpus.edges is not None:
        graph_version = corpus.edges["sha256"]
    else:
        directory = graph_dir(ai_dir)
        graph_version = (
            kg.graph_version(directory)
            if (directory / kg.EDGES_FILE).is_file()
            else None
        )
    return VerifyInput(
        corpus.manifest,
        len(corpus.chunks),
        chunks_hash(corpus.chunks),
        corpus.vector_digest,
        sample,
        graph_version,
    )


def wikipedia_input(wiki_dir: Path, say=show) -> VerifyInput:
    """Without loading the corpus: file hashes as a stream, the chunk ids and text
    hashes from two columns of the chunk file, and the sampled rows."""
    import pyarrow.parquet as pq

    manifest = wc.read_previous(wiki_dir)
    if not manifest:
        raise FileNotFoundError(f"{wiki_dir / wc.MANIFEST_NAME} not found")
    problems = []
    say("  step: hash the shard files and compare them with the manifest")
    expected = {entry["name"]: entry["sha256"] for entry in manifest["shards"]}
    files = sorted((wiki_dir / wc.SHARD_DIR_NAME).glob("shard_*.jsonl"))
    if {path.name for path in files} != set(expected):
        problems.append("shard files were added or removed since the manifest")
    started = time.monotonic()
    for number, path in enumerate(files, 1):
        if uc.sha256_file(path) != expected.get(path.name):
            problems.append(f"shard {path.name} differs from the manifest")
        say(progress_line("shards", number, len(files), started, "files"))
    say("  step: hash the edge file")
    edges = wiki_dir / wc.EDGES_NAME
    if not edges.is_file() or uc.sha256_file(edges) != (
        manifest.get("edges") or {}
    ).get("sha256"):
        problems.append("the edge file differs from the manifest")
    chunk_file = wiki_dir / wc.CHUNKS_NAME
    if not chunk_file.is_file():
        raise FileNotFoundError(f"{chunk_file} not found")
    say("  step: hash the chunk file")
    vector_digest = file_sha256_progress(chunk_file, "chunk file", say)
    say("  step: read the chunk ids and text hashes, keep the sampled rows")
    parquet = pq.ParquetFile(chunk_file)
    total = parquet.metadata.num_rows
    wanted = set(sample_positions(total))
    pairs, sample, row = [], [], 0
    started = time.monotonic()
    for group in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(
            group, columns=["chunk_id", "text_hash", "article_id"]
        )
        columns = [table.column(i).to_pylist() for i in range(3)]
        for offset, (chunk_id, text_hash, article_id) in enumerate(zip(*columns)):
            pairs.append((chunk_id, text_hash))
            if row + offset in wanted:
                sample.append((chunk_id, text_hash, f"{wp.UNIT_PREFIX}{article_id}"))
        row += table.num_rows
        say(progress_line("chunk ids", row, total, started, "rows"))
    return VerifyInput(
        manifest,
        total,
        chunks_hash_from_pairs(pairs),
        vector_digest,
        sample,
        (manifest.get("edges") or {}).get("sha256"),
        problems,
    )


def check_sample(
    collection, sample: list[tuple[str, str, str]], label: str, say
) -> list:
    """Every sampled chunk must be in the collection with the same text and unit."""
    bad: list[str] = []
    started = time.monotonic()
    for start in range(0, len(sample), SAMPLE_BATCH):
        part = sample[start : start + SAMPLE_BATCH]
        got = collection.get(
            ids=[item[0] for item in part], include=["documents", "metadatas"]
        )
        found = dict(
            zip(
                got["ids"],
                zip(got["documents"], got["metadatas"], strict=True),
                strict=True,
            )
        )
        for chunk_id, text_hash, unit_id in part:
            item = found.get(chunk_id)
            if (
                item is None
                or uc.sha256_text(item[0]) != text_hash
                or (item[1] or {}).get("unit_id") != unit_id
            ):
                bad.append(chunk_id)
        say(progress_line(f"{label} sample", start + len(part), len(sample), started))
    if bad:
        return [
            f"{len(bad)} of {len(sample)} sampled chunks are missing or differ, e.g. {bad[:3]}"
        ]
    return []


def verify(
    corpus_id: str, data: VerifyInput, hashes: dict, client, ai_dir: Path, say=show
) -> list[str]:
    """Return the differences between the record and what is on disk; empty is good."""
    record = data.manifest.get(RECORD_KEY)
    if not record:
        return ["the manifest has no build record"]
    problems = list(data.problems)
    say("  step: corpus fingerprint and model files")
    if record["corpus_fingerprint"] != data.manifest["fingerprint"]:
        problems.append("the corpus changed since the build")
    for key, value in hashes.items():
        if record["model"].get(key) != value:
            problems.append(f"model file hash differs from the manifest: {key}")
    recorded_file = (record.get("vectors") or {}).get("sha256")
    if recorded_file and recorded_file != data.vector_digest:
        problems.append("the chunk file differs from the build")
    say("  step: build id")
    now = make_build_id(
        data.manifest["fingerprint"], hashes, data.chunks_digest, data.vector_digest
    )
    if record["build_id"] != now or record["chunk_count"] != data.chunk_total:
        problems.append("the chunks or the model differ from the build")
    name = record["collection"]
    say(f"  step: collection {name}")
    try:
        collection = client.get_collection(name, embedding_function=None)
    except Exception:  # any client error means the collection is unusable
        problems.append(f"collection {name} is missing")
    else:
        count = collection.count()
        say(
            f"  collection holds {count} chunks, the record says {record['chunk_count']}"
        )
        if count != record["chunk_count"]:
            problems.append(
                f"collection {name} holds {count} chunks, not {record['chunk_count']}"
            )
        problems += check_sample(collection, data.sample, corpus_id, say)
    say("  step: keyword file")
    keyword = ai_dir / record["keyword_index"]["file"]
    if (
        not keyword.is_file()
        or file_sha256_progress(keyword, "keyword file", say)
        != record["keyword_index"]["version"]
    ):
        problems.append("the keyword index file differs from the manifest")
    say("  step: graph")
    if data.graph_version != record["graph"]["version"]:
        problems.append("the graph differs from the manifest")
    return problems
