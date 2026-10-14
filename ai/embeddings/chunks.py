"""Corpus readers and the chunker for the embedding build.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Curated corpus: reads the approved units, checks them against the manifest and cuts
them into chunks of at most 256 tokens (the two special tokens included).
Wikipedia corpus: the chunks and vectors are made in Databricks and fetched to
chunks.parquet; this reads them and checks them against the approved shards.
Library only; the command is `ai.embeddings.build`.
"""

from __future__ import annotations

import json
import time
from collections import Counter as Tally
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from ai.embeddings import embedder as emb
from ai.embeddings.progress import progress_line
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import report_tokens as rt
from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_prepare as wp

CURATED_ID = "curated"
WIKIPEDIA_ID = wc.CORPUS_ID
MAX_TOKENS = rt.MAX_TOKENS
CHUNK_COLUMNS = (
    "chunk_id",
    "article_id",
    "piece",
    "section",
    "section_index",
    "text",
    "token_count",
    "text_hash",
    "article_content_hash",
    "model_onnx_sha256",
    "tokenizer_sha256",
)
VECTOR_READ_BATCH = 1024
# A progress line every this many batches of the chunk file.
CHUNK_REPORT_EVERY = 20

Counter = Callable[[str], int]


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    text: str
    metadata: dict


@dataclass
class Corpus:
    corpus_id: str
    mode: str
    manifest_path: Path
    manifest: dict
    chunks: list[Chunk]
    unit_count: int
    split_units: int
    # None for Wikipedia: the sentences left out are counted in Databricks.
    skipped: list[dict] | None = field(default_factory=list)
    units: list[uc.Unit] | None = None
    edges: dict | None = None
    # Wikipedia only: the fetched chunk file, its SHA-256 and the model file hashes.
    vector_file: Path | None = None
    vector_digest: str | None = None
    vector_model: dict | None = None
    # Wikipedia only: articles whose text was all left out in Databricks.
    without_chunks: int = 0


def chunk_id(unit_id: str, piece: int) -> str:
    return f"{unit_id}#{piece}"


def pack(parts: list[str], joiner: str, count: Counter, on_long) -> list[str]:
    """Join consecutive parts while the joined text fits; no overlap.

    A single part over the limit goes to `on_long` and is not packed.
    """
    pieces, current = [], ""
    for part in parts:
        if count(part) > MAX_TOKENS:
            on_long(part)
            continue
        candidate = f"{current}{joiner}{part}" if current else part
        if current and count(candidate) > MAX_TOKENS:
            pieces.append(current)
            candidate = part
        current = candidate
    if current:
        pieces.append(current)
    return pieces


def curated_chunks(unit: uc.Unit, count: Counter) -> list[Chunk]:
    """One chunk when the body fits, else paragraph packing; a long paragraph fails."""

    def paragraph_too_long(paragraph: str) -> None:
        raise ValueError(
            f"{unit.unit_id}: a paragraph has {count(paragraph)} tokens, "
            f"over the limit of {MAX_TOKENS}"
        )

    if count(unit.body) <= MAX_TOKENS:
        pieces = [unit.body]
    else:
        paragraphs = [part for part in unit.body.split("\n\n") if part.strip()]
        pieces = pack(paragraphs, "\n\n", count, paragraph_too_long)
    header = unit.header
    base = {
        "corpus": CURATED_ID,
        "unit_id": unit.unit_id,
        "kind": unit.kind,
        "ecosystem": header["ecosystem"],
        "source": ";".join(str(s.get("path") or s["name"]) for s in header["source"]),
        "section": unit.body.splitlines()[0].lstrip("# ").strip(),
        "version": int(header["version"]),
        "last_updated": str(header["last_updated"]),
        "content_hash": unit.content_hash,
    }
    return [
        Chunk(chunk_id(unit.unit_id, number), text, {**base, "piece": number})
        for number, text in enumerate(pieces)
    ]


def read_curated(corpus_dir: Path, count: Counter) -> Corpus:
    """Read the curated units; an unapproved unit or a manifest mismatch stops it."""
    units = uc.scan_units(corpus_dir)
    if not units:
        raise FileNotFoundError(f"no units in {corpus_dir}")
    waiting = [unit.unit_id for unit in units if unit.approval_status != "approved"]
    if waiting:
        raise ValueError(f"{len(waiting)} unit(s) are not approved, e.g. {waiting[:3]}")
    path = corpus_dir / uc.MANIFEST_NAME
    manifest = json.loads(path.read_text(encoding="utf-8"))
    current = uc.fingerprint(units)
    if manifest["unit_count"] != len(units) or manifest["fingerprint"] != current:
        raise ValueError("the units differ from the manifest: run the units step")
    chunks, split = [], 0
    for unit in units:
        pieces = curated_chunks(unit, count)
        split += len(pieces) > 1
        chunks.extend(pieces)
    return Corpus(
        corpus_id=CURATED_ID,
        mode="production",
        manifest_path=path,
        manifest=manifest,
        chunks=chunks,
        unit_count=len(units),
        split_units=split,
        units=units,
    )


def check_wikipedia_manifest(
    wiki_dir: Path, progress=None
) -> tuple[dict, list[dict], dict]:
    """The shards and edges must match the manifest, and the corpus be approved."""
    manifest = wc.read_previous(wiki_dir)
    if not manifest:
        raise FileNotFoundError(
            f"{wiki_dir / wc.MANIFEST_NAME} not found: run the Wikipedia corpus build"
        )
    articles, shards = wc.scan_shards(wiki_dir / wc.SHARD_DIR_NAME, progress)
    selection = wc.current_selection(wiki_dir, articles)
    edges = wc.scan_edges(wiki_dir / wc.EDGES_NAME, articles)
    if edges is None:
        raise FileNotFoundError(f"no edge file in {wiki_dir}")
    if (shards, selection, edges) != (
        manifest["shards"],
        manifest["selection"],
        manifest["edges"],
    ):
        raise ValueError("the shards or edges differ from the manifest")
    if wc.approval_status(manifest["approval"], selection) != "approved":
        raise ValueError("the Wikipedia corpus is not approved")
    return manifest, articles, edges


def wikipedia_chunk(row: dict, article: dict) -> Chunk:
    """One fetched chunk row; it must come from the approved article text."""
    where = row["chunk_id"]
    if row["article_content_hash"] != article["content_hash"]:
        raise ValueError(f"{where}: not cut from the approved article text")
    if row["token_count"] > MAX_TOKENS or row["text_hash"] != uc.sha256_text(
        row["text"]
    ):
        raise ValueError(f"{where}: over the token limit or text hash differs")
    stamp = str(article["revision_timestamp"])
    metadata = {
        "corpus": WIKIPEDIA_ID,
        "unit_id": article["unit_id"],
        "kind": article["kind"],
        "ecosystem": article["ecosystem"],
        "source": article["snapshot"],
        "section": row["section"],
        "version": int(article["version"]),
        "last_updated": stamp,
        "content_hash": article["content_hash"],
        "title": article["title"],
        "revision_year": int(stamp[:4]),
        "piece": int(row["piece"]),
    }
    return Chunk(where, row["text"], metadata)


def read_wikipedia(wiki_dir: Path, progress=None) -> Corpus:
    """Read the approved shards and the chunks made in Databricks; they must agree.

    The chunk file is read in batches, so only the chunks stay in memory. An
    article whose text was all left out has no chunk; it is counted, not an error.
    """
    import pyarrow.parquet as pq

    say = progress or (lambda line: None)
    say("  step: read and check the shards")
    manifest, articles, edges = check_wikipedia_manifest(wiki_dir, progress)
    chunk_file = wiki_dir / wc.CHUNKS_NAME
    if not chunk_file.is_file():
        raise FileNotFoundError(
            f"{chunk_file} not found: run notebook 05, then fetch the chunks part"
        )
    by_id = {int(a["unit_id"].removeprefix(wp.UNIT_PREFIX)): a for a in articles}
    chunks, seen, models = [], set(), set()
    pieces: Tally = Tally()
    sections: set[tuple[int, int]] = set()
    say("  step: read the chunk file and check every chunk against its article")
    parquet = pq.ParquetFile(chunk_file)
    total, started = parquet.metadata.num_rows, time.monotonic()
    for number, batch in enumerate(
        parquet.iter_batches(batch_size=VECTOR_READ_BATCH, columns=list(CHUNK_COLUMNS)),
        1,
    ):
        if number % CHUNK_REPORT_EVERY == 0:
            say(progress_line("chunk file", len(chunks), total, started, "rows"))
        for row in batch.to_pylist():
            article_id = int(row["article_id"])
            article = by_id.get(article_id)
            if article is None:
                raise ValueError(f"{row['chunk_id']}: its article is not selected")
            if row["chunk_id"] in seen:
                raise ValueError(f"{row['chunk_id']} appears twice in the chunk file")
            seen.add(row["chunk_id"])
            chunks.append(wikipedia_chunk(row, article))
            models.add((row["model_onnx_sha256"], row["tokenizer_sha256"]))
            pieces[article_id] += 1
            sections.add((article_id, row["section_index"]))
    say(progress_line("chunk file", len(chunks), total, started, "rows"))
    if not chunks:
        raise ValueError(f"{chunk_file} holds no chunk")
    if len(models) != 1:
        raise ValueError("the chunks come from more than one model")
    onnx_hash, tokenizer_hash = models.pop()
    per_article = Tally(article for article, _ in sections)
    split = sum(1 for article, n in pieces.items() if n > per_article[article])
    return Corpus(
        corpus_id=WIKIPEDIA_ID,
        mode=manifest["mode"],
        manifest_path=wiki_dir / wc.MANIFEST_NAME,
        manifest=manifest,
        chunks=chunks,
        unit_count=len(articles),
        split_units=split,
        skipped=None,
        edges=edges,
        vector_file=chunk_file,
        vector_digest=emb.file_sha256(chunk_file),
        vector_model={
            "model_onnx_sha256": onnx_hash,
            "tokenizer_sha256": tokenizer_hash,
        },
        without_chunks=len(by_id) - len(pieces),
    )


class StoredVectors:
    """The fetched vectors, handed out in file order; the chunk ids are checked."""

    def __init__(self, path: Path, dimension: int = emb.DIMENSION):
        import pyarrow.parquet as pq

        self.dimension = dimension
        self.batches = pq.ParquetFile(path).iter_batches(
            batch_size=VECTOR_READ_BATCH, columns=["chunk_id", "vector"]
        )
        self.ids: list[str] = []
        self.vectors = np.empty((0, dimension), dtype=np.float32)

    def _read(self, need: int) -> None:
        while len(self.ids) < need:
            batch = next(self.batches, None)
            if batch is None:
                raise ValueError("the chunk file holds fewer vectors than chunks")
            ids = batch.column(0).to_pylist()
            flat = batch.column(1).flatten().to_numpy(zero_copy_only=False)
            if flat.size != len(ids) * self.dimension:
                raise ValueError("a stored vector does not have the model size")
            self.ids += ids
            block = flat.astype(np.float32).reshape(-1, self.dimension)
            self.vectors = np.concatenate([self.vectors, block])

    def vectors_for(self, chunks: list[Chunk]) -> np.ndarray:
        self._read(len(chunks))
        if self.ids[: len(chunks)] != [chunk.chunk_id for chunk in chunks]:
            raise ValueError("the stored vectors are not in chunk order")
        out = self.vectors[: len(chunks)]
        self.ids = self.ids[len(chunks) :]
        self.vectors = self.vectors[len(chunks) :]
        return out


def stored_sample(path: Path, chunk_ids: list[str]) -> dict[str, np.ndarray]:
    """The stored vectors of a few chunks, by chunk id.

    Reads one row group at a time and picks the rows inside pyarrow, so only the
    wanted vectors become Python objects and memory stays at one row group.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    wanted = pa.array(chunk_ids, type=pa.string())
    found: dict[str, np.ndarray] = {}
    parquet = pq.ParquetFile(path)
    for group in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(group, columns=["chunk_id", "vector"])
        ids = table.column("chunk_id")
        rows = table.filter(pc.is_in(ids, value_set=wanted.cast(ids.type)))
        for cid, vec in zip(
            rows.column(0).to_pylist(), rows.column(1).to_pylist(), strict=True
        ):
            found[cid] = np.asarray(vec, dtype=np.float32)
        if len(found) == len(chunk_ids):
            break
    return found
