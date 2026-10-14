"""SQLite FTS5 keyword index of the Wikipedia corpus: build, read, search.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The tokens of the 13b keyword file go into an FTS5 table on disk, ranked with BM25,
so the index does not have to fit in memory. The keyword file is read as a stream.
The filter columns come from the stored chunk metadata. The `meta` table holds the
build id and the SHA-256 of the keyword file; retrieval refuses an index that does
not match the build record. The build prints its plan and asks before writing
(y or yes); --apply writes without asking, --dry-run never writes.

Usage (repository root):
    python -m ai.rag.keyword_fts [--apply | --dry-run]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings.progress import progress_line, show
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc

CORPUS_ID = wc.CORPUS_ID
INDEX_NAME = "wikipedia.sqlite"
OK = "ok"
NO_TERMS = "no_terms"
READ_BLOCK = 1 << 20
PAGE = 5000
REPORT_EVERY = 10000
_IDS_START = '{"chunk_ids":'
_TOKENS_START = ',"tokens":['
_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE chunks (
    chunk_id TEXT NOT NULL UNIQUE, unit_id TEXT NOT NULL, kind TEXT NOT NULL,
    ecosystem TEXT NOT NULL, source TEXT NOT NULL, title TEXT NOT NULL,
    revision_year INTEGER NOT NULL
);
CREATE VIRTUAL TABLE tokens_fts USING fts5(
    body, content='', tokenize="unicode61 remove_diacritics 0 tokenchars '_'"
);
"""
_SEARCH = """
SELECT c.chunk_id, -bm25(tokens_fts) AS score
FROM tokens_fts CROSS JOIN chunks AS c ON c.rowid = tokens_fts.rowid
WHERE tokens_fts MATCH ?{where}
ORDER BY score DESC, c.chunk_id
LIMIT ?
"""


def index_path(ai_dir: Path) -> Path:
    return br.keyword_path(ai_dir, CORPUS_ID).with_name(INDEX_NAME)


def read_keyword_file(path: Path):
    """Yield the chunk id list, then the token list of each chunk, from a keyword file.

    The file is read in blocks, never as one string, and the buffer is cut only
    when a block is added, so a long file costs one pass.
    """
    decoder = json.JSONDecoder()
    with path.open(encoding="utf-8") as handle:
        buffer = handle.read(READ_BLOCK)
        while _TOKENS_START not in buffer:
            more = handle.read(READ_BLOCK)
            if not more:
                raise ValueError(f"{path}: not a keyword file")
            buffer += more
        head, _, buffer = buffer.partition(_TOKENS_START)
        if not head.startswith(_IDS_START):
            raise ValueError(f"{path}: not a keyword file")
        yield json.loads(head[len(_IDS_START) :])
        position = 0
        while True:
            while position < len(buffer) and buffer[position] in ", \n":
                position += 1
            if buffer.startswith("]", position):
                return
            try:
                tokens, position = decoder.raw_decode(buffer, position)
            except json.JSONDecodeError:
                more = handle.read(READ_BLOCK)
                if not more:
                    raise ValueError(f"{path}: file ends inside a list") from None
                buffer = buffer[position:] + more
                position = 0
                continue
            yield tokens


def metadata_by_id(collection, say=show) -> dict[str, tuple]:
    """Filter columns of every chunk, read page by page (no documents)."""
    total, started, rows = collection.count(), time.monotonic(), {}
    for offset in range(0, total, PAGE):
        page = collection.get(include=["metadatas"], limit=PAGE, offset=offset)
        for chunk_id, meta in zip(page["ids"], page["metadatas"], strict=True):
            try:
                rows[chunk_id] = (
                    meta["unit_id"],
                    meta["kind"],
                    meta["ecosystem"],
                    meta["source"],
                    meta["title"],
                    int(meta["revision_year"]),
                )
            except KeyError as error:
                raise ValueError(f"{chunk_id}: metadata has no {error}") from None
        say(progress_line("chunk metadata", len(rows), total, started))
    return rows


def write_index(
    path: Path, keyword_file: Path, collection, record: dict, file_hash: str, say=show
) -> int:
    """Build the index in a temporary file, then replace `path`; returns the rows."""
    rows = metadata_by_id(collection, say)
    temp = path.with_suffix(".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp.unlink(missing_ok=True)
    db = sqlite3.connect(temp)
    try:
        db.execute("PRAGMA journal_mode = OFF")
        db.execute("PRAGMA synchronous = OFF")
        db.executescript(_SCHEMA)
        reader = read_keyword_file(keyword_file)
        ids = next(reader)
        if len(ids) != len(rows) or set(ids) != set(rows):
            raise ValueError(
                "the keyword file and the collection hold different chunks"
            )
        started = time.monotonic()
        for number, (chunk_id, tokens) in enumerate(zip(ids, reader, strict=True), 1):
            db.execute(
                "INSERT INTO chunks(rowid, chunk_id, unit_id, kind, ecosystem, source,"
                " title, revision_year) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (number, chunk_id, *rows[chunk_id]),
            )
            db.execute(
                "INSERT INTO tokens_fts(rowid, body) VALUES (?, ?)",
                (number, " ".join(tokens)),
            )
            if number % REPORT_EVERY == 0:
                say(progress_line("fts index", number, len(ids), started))
        say(progress_line("fts index", len(ids), len(ids), started))
        meta = {
            "corpus_id": CORPUS_ID,
            "build_id": record["build_id"],
            "keyword_file_sha256": file_hash,
            "chunk_count": str(len(ids)),
        }
        db.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        db.commit()
    except BaseException:
        db.close()
        temp.unlink(missing_ok=True)
        raise
    db.close()
    temp.replace(path)
    return len(ids)


def read_meta(path: Path) -> dict:
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return dict(db.execute("SELECT key, value FROM meta"))
    finally:
        db.close()


def check_index(path: Path, record: dict, file_hash: str) -> str | None:
    """Why the index does not fit the build record, else None."""
    if not path.is_file():
        return "there is no FTS index file: run python -m ai.rag.keyword_fts"
    try:
        meta = read_meta(path)
    except sqlite3.Error as error:
        return f"the FTS index cannot be read ({error})"
    expected = {
        "build_id": record["build_id"],
        "keyword_file_sha256": file_hash,
        "chunk_count": str(record["chunk_count"]),
    }
    if any(meta.get(key) != value for key, value in expected.items()):
        return (
            "the FTS index belongs to another build: run python -m ai.rag.keyword_fts"
        )
    return None


class FtsKeyword:
    """Keyword retriever over the FTS5 index; the file is opened read-only."""

    def __init__(self, path: Path):
        self.db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)

    def search(
        self,
        terms: tuple[str, ...],
        filters: dict[str, list],
        k: int,
        min_score: float | None,
    ) -> tuple[str, list[tuple[str, float]]]:
        """Return (status, [(chunk id, score)]) best first; ties by chunk id."""
        if not terms:
            return NO_TERMS, []
        # Terms are `\w+` words, so a quoted term cannot hold a quote.
        match = " OR ".join(f'"{term}"' for term in terms)
        clauses, values = "", [match]
        for key, allowed in sorted(filters.items()):
            marks = ", ".join("?" * len(allowed))
            clauses += f" AND c.{key} IN ({marks})"
            values += allowed
        values.append(k)
        rows = self.db.execute(_SEARCH.format(where=clauses), values).fetchall()
        pairs = [(chunk_id, float(score)) for chunk_id, score in rows]
        if min_score is not None:
            pairs = [pair for pair in pairs if pair[1] >= min_score]
        return OK, pairs

    def close(self) -> None:
        self.db.close()


def prepare(corpus_dir: Path) -> tuple[dict, str, Path, Path]:
    """The build record and keyword file hash of the Wikipedia corpus."""
    ai_dir = corpus_dir.parent
    manifest = wc.read_previous(corpus_dir / CORPUS_ID)
    record = manifest.get(br.RECORD_KEY) if manifest else None
    if not record:
        raise ValueError(
            "the Wikipedia corpus has no build record: run the embedding build"
        )
    keyword = ai_dir / record["keyword_index"]["file"]
    if not keyword.is_file():
        raise FileNotFoundError(f"{keyword} not found: run the embedding build")
    show("  step: hash the keyword file")
    file_hash = emb.file_sha256(keyword)
    if file_hash != record["keyword_index"]["version"]:
        raise ValueError("the keyword file differs from the build record")
    return record, file_hash, keyword, index_path(ai_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    record, file_hash, keyword, path = prepare(args.corpus_dir)
    problem = check_index(path, record, file_hash)
    print(f"Corpus {CORPUS_ID}, build id {record['build_id']}")
    print(f"  keyword file {keyword.name}, {record['chunk_count']} chunks")
    print(f"  FTS index {path}: " + ("new build" if problem else "unchanged"))
    if not problem:
        print("Nothing to build.")
        return 0
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    chroma = br.chroma_dir(args.corpus_dir.parent)
    if not chroma.is_dir():
        raise FileNotFoundError(f"{chroma} not found: run the embedding build")
    collection = coll.open_client(chroma).get_collection(
        record["collection"], embedding_function=None
    )
    rows = write_index(path, keyword, collection, record, file_hash)
    print(f"FTS index written: {path} ({rows} chunks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
