"""Keyword index over the chunks (BM25).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The stored file holds the chunk ids and their tokens. The BM25 index is built in
memory when the file is loaded. Tokens are lowercase `\\w+` words; there are no
stop words and no stemming. Library only.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path

from ai.embeddings.chunks import Chunk
from ai.embeddings.progress import progress_line

_WORD = re.compile(r"\w+")
REPORT_EVERY = 10000


def tokenize(text: str) -> list[str]:
    return _WORD.findall(text.lower())


def write_index(path: Path, chunks: list[Chunk], progress=None) -> str:
    """Write the tokenised chunks as one JSON object; returns the SHA-256 of the file.

    The file is written piece by piece, so the tokens of all chunks are never in
    memory at once.
    """
    digest = hashlib.sha256()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as handle:

        def put(text: str) -> None:
            handle.write(text)
            digest.update(text.encode("utf-8"))

        ids = [chunk.chunk_id for chunk in chunks]
        put(
            '{"chunk_ids":' + json.dumps(ids, ensure_ascii=False, separators=(",", ":"))
        )
        put(',"tokens":[')
        started = time.monotonic()
        for number, chunk in enumerate(chunks):
            tokens = json.dumps(
                tokenize(chunk.text), ensure_ascii=False, separators=(",", ":")
            )
            put(("," if number else "") + tokens)
            if progress and (number + 1) % REPORT_EVERY == 0:
                progress(
                    progress_line("keyword file", number + 1, len(chunks), started)
                )
        put("]}\n")
        if progress:
            progress(progress_line("keyword file", len(chunks), len(chunks), started))
    temp.replace(path)
    return digest.hexdigest()


def load_index(path: Path):
    """Return (chunk ids, BM25 index) built from the stored file."""
    from rank_bm25 import BM25Okapi

    data = json.loads(path.read_text(encoding="utf-8"))
    return data["chunk_ids"], BM25Okapi(data["tokens"])
