"""Versioned ChromaDB collections: create, fill, list, prune.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A collection is named `<corpus>_<build id>` and is never edited after the build.
The vectors come from our own embedding function, so the collection has none.
Distance is cosine. Library only.
"""

from __future__ import annotations

import time
from pathlib import Path

import chromadb

from ai.embeddings.chunks import Chunk
from ai.embeddings.progress import progress_line, show

ADD_BATCH = 256


def open_client(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(path))


def collection_name(corpus_id: str, build_id: str) -> str:
    return f"{corpus_id}_{build_id}"


def names(client, corpus_id: str) -> list[str]:
    prefix = f"{corpus_id}_"
    found = (c.name for c in client.list_collections())
    return sorted(name for name in found if name.startswith(prefix))


def fill(
    client, name: str, chunks: list[Chunk], source, label: str = "", progress=show
) -> None:
    """Create the collection and add every chunk; a failure deletes the partial one.

    `source.vectors_for(chunks)` gives the vectors: the local model or stored ones.
    """
    collection = client.create_collection(
        name, configuration={"hnsw": {"space": "cosine"}}, embedding_function=None
    )
    started = time.monotonic()
    try:
        for start in range(0, len(chunks), ADD_BATCH):
            batch = chunks[start : start + ADD_BATCH]
            vectors = source.vectors_for(batch)
            collection.add(
                ids=[chunk.chunk_id for chunk in batch],
                embeddings=vectors.tolist(),
                documents=[chunk.text for chunk in batch],
                metadatas=[chunk.metadata for chunk in batch],
            )
            done = start + len(batch)
            progress(progress_line(label or name, done, len(chunks), started))
    except BaseException:
        client.delete_collection(name)
        raise


def prune(client, corpus_id: str, keep: set[str]) -> list[str]:
    """Delete the collections of this corpus that are not in `keep`."""
    removed = [name for name in names(client, corpus_id) if name not in keep]
    for name in removed:
        client.delete_collection(name)
    return removed
