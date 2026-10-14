"""Embedding function: the MiniLM ONNX model loaded from an explicit path.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loads `model.onnx` and `tokenizer.json` from a folder, never through the implicit
ChromaDB default. Text over 256 tokens raises an error and is never cut. Vectors
are mean-pooled and normalised, so cosine similarity applies. The SHA-256 of the
files is returned for the build manifest.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np

from scripts.knowledge import report_tokens as rt

MODEL_ID = "all-MiniLM-L6-v2"
DIMENSION = 384
MAX_TOKENS = rt.MAX_TOKENS
ARCHIVE_SHA256 = "913d7300ceae3b2dbc2c50d1de4baacab4be7b9380491c27fab7418616a16ec3"
DEFAULT_MODEL_DIR = rt.DEFAULT_TOKENIZER.parent
MODEL_FILE = "model.onnx"
TOKENIZER_FILE = "tokenizer.json"
ARCHIVE_FILE = "onnx.tar.gz"


def file_sha256(path: Path) -> str:
    # Binary hash: no line-ending change, unlike the unit file hash.
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def model_hashes(model_dir: Path = DEFAULT_MODEL_DIR) -> dict:
    """SHA-256 of the model files; the archive next to the folder is checked if present."""
    archive = model_dir.parent / ARCHIVE_FILE
    archive_hash = file_sha256(archive) if archive.is_file() else None
    if archive_hash not in (None, ARCHIVE_SHA256):
        raise ValueError(f"{archive}: SHA-256 differs from the pinned archive")
    return {
        "archive_sha256": archive_hash,
        "model_onnx_sha256": file_sha256(model_dir / MODEL_FILE),
        "tokenizer_sha256": file_sha256(model_dir / TOKENIZER_FILE),
    }


def token_counter(model_dir: Path = DEFAULT_MODEL_DIR):
    """A function that counts the tokens of a text, special tokens included."""
    tokenizer = rt.load_tokenizer(model_dir / TOKENIZER_FILE)
    return lambda text: rt.count_tokens(tokenizer, text)


class Embedder:
    def __init__(self, model_dir: Path = DEFAULT_MODEL_DIR):
        import onnxruntime

        self.tokenizer = rt.load_tokenizer(model_dir / TOKENIZER_FILE)
        self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
        # Batches differ in length; the memory arena would keep growing with them.
        options = onnxruntime.SessionOptions()
        options.enable_cpu_mem_arena = False
        options.enable_mem_pattern = False
        self.session = onnxruntime.InferenceSession(
            str(model_dir / MODEL_FILE), options, providers=["CPUExecutionProvider"]
        )

    def vectors_for(self, chunks) -> np.ndarray:
        return self.embed([chunk.text for chunk in chunks])

    def embed(self, texts: list[str], batch_size: int = 16) -> np.ndarray:
        """One normalised float32 vector per text, in the order of `texts`."""
        # Similar lengths share a batch, so little padding is computed.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        vectors = np.empty((len(texts), DIMENSION), dtype=np.float32)
        for start in range(0, len(order), batch_size):
            index = order[start : start + batch_size]
            encoded = self.tokenizer.encode_batch([texts[i] for i in index])
            if len(encoded[0].ids) > MAX_TOKENS:
                raise ValueError(f"a text has more than {MAX_TOKENS} tokens")
            ids = np.array([e.ids for e in encoded], dtype=np.int64)
            mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
            hidden = self.session.run(
                None,
                {
                    "input_ids": ids,
                    "attention_mask": mask,
                    "token_type_ids": np.zeros_like(ids),
                },
            )[0]
            weights = mask[:, :, None]
            pooled = (hidden * weights).sum(1) / np.clip(weights.sum(1), 1e-9, None)
            norm = np.linalg.norm(pooled, axis=1, keepdims=True)
            vectors[index] = pooled / np.where(norm == 0, 1e-12, norm)
        return vectors
