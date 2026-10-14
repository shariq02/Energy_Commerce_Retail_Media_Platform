# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # KNOWLEDGE -- WIKIPEDIA CHUNK AND EMBEDDING LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the chunk rules and the embedding of the selected Wikipedia
# MAGIC sections. A section that fits 256 tokens (the two special tokens included) is
# MAGIC one chunk; a longer one is split at sentence ends without overlap; a sentence
# MAGIC over the limit is left out and counted; link-list sections are dropped. The
# MAGIC vectors are mean-pooled and normalised, as in the local build. Pulled in with
# MAGIC `%run ./_wikipedia_chunks`. Definitions only -- no side effects at import.

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib
import json
import re

import numpy as np

# COMMAND ----------

# DBTITLE 1,Constants
MAX_TOKENS = 256
DIMENSION = 384
MODEL_FILE = "model.onnx"
TOKENIZER_FILE = "tokenizer.json"
# Lists of links and sources, not prose.
LINK_LIST_SECTIONS = frozenset(
    {
        "External links",
        "See also",
        "References",
        "Further reading",
        "Bibliography",
        "Sources",
        "Notes",
    }
)
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(\[])")

# COMMAND ----------

# DBTITLE 1,Hashes


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_sha256(path):
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def normalize_sections(raw):
    # Spark rows and numpy integers become the plain shapes of the local shards.
    return [
        {"index": int(s["index"]), "section": str(s["section"]), "text": str(s["text"])}
        for s in raw
    ]


def unit_hash(title, sections):
    # Same as scripts/knowledge/wikipedia_prepare.py: ties a chunk to the shard text.
    payload = json.dumps(
        {"title": title, "sections": sections},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return sha256_text(payload)


# COMMAND ----------

# DBTITLE 1,Chunk rules


def pack(parts, joiner, count, on_long):
    # Same rule as ai/embeddings/chunks.py: join consecutive parts while they fit.
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


def split_section(text, count, on_long):
    text = text.strip()
    if count(text) <= MAX_TOKENS:
        return [text]
    return pack(_SENTENCE_END.split(text), " ", count, on_long)


def chunk_article(article_id, title, sections, count):
    """Return (chunk rows, sentences left out, link-list sections dropped)."""
    content_hash = unit_hash(title, sections)
    rows, long_sentences, dropped = [], [], 0
    for section in sections:
        if section["section"] in LINK_LIST_SECTIONS:
            dropped += 1
            continue
        for text in split_section(section["text"], count, long_sentences.append):
            piece = len(rows)
            rows.append(
                {
                    "chunk_id": f"wikipedia.{article_id}#{piece}",
                    "article_id": int(article_id),
                    "piece": piece,
                    "section": section["section"],
                    "section_index": section["index"],
                    "text": text,
                    "token_count": count(text),
                    "text_hash": sha256_text(text),
                    "article_content_hash": content_hash,
                }
            )
    return rows, len(long_sentences), dropped


# COMMAND ----------

# DBTITLE 1,Model


def load_tokenizer(model_dir, padded=False):
    from tokenizers import Tokenizer

    tokenizer = Tokenizer.from_file(f"{model_dir}/{TOKENIZER_FILE}")
    tokenizer.no_truncation()
    tokenizer.no_padding()
    if padded:
        tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
    return tokenizer


def token_counter(tokenizer):
    return lambda text: len(tokenizer.encode(text).ids)


def load_session(model_dir):
    """The ONNX session on the GPU when one is there; returns (session, provider).

    TF32 is off, so the vectors keep full float32 precision like the local ones.
    """
    import onnxruntime

    # Loads the CUDA and cuDNN libraries of the pip packages before the session.
    if hasattr(onnxruntime, "preload_dlls"):
        onnxruntime.preload_dlls()
    session = onnxruntime.InferenceSession(
        f"{model_dir}/{MODEL_FILE}",
        providers=[("CUDAExecutionProvider", {"use_tf32": 0}), "CPUExecutionProvider"],
    )
    return session, session.get_providers()[0]


def embed_texts(session, tokenizer, texts, batch_size):
    """One normalised float32 vector per text, in the order of `texts`.

    Same steps as ai/embeddings/embedder.py; `tokenizer` must pad.
    """
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
    vectors = np.empty((len(texts), DIMENSION), dtype=np.float32)
    for start in range(0, len(order), batch_size):
        index = order[start : start + batch_size]
        encoded = tokenizer.encode_batch([texts[i] for i in index])
        if len(encoded[0].ids) > MAX_TOKENS:
            raise ValueError(f"a text has more than {MAX_TOKENS} tokens")
        ids = np.array([e.ids for e in encoded], dtype=np.int64)
        mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
        hidden = session.run(
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
