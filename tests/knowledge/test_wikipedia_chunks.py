"""Wikipedia chunk library (Databricks notebook library) -- chunk rules and pooling.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The notebook library is loaded the way %run loads it. Tokens are counted as words
and the model is a fake; nothing is read from Databricks.
"""

from __future__ import annotations

import numpy as np
import pytest

from ai.embeddings import chunks as local_chunks
from scripts.knowledge import wikipedia_prepare as wp
from tests._notebook_loader import load_wikipedia_chunks

pytestmark = [pytest.mark.unit, pytest.mark.ai]

LIB = load_wikipedia_chunks()


def words(text):
    return len(text.split())


@pytest.fixture
def small_limit(monkeypatch):
    monkeypatch.setitem(LIB, "MAX_TOKENS", 5)


def test_pack_is_the_same_rule_as_the_local_build(monkeypatch):
    monkeypatch.setitem(LIB, "MAX_TOKENS", 4)
    monkeypatch.setattr(local_chunks, "MAX_TOKENS", 4)
    parts = ["a b", "c d", "e f g h i j", "k"]
    here, there = [], []
    assert LIB["pack"](parts, " ", words, here.append) == local_chunks.pack(
        parts, " ", words, there.append
    )
    assert here == there == ["e f g h i j"]


def test_long_section_splits_at_sentences_and_a_long_sentence_is_left_out(small_limit):
    sections = [
        {"index": 0, "section": "Lead", "text": "One two. Three four. Five six."},
        {"index": 1, "section": "Long", "text": "Short one. Long " + "w " * 7 + "end."},
        {"index": 2, "section": "See also", "text": "A link."},
    ]
    rows, left_out, dropped = LIB["chunk_article"](7, "Wind", sections, words)
    assert [r["text"] for r in rows] == [
        "One two. Three four.",
        "Five six.",
        "Short one.",
    ]
    assert [r["chunk_id"] for r in rows] == [f"wikipedia.7#{n}" for n in range(3)]
    assert [r["section_index"] for r in rows] == [0, 0, 1]
    assert left_out == 1 and dropped == 1


def test_article_hash_matches_the_local_shard_hash():
    raw = [{"index": np.int64(0), "section": "Lead", "text": "Wind power."}]
    sections = LIB["normalize_sections"](raw)
    assert type(sections[0]["index"]) is int
    assert LIB["unit_hash"]("Wind", sections) == wp.unit_hash("Wind", sections)
    rows, _, _ = LIB["chunk_article"](3, "Wind", sections, words)
    assert rows[0]["article_content_hash"] == wp.unit_hash("Wind", sections)
    assert rows[0]["text_hash"] == LIB["sha256_text"]("Wind power.")


class Encoding:
    def __init__(self, ids, mask):
        self.ids, self.attention_mask = ids, mask


class PaddingTokenizer:
    """One token per word, padded to the longest text of the batch."""

    def encode_batch(self, texts):
        width = max(words(text) for text in texts) + 2
        out = []
        for text in texts:
            n = words(text) + 2
            out.append(
                Encoding([1] * n + [0] * (width - n), [1] * n + [0] * (width - n))
            )
        return out


class Session:
    """Hidden state 1 for real tokens and 100 for padding: pooling must skip padding."""

    def run(self, _, feeds):
        mask = feeds["attention_mask"]
        hidden = np.where(mask[:, :, None] == 1, 1.0, 100.0)
        return [np.repeat(hidden, LIB["DIMENSION"], axis=2).astype(np.float32)]


def test_embedding_skips_padding_and_keeps_the_text_order():
    texts = ["a b c d e f", "a", "a b"]
    vectors = LIB["embed_texts"](Session(), PaddingTokenizer(), texts, batch_size=2)
    assert vectors.shape == (3, LIB["DIMENSION"])
    expected = np.full(LIB["DIMENSION"], 1 / np.sqrt(LIB["DIMENSION"]))
    assert np.allclose(vectors, expected)
    assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0)
