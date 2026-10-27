"""Embedder profiles, the generic embedder and the model download.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The ONNX session and the tokenizer are replaced by small fakes; no model file is read
and nothing is sent over the network.
"""

from __future__ import annotations

import hashlib
import sys
import types

import numpy as np
import pytest

from ai.embeddings import embedder as emb
from scripts.knowledge import embedder_model_download as dl

pytestmark = [pytest.mark.unit, pytest.mark.ai]


class FakeEncoding:
    def __init__(self, ids):
        self.ids = ids
        self.attention_mask = [1] * len(ids)


class FakeTokenizer:
    """One token per word; padding is added by the test helper."""

    def __init__(self, pad_id=7):
        self.pad_id = pad_id
        self.padding = None

    def token_to_id(self, token):
        return self.pad_id if token == "[PAD]" else None

    def enable_padding(self, pad_id, pad_token):
        self.padding = (pad_id, pad_token)

    def encode_batch(self, texts):
        words = [text.split() for text in texts]
        width = max(len(w) for w in words)
        out = []
        for w in words:
            enc = FakeEncoding([1] * len(w) + [self.pad_id] * (width - len(w)))
            enc.attention_mask = [1] * len(w) + [0] * (width - len(w))
            out.append(enc)
        return out


class FakeSession:
    def __init__(self, names, dimension):
        self.names = names
        self.dimension = dimension
        self.fed = None

    def get_inputs(self):
        return [types.SimpleNamespace(name=n) for n in self.names]

    def run(self, _outputs, feed):
        self.fed = feed
        batch, width = feed["input_ids"].shape
        # token t of a text has the value t + 1 in every dimension
        hidden = np.tile(
            (np.arange(width, dtype=np.float32) + 1)[None, :, None],
            (batch, 1, self.dimension),
        )
        return [hidden]


@pytest.fixture
def fake_runtime(monkeypatch):
    holder = {}

    def make_session(path, options, providers):
        holder["session"] = FakeSession(holder["names"], holder["dimension"])
        return holder["session"]

    runtime = types.SimpleNamespace(
        SessionOptions=lambda: types.SimpleNamespace(),
        InferenceSession=make_session,
    )
    monkeypatch.setitem(sys.modules, "onnxruntime", runtime)
    monkeypatch.setattr(emb.rt, "load_tokenizer", lambda path: FakeTokenizer())
    return holder


def test_the_minilm_profile_keeps_the_module_constants():
    profile = emb.MINILM
    assert (profile.id, profile.dimension) == (emb.MODEL_ID, emb.DIMENSION)
    assert profile.max_tokens == emb.MAX_TOKENS and profile.pooling == "mean"
    assert profile.source is None and not profile.files
    assert emb.default_model_dir(profile) == emb.DEFAULT_MODEL_DIR


def test_the_gte_profile_is_pinned_to_a_commit_and_to_file_hashes():
    profile = emb.GTE_MODERNBERT
    assert (profile.dimension, profile.max_tokens, profile.pooling) == (
        768,
        8192,
        "cls",
    )
    assert len(profile.revision) == 40
    assert set(profile.files) == {emb.MODEL_FILE, emb.TOKENIZER_FILE}
    for remote, size, sha256 in profile.files.values():
        assert remote and size > 0 and len(sha256) == 64
    assert emb.default_model_dir(profile).parts[-3:] == (
        ".cache",
        "ecrmap",
        "models",
    ) or (emb.default_model_dir(profile).name == profile.id)
    assert set(emb.PROFILES) == {"minilm", "gte-modernbert-base"}


def test_mean_pooling_averages_the_real_tokens_and_normalises(fake_runtime):
    fake_runtime.update(
        names=["input_ids", "attention_mask", "token_type_ids"], dimension=384
    )
    embedder = emb.Embedder()
    vectors = embedder.embed(["a b c", "a"])
    assert vectors.shape == (2, 384)
    assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0, atol=1e-5)
    assert "token_type_ids" in fake_runtime["session"].fed
    assert embedder.tokenizer.padding == (7, "[PAD]")


def test_cls_pooling_takes_the_first_token_and_sends_no_token_types(fake_runtime):
    fake_runtime.update(names=["input_ids", "attention_mask"], dimension=768)
    embedder = emb.Embedder(profile=emb.GTE_MODERNBERT)
    vectors = embedder.embed(["a b c"])
    assert vectors.shape == (1, 768)
    assert np.allclose(vectors[0], 1 / np.sqrt(768), atol=1e-6)
    assert set(fake_runtime["session"].fed) == {"input_ids", "attention_mask"}


def test_an_unknown_model_input_stops_with_the_input_names(fake_runtime):
    fake_runtime.update(names=["input_ids", "position_ids"], dimension=768)
    with pytest.raises(ValueError, match="position_ids"):
        emb.Embedder(profile=emb.GTE_MODERNBERT)


def test_a_text_over_the_token_limit_is_never_cut(fake_runtime):
    fake_runtime.update(names=["input_ids", "attention_mask"], dimension=384)
    embedder = emb.Embedder()
    with pytest.raises(ValueError, match="more than 256 tokens"):
        embedder.embed([" ".join(["w"] * 300)])


def test_prefixes_are_added_to_documents_and_queries_once(fake_runtime):
    fake_runtime.update(names=["input_ids", "attention_mask"], dimension=4)
    profile = emb.Profile(
        id="p",
        slug="p",
        dimension=4,
        max_tokens=50,
        pooling="cls",
        pad_token="[PAD]",
        query_prefix="query: ",
        document_prefix="passage: ",
    )
    embedder = emb.Embedder(profile=profile)
    embedder.embed(["x"])
    assert fake_runtime["session"].fed["input_ids"].shape == (1, 2)
    embedder.embed_queries(["x"])
    assert fake_runtime["session"].fed["input_ids"].shape == (1, 2)


def test_pinned_file_hashes_stop_a_changed_file(tmp_path, monkeypatch):
    profile = emb.Profile(
        id="p",
        slug="p",
        dimension=4,
        max_tokens=8,
        pooling="cls",
        pad_token="[PAD]",
        files={
            emb.MODEL_FILE: ("m", 1, hashlib.sha256(b"model").hexdigest()),
            emb.TOKENIZER_FILE: ("t", 1, hashlib.sha256(b"tokens").hexdigest()),
        },
    )
    (tmp_path / emb.MODEL_FILE).write_bytes(b"model")
    (tmp_path / emb.TOKENIZER_FILE).write_bytes(b"tokens")
    found = emb.profile_hashes(profile, tmp_path)
    assert found["model_onnx_sha256"] == hashlib.sha256(b"model").hexdigest()
    (tmp_path / emb.TOKENIZER_FILE).write_bytes(b"other")
    with pytest.raises(ValueError, match="differs from the pinned file"):
        emb.profile_hashes(profile, tmp_path)


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, block):
        for start in range(0, len(self.data), 4):
            yield self.data[start : start + 4]


class FakeSessionHttp:
    def __init__(self, files):
        self.files = files
        self.urls = []

    def get(self, url, stream, timeout):
        self.urls.append(url)
        return FakeResponse(self.files[url.rsplit("/", 1)[-1]])


@pytest.fixture
def small_profile(monkeypatch):
    data = {"model.onnx": b"model bytes", "tokenizer.json": b"token bytes"}
    profile = emb.Profile(
        id="small",
        slug="small",
        dimension=4,
        max_tokens=8,
        pooling="cls",
        pad_token="[PAD]",
        source="owner/small",
        revision="r" * 40,
        files={
            name: (name, len(body), hashlib.sha256(body).hexdigest())
            for name, body in data.items()
        },
    )
    monkeypatch.setitem(emb.PROFILES, "small", profile)
    return profile, data


def test_the_download_fetches_missing_files_and_checks_size_and_hash(
    small_profile, tmp_path, capsys
):
    _profile, data = small_profile
    session = FakeSessionHttp(data)
    argv = ["--embedder", "small", "--model-dir", str(tmp_path), "--apply"]
    assert dl.main(argv, session=session) == 0
    assert (tmp_path / "model.onnx").read_bytes() == data["model.onnx"]
    assert not list(tmp_path.glob("*.part"))
    assert session.urls[0] == (f"{dl.HUB}/owner/small/resolve/{'r' * 40}/model.onnx")
    session.urls.clear()
    assert dl.main(argv, session=session) == 0
    assert session.urls == [] and "Nothing to fetch" in capsys.readouterr().out


def test_a_download_with_a_wrong_hash_is_not_kept(small_profile, tmp_path):
    _profile, data = small_profile
    session = FakeSessionHttp({**data, "model.onnx": b"model BYTES"})
    argv = ["--embedder", "small", "--model-dir", str(tmp_path), "--apply"]
    with pytest.raises(ValueError, match="differs from the pin"):
        dl.main(argv, session=session)
    assert not (tmp_path / "model.onnx").exists()
    assert not list(tmp_path.glob("*.part"))


def test_the_download_dry_run_writes_nothing(small_profile, tmp_path):
    _profile, data = small_profile
    session = FakeSessionHttp(data)
    argv = ["--embedder", "small", "--model-dir", str(tmp_path / "x"), "--dry-run"]
    assert dl.main(argv, session=session) == 0
    assert session.urls == [] and not (tmp_path / "x").exists()
