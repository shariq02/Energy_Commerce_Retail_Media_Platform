"""Embedding function: an ONNX model loaded from an explicit path.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Loads `model.onnx` and `tokenizer.json` from a folder, never through the implicit
ChromaDB default. A text over the token limit of the profile raises an error and is
never cut. Vectors are pooled as the profile says (mean or first token) and
normalised, so cosine similarity applies. The SHA-256 of the files is returned for
the build manifest. The module constants are the MiniLM profile; `PROFILES` holds
the others.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
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
KNOWN_INPUTS = ("input_ids", "attention_mask", "token_type_ids")


@dataclass(frozen=True)
class Profile:
    """What differs between embedding models. Files are named as in the folder."""

    id: str
    slug: str
    dimension: int
    max_tokens: int
    pooling: str  # "mean" or "cls"
    pad_token: str
    query_prefix: str = ""
    document_prefix: str = ""
    source: str | None = None  # Hugging Face repository
    revision: str | None = None  # commit of the repository
    # name in the folder -> (path in the repository, size, SHA-256)
    files: dict = field(default_factory=dict)


MINILM = Profile(
    id=MODEL_ID,
    slug="minilm",
    dimension=DIMENSION,
    max_tokens=MAX_TOKENS,
    pooling="mean",
    pad_token="[PAD]",
)
# Pins read from the Hub API (tree of the commit) and from the downloaded
# tokenizer file on 2026-10-27; the model is the fp32 file of the repository.
GTE_MODERNBERT = Profile(
    id="gte-modernbert-base",
    slug="gte-modernbert-base",
    dimension=768,
    max_tokens=8192,
    pooling="cls",
    pad_token="[PAD]",
    source="Alibaba-NLP/gte-modernbert-base",
    revision="e7f32e3c00f91d699e8c43b53106206bcc72bb22",
    files={
        MODEL_FILE: (
            "onnx/model.onnx",
            596392315,
            "947f31df7effaeec4edb57c50e4ed7e0f2034d9336063f92615b92e3e0d24d78",
        ),
        TOKENIZER_FILE: (
            "tokenizer.json",
            3583228,
            "6c8aaa9a542084f2457eab775d4eeb51f92a70c0fd9de28d5edb0ddec3c08d30",
        ),
    },
)
PROFILES = {profile.slug: profile for profile in (MINILM, GTE_MODERNBERT)}


def default_model_dir(profile: Profile = MINILM) -> Path:
    """MiniLM keeps its folder in the ChromaDB cache; the others use `~/.cache/ecrmap`."""
    if profile.slug == MINILM.slug:
        return DEFAULT_MODEL_DIR
    return Path.home() / ".cache" / "ecrmap" / "models" / profile.id


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


def profile_hashes(profile: Profile, model_dir: Path | None = None) -> dict:
    """SHA-256 of the files of a profile; a pinned file that differs stops here."""
    if profile.slug == MINILM.slug:
        return model_hashes(model_dir or DEFAULT_MODEL_DIR)
    folder = model_dir or default_model_dir(profile)
    found = {}
    for name, (_remote, _size, pinned) in profile.files.items():
        digest = file_sha256(folder / name)
        if digest != pinned:
            raise ValueError(f"{folder / name}: SHA-256 differs from the pinned file")
        found[name] = digest
    return {
        "archive_sha256": None,
        "model_onnx_sha256": found[MODEL_FILE],
        "tokenizer_sha256": found[TOKENIZER_FILE],
    }


def token_counter(model_dir: Path = DEFAULT_MODEL_DIR):
    """A function that counts the tokens of a text, special tokens included."""
    tokenizer = rt.load_tokenizer(model_dir / TOKENIZER_FILE)
    return lambda text: rt.count_tokens(tokenizer, text)


def profile_embedder(profile: Profile, model_dir: Path | None = None):
    return Embedder(model_dir or default_model_dir(profile), profile)


class Embedder:
    def __init__(self, model_dir: Path = DEFAULT_MODEL_DIR, profile: Profile = MINILM):
        import onnxruntime

        self.profile = profile
        self.tokenizer = rt.load_tokenizer(model_dir / TOKENIZER_FILE)
        pad_id = self.tokenizer.token_to_id(profile.pad_token)
        if pad_id is None:
            raise ValueError(f"the tokenizer has no {profile.pad_token} token")
        self.tokenizer.enable_padding(pad_id=pad_id, pad_token=profile.pad_token)
        # Batches differ in length; the memory arena would keep growing with them.
        options = onnxruntime.SessionOptions()
        options.enable_cpu_mem_arena = False
        options.enable_mem_pattern = False
        self.session = onnxruntime.InferenceSession(
            str(model_dir / MODEL_FILE), options, providers=["CPUExecutionProvider"]
        )
        # The input names are read from the session, never assumed.
        self.input_names = [item.name for item in self.session.get_inputs()]
        unknown = set(self.input_names) - set(KNOWN_INPUTS)
        if unknown:
            raise ValueError(
                f"the model has inputs that are not handled: {sorted(unknown)} "
                f"(inputs: {self.input_names})"
            )

    def vectors_for(self, chunks) -> np.ndarray:
        return self.embed([chunk.text for chunk in chunks])

    def embed_queries(self, texts: list[str]) -> np.ndarray:
        """Vectors of search queries: the query prefix of the profile first."""
        prefix = self.profile.query_prefix
        return self.embed([prefix + text for text in texts], prefixed=True)

    def embed(
        self, texts: list[str], batch_size: int = 16, prefixed: bool = False
    ) -> np.ndarray:
        """One normalised float32 vector per text, in the order of `texts`."""
        profile = self.profile
        if profile.document_prefix and not prefixed:
            texts = [profile.document_prefix + text for text in texts]
        # Similar lengths share a batch, so little padding is computed.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        vectors = np.empty((len(texts), profile.dimension), dtype=np.float32)
        for start in range(0, len(order), batch_size):
            index = order[start : start + batch_size]
            encoded = self.tokenizer.encode_batch([texts[i] for i in index])
            if len(encoded[0].ids) > profile.max_tokens:
                raise ValueError(f"a text has more than {profile.max_tokens} tokens")
            ids = np.array([e.ids for e in encoded], dtype=np.int64)
            mask = np.array([e.attention_mask for e in encoded], dtype=np.int64)
            feed = {"input_ids": ids, "attention_mask": mask}
            if "token_type_ids" in self.input_names:
                feed["token_type_ids"] = np.zeros_like(ids)
            hidden = self.session.run(None, feed)[0]
            if profile.pooling == "cls":
                pooled = hidden[:, 0, :]
            else:
                weights = mask[:, :, None]
                pooled = (hidden * weights).sum(1) / np.clip(weights.sum(1), 1e-9, None)
            norm = np.linalg.norm(pooled, axis=1, keepdims=True)
            vectors[index] = pooled / np.where(norm == 0, 1e-12, norm)
        return vectors
