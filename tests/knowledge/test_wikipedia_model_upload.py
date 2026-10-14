"""Model upload -- plan, hashes file, force and the dry run.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A fake Files API stands in for Databricks; nothing is sent over the network.
"""

from __future__ import annotations

import json

import pytest

from ai.embeddings import embedder as emb
from scripts.knowledge import wikipedia_model_upload as mu

pytestmark = [pytest.mark.unit, pytest.mark.ai]


class FakeApi:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.sent: list[str] = []

    def size(self, path):
        return len(self.files[path]) if path in self.files else None

    def read_text(self, path):
        return self.files[path].decode() if path in self.files else None

    def make_dir(self, path):
        pass

    def upload(self, path, data):
        self.files[path] = data if isinstance(data, bytes) else data.read()
        self.sent.append(path.rsplit("/", 1)[-1])


@pytest.fixture
def model_dir(tmp_path):
    folder = tmp_path / "model" / "onnx"
    folder.mkdir(parents=True)
    (folder / emb.MODEL_FILE).write_bytes(b"model bytes")
    (folder / emb.TOKENIZER_FILE).write_bytes(b"tokenizer bytes")
    return folder


def run(api, model_dir, *extra):
    argv = ["--model-dir", str(model_dir), *extra]
    return mu.main(argv, api=api, make_volume=lambda: None)


def test_upload_sends_the_files_and_their_hashes_once(model_dir, capsys):
    api = FakeApi()
    assert run(api, model_dir, "--apply") == 0
    assert api.sent == [emb.MODEL_FILE, emb.TOKENIZER_FILE, mu.HASHES_NAME]
    hashes = json.loads(api.read_text(f"{mu.REMOTE_DIR}/{mu.HASHES_NAME}"))
    assert set(hashes) == {"model_onnx_sha256", "tokenizer_sha256"}
    assert hashes["model_onnx_sha256"] == emb.file_sha256(model_dir / emb.MODEL_FILE)
    api.sent.clear()
    assert run(api, model_dir, "--apply") == 0
    assert api.sent == []
    assert "Nothing to send" in capsys.readouterr().out


def test_a_changed_file_is_sent_again_with_new_hashes(model_dir):
    api = FakeApi()
    run(api, model_dir, "--apply")
    (model_dir / emb.TOKENIZER_FILE).write_bytes(b"a longer tokenizer file")
    api.sent.clear()
    run(api, model_dir, "--apply")
    assert api.sent == [emb.TOKENIZER_FILE, mu.HASHES_NAME]


def test_force_sends_all_and_dry_run_sends_nothing(model_dir):
    api = FakeApi()
    assert run(api, model_dir, "--dry-run") == 0
    assert api.sent == []
    run(api, model_dir, "--apply")
    api.sent.clear()
    run(api, model_dir, "--apply", "--force")
    assert len(api.sent) == 3


def test_missing_settings_stop_with_a_message(model_dir, monkeypatch, capsys):
    monkeypatch.setattr(mu, "file_settings", lambda env=None: None)
    assert mu.main(["--model-dir", str(model_dir), "--apply"]) == 1
    assert "DATABRICKS_TOKEN" in capsys.readouterr().out
