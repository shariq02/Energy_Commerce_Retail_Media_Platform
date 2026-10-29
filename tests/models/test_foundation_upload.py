"""Unit tests for the foundation model upload command.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

No network call is made: the Hub session and the Files API are small fakes.
"""

from __future__ import annotations

import pytest

from scripts.models import foundation_model_upload as up

pytestmark = [pytest.mark.unit]


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, shas):
        self.shas = shas
        self.urls = []

    def get(self, url, **_):
        self.urls.append(url)
        for repo, sha in self.shas.items():
            if f"/models/{repo}/revision/" in url:
                return _Response({"sha": sha})
        raise AssertionError(url)


def test_the_committed_configuration_is_complete_and_unpinned_models_are_found():
    models = up.load_config()
    assert {m["key"] for m in models} >= {"chronos2", "timesfm25", "ttm_r2"}
    for m in models:
        assert m["licence"] and m["include"] and m["ref"]


def test_a_model_key_may_appear_once(tmp_path):
    path = tmp_path / "models.yml"
    row = "- {key: a, repo_id: r, ref: main, licence: mit, include: ['*.json']}\n"
    path.write_text("models:\n" + row + row, encoding="utf-8")
    with pytest.raises(ValueError, match="twice"):
        up.load_config(path)


def test_files_are_chosen_by_pattern_and_sorted():
    tree = [
        {"type": "file", "path": "model.safetensors", "size": 5},
        {"type": "file", "path": "config.json", "size": 1},
        {"type": "file", "path": "notes.md", "size": 2},
        {"type": "directory", "path": "onnx"},
    ]
    chosen = up.select_files(tree, ["*.json", "*.safetensors"])
    assert [f["path"] for f in chosen] == ["config.json", "model.safetensors"]


def test_the_manifest_keeps_the_other_models_and_records_the_hashes():
    model = {"key": "a", "repo_id": "o/a", "licence": "mit"}
    first = up.merge_manifest(None, up.manifest_entry(model, "r1", {"f": "h1"}))
    second = up.merge_manifest(
        first,
        up.manifest_entry({**model, "key": "b"}, "r2", {"g": "h2"}),
    )
    assert sorted(second["models"]) == ["a", "b"]
    assert second["models"]["a"]["revision"] == "r1"
    assert second["models"]["b"]["files"] == {"g": "h2"}
    assert second["models"]["a"]["licence_restriction"] == "none"


def test_a_model_without_a_revision_gets_the_latest_of_its_ref():
    session = _Session({"o/a": "sha-a"})
    models = [
        {"key": "a", "repo_id": "o/a", "ref": "main", "revision": None},
        {"key": "b", "repo_id": "o/b", "ref": "main", "revision": "pinned"},
    ]
    found = up.pin_revisions(session, models)
    assert [(m["key"], m["latest"]) for m in found] == [("a", "sha-a")]


def test_a_file_is_sent_to_the_folder_of_its_model_and_revision():
    assert up.remote_dir("chronos2", "abc").endswith("/foundation_models/chronos2/abc")
