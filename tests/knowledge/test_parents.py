"""The stack file and the parent commands of the extension.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The Databricks connection, the Files API and the model download are replaced by fakes;
nothing is sent over the network.
"""

from __future__ import annotations

import argparse

import pytest

from ai.embeddings import embedder as emb
from ai.knowledge_quality import stack as qstack
from ai.rag.config import RetrievalConfig
from scripts.knowledge import run_all as ra
from scripts.knowledge import run_rerank as rk

pytestmark = [pytest.mark.unit, pytest.mark.ai]

GTE = emb.GTE_MODERNBERT.slug


def write_stack(ai_dir, corpus, text):
    path = qstack.stack_path(ai_dir, corpus)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_the_default_stack_has_the_second_embedder_for_the_curated_corpus_only(
    tmp_path,
):
    curated = qstack.load(tmp_path, "curated")
    assert curated["embedders"] == [GTE] and not curated["rerank"]
    assert not qstack.judge_on(curated)
    assert qstack.load(tmp_path, "wikipedia")["embedders"] == []


def test_the_stack_file_changes_the_defaults_and_refuses_unknown_names(tmp_path):
    write_stack(tmp_path, "curated", "embedders: []\nrerank: true\njudge_gate: true\n")
    stack = qstack.load(tmp_path, "curated")
    assert stack["embedders"] == [] and stack["rerank"] and qstack.judge_on(stack)
    write_stack(tmp_path, "curated", "reranker: true\n")
    with pytest.raises(ValueError, match="unknown key"):
        qstack.load(tmp_path, "curated")
    write_stack(tmp_path, "curated", "embedders: [other-model]\n")
    with pytest.raises(ValueError, match="unknown embedder"):
        qstack.load(tmp_path, "curated")
    write_stack(tmp_path, "wikipedia", f"embedders: [{GTE}]\n")
    with pytest.raises(ValueError, match="only the curated corpus"):
        qstack.load(tmp_path, "wikipedia")


def test_the_stack_is_applied_to_a_config_and_drops_the_threshold_of_a_removed_embedder():
    config = RetrievalConfig(embedders=(GTE,), extra_min_scores={GTE: 0.4, "gone": 0.1})
    stack = {
        **qstack.defaults("curated"),
        "judge_gate": True,
        "rerank": True,
        "rerank_candidates": 30,
    }
    applied = qstack.apply(config, stack)
    assert applied.embedders == (GTE,) and applied.extra_min_scores == {GTE: 0.4}
    assert applied.judge_gate and applied.rerank and applied.rerank_candidates == 30
    off = qstack.apply(applied, {**qstack.defaults("wikipedia")})
    assert off.embedders == () and off.extra_min_scores is None and not off.rerank


def run_all_args(tmp_path, apply=False):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    corpus_dir.mkdir(parents=True, exist_ok=True)
    return argparse.Namespace(corpus_dir=corpus_dir, apply=apply)


def test_run_all_builds_the_extra_embedders_that_the_stack_file_names(tmp_path):
    args = run_all_args(tmp_path)
    assert [p.slug for p in ra.wanted_embedders(args)] == [GTE]
    write_stack(tmp_path / "ai", "curated", "embedders: []\n")
    assert ra.wanted_embedders(args) == []


def test_the_download_step_plans_in_the_dry_pass_and_downloads_in_the_write_pass(
    tmp_path, monkeypatch, capsys
):
    missing = [("model.onnx", "onnx/model.onnx", 600_000_000, "x", "missing")]
    monkeypatch.setattr(ra.dl, "plan", lambda profile, folder: missing)
    assert ra.step_embedder_download(run_all_args(tmp_path)) == 0
    assert "600 MB to download in the write pass" in capsys.readouterr().out
    calls = []
    monkeypatch.setattr(ra.dl, "main", lambda argv: calls.append(argv) or 0)
    assert ra.step_embedder_download(run_all_args(tmp_path, apply=True)) == 0
    assert calls == [["--embedder", GTE, "--apply"]]
    present = [("model.onnx", "onnx/model.onnx", 600_000_000, "x", "ok")]
    monkeypatch.setattr(ra.dl, "plan", lambda profile, folder: present)
    calls.clear()
    assert ra.step_embedder_download(run_all_args(tmp_path, apply=True)) == 0
    assert calls == [] and "there and match" in capsys.readouterr().out


def test_the_model_files_are_ready_only_when_every_file_matches(monkeypatch):
    profile = emb.GTE_MODERNBERT
    monkeypatch.setattr(ra.dl, "plan", lambda p, folder: [("m", "r", 1, "s", "ok")])
    assert ra.model_files_ready(profile)
    monkeypatch.setattr(ra.dl, "plan", lambda p, folder: [("m", "r", 1, "s", "wrong")])
    assert not ra.model_files_ready(profile)


@pytest.fixture
def rerank_run(monkeypatch):
    calls = []
    monkeypatch.setattr(rk.wf, "connection_settings", lambda env=None: {"host": "h"})
    monkeypatch.setattr(
        rk.mu, "file_settings", lambda env=None: {"host": "h", "token": "t"}
    )
    monkeypatch.setattr(rk.mu, "FilesApi", lambda settings: object())
    monkeypatch.setattr(rk.rp, "cmd_fetch", lambda args: calls.append("fetch") or 0)
    monkeypatch.setattr(rk.rp, "cmd_export", lambda args: calls.append("export") or 0)
    monkeypatch.setattr(rk.rp, "cmd_upload", lambda args: calls.append("upload") or 0)
    monkeypatch.setattr(
        rk.fmu, "main", lambda argv, api=None: calls.append(("weights", argv)) or 0
    )
    return calls


def test_the_reranker_command_needs_the_databricks_settings(monkeypatch, capsys):
    monkeypatch.setattr(rk.wf, "connection_settings", lambda env=None: None)
    assert rk.main([]) == 1
    assert "Databricks settings" in capsys.readouterr().out


def test_the_reranker_command_stops_when_every_pair_has_a_score(
    rerank_run, monkeypatch, capsys
):
    monkeypatch.setattr(rk.rp, "pending_pairs", lambda args: (171, []))
    assert rk.main([]) == 0
    assert (
        rerank_run == ["fetch"] and "the reranker is ready" in capsys.readouterr().out
    )


def test_the_reranker_command_sends_weights_and_pairs_then_asks_for_the_notebook(
    rerank_run, monkeypatch, capsys
):
    monkeypatch.setattr(rk.rp, "pending_pairs", lambda args: (171, [{"key": "k"}]))
    monkeypatch.setattr(
        rk, "weights_to_send", lambda session=None, api=None: 91_000_000
    )
    assert rk.main([]) == 0
    assert rerank_run == [
        "fetch",
        ("weights", ["--model", rk.WEIGHTS_KEY]),
        "export",
        "upload",
    ]
    assert "01_rerank_scores" in capsys.readouterr().out
    rerank_run.clear()
    monkeypatch.setattr(rk, "weights_to_send", lambda session=None, api=None: 0)
    assert rk.main([]) == 0
    assert rerank_run == ["fetch", "export", "upload"]


def test_a_failed_step_stops_the_reranker_command(rerank_run, monkeypatch):
    monkeypatch.setattr(rk.rp, "pending_pairs", lambda args: (1, [{"key": "k"}]))
    monkeypatch.setattr(rk, "weights_to_send", lambda session=None, api=None: 0)
    monkeypatch.setattr(rk.rp, "cmd_export", lambda args: 3)
    assert rk.main([]) == 3
