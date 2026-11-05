"""The local judge -- fitting, the answers, the factory and the training data.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The data is synthetic and seeded; nothing is read from a collection or sent anywhere.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from ai.judge import factory, fit, training
from ai.judge.local import LocalJudge
from ai.knowledge_quality import evaluation
from ai.rag import retrieve as rv
from ai.rag.config import RetrievalConfig

pytestmark = [pytest.mark.unit, pytest.mark.ai]


def separable(n_positive=40, n_negative=40, seed=1):
    rng = np.random.default_rng(seed)
    scores = np.concatenate(
        [rng.normal(2.0, 0.5, n_positive), rng.normal(-2.0, 0.5, n_negative)]
    )
    labels = np.concatenate([np.ones(n_positive), np.zeros(n_negative)])
    return scores, labels


def test_platt_scaling_orders_scores_and_is_calibrated_on_separable_data():
    scores, labels = separable()
    found = fit.fit_platt(scores, labels)
    assert found["a"] > 0 and found["auc"] == 1.0 and found["log_loss"] < 0.3
    p = fit.sigmoid(found["a"] * np.array([2.0, -2.0]) + found["b"])
    assert p[0] > 0.9 and p[1] < 0.1


def test_a_part_is_refused_when_the_labelled_data_is_too_small():
    scores, labels = separable(10, 10)
    with pytest.raises(fit.NotEnoughData, match="at least 30"):
        fit.fit_platt(scores, labels)
    rows = [{"dense": 0.5}] * 12 + [{"dense": 0.1}] * 5
    with pytest.raises(fit.NotEnoughData, match="at least"):
        fit.fit_gate(rows, [1] * 12 + [0] * 5, minimum_positive=30)


def gate_rows(seed=2):
    rng = np.random.default_rng(seed)
    positive = [
        {"dense": float(rng.normal(0.6, 0.05)), "keyword": float(rng.normal(8, 1))}
        for _ in range(40)
    ]
    negative = [
        {"dense": float(rng.normal(0.25, 0.05)), "keyword": None} for _ in range(15)
    ]
    return positive + negative, [1] * 40 + [0] * 15


def test_the_gate_separates_questions_the_corpus_can_answer_from_those_it_cannot():
    rows, labels = gate_rows()
    gate = fit.fit_gate(rows, labels)
    assert gate["features"] == ["dense", "keyword"] and gate["auc"] == 1.0
    judge = LocalJudge({"gate": gate})
    high = judge.answerable("q", "ctx", {"dense": 0.62, "keyword": 8.0})
    low = judge.answerable("q", "ctx", {"dense": 0.22, "keyword": None})
    assert high > 0.8 and low < 0.2
    assert judge.answerable("q", "ctx", {}) is not None


def unit(vector):
    vector = np.asarray(vector, dtype=float)
    return vector / np.linalg.norm(vector)


def test_the_router_picks_the_ecosystem_of_the_nearest_centroid_and_needs_two():
    centroids = {
        "energy": np.array([1.0, 0.0, 0.0]),
        "commerce": np.array([0.0, 1.0, 0.0]),
    }
    rng = np.random.default_rng(3)
    vectors = np.array(
        [unit(centroids["energy"] + rng.normal(0, 0.1, 3)) for _ in range(8)]
        + [unit(centroids["commerce"] + rng.normal(0, 0.1, 3)) for _ in range(8)]
    )
    labels = ["energy"] * 8 + ["commerce"] * 8
    router = fit.fit_router(centroids, vectors, labels)
    assert router["labels"] == ["commerce", "energy"] and router["accuracy"] == 1.0
    judge = LocalJudge({"router": router}, embed=lambda texts: [[1.0, 0.05, 0.0]])
    assert judge.area_filter("a power question", 0.7) == ["energy", "shared"]
    flat = LocalJudge({"router": router}, embed=lambda texts: [[1.0, 1.0, 0.0]])
    assert flat.area_filter("unclear", 0.99) is None
    assert LocalJudge({"router": router}).area_filter("no embedder", 0.5) is None
    with pytest.raises(fit.NotEnoughData):
        fit.fit_router({"energy": centroids["energy"]}, vectors, labels)


def test_a_part_that_was_not_fitted_gives_no_answer_and_the_summary_lists_the_parts():
    judge = LocalJudge({"gate": {"features": []}})
    assert judge.relevance("q", "text", 1.0) is None
    assert judge.relevance("q", "text", None) is None
    assert judge.area_filter("q", 0.5) is None and judge.parts() == ["gate"]
    assert judge.summary() == {"backend": "local", "parts": ["gate"]}
    scores, labels = separable()
    relevant = LocalJudge({"relevance": fit.fit_platt(scores, labels)})
    assert relevant.relevance("q", "t", 2.0) > relevant.relevance("q", "t", -2.0)


def test_the_parameters_are_stored_per_build_and_refused_for_another_one(tmp_path):
    body = {"build_id": "b1", "corpus_id": "curated", "gate": {"features": []}}
    path = factory.save_params(tmp_path, "curated", "b1", body)
    assert (
        path.name == factory.PARAMS_FILE
        and factory.load_params(tmp_path, "curated", "b1") == body
    )
    assert factory.load_params(tmp_path, "curated", "b2") is None
    other = factory.params_path(tmp_path, "curated", "b3")
    other.parent.mkdir(parents=True)
    other.write_text('{"build_id": "b1"}', encoding="utf-8")
    with pytest.raises(ValueError, match="another build"):
        factory.load_params(tmp_path, "curated", "b3")


def test_a_judge_is_made_only_for_a_switch_and_only_with_fitted_parameters(tmp_path):
    off = RetrievalConfig()
    assert factory.make_judge(off, tmp_path, "curated", "b1") is None
    on = RetrievalConfig(judge_gate=True)
    assert factory.make_judge(on, tmp_path, "curated", "b1") is None
    factory.save_params(
        tmp_path, "curated", "b1", {"build_id": "b1", "gate": {"features": []}}
    )
    assert isinstance(factory.make_judge(on, tmp_path, "curated", "b1"), LocalJudge)


def outcome(question_id, expected, top, rerank=None):
    return evaluation.Outcome(
        question_id,
        "definitional_wording",
        "metric" if expected else None,
        expected,
        "tune",
        top_score=top,
        notes={"rerank": {"scores": rerank}} if rerank else {},
    )


def test_the_training_data_comes_from_the_labels_of_the_questions():
    outcomes = [
        outcome("q1", "u1", {"dense": 0.6}, {"u1#0": 3.0, "u2#0": -1.0}),
        outcome("q2", None, {"dense": 0.2}),
    ]
    rows, labels = training.gate_data(outcomes)
    assert rows == [{"dense": 0.6}, {"dense": 0.2}] and labels == [1, 0]
    scores, relevance = training.relevance_data(outcomes)
    assert scores == [3.0, -1.0] and relevance == [1, 0]


class StubCollection:
    def get(self, include):
        return {
            "embeddings": np.array(
                [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0], [0.1, 0.9], [0.5, 0.5]]
            ),
            "metadatas": [
                {"unit_id": "e1", "ecosystem": "energy"},
                {"unit_id": "e2", "ecosystem": "energy"},
                {"unit_id": "c1", "ecosystem": "commerce"},
                {"unit_id": "c2", "ecosystem": "commerce"},
                {"unit_id": "s1", "ecosystem": "shared"},
            ],
        }


def test_the_router_data_uses_the_ecosystems_of_the_units_and_leaves_out_shared():
    questions = [
        SimpleNamespace(text="power", expected_unit_id="e1"),
        SimpleNamespace(text="shop", expected_unit_id="c1"),
        SimpleNamespace(text="rule", expected_unit_id="s1"),
        SimpleNamespace(text="none", expected_unit_id=None),
    ]
    store = SimpleNamespace(collection=StubCollection())

    def embed(texts):
        return np.array([[1.0, 0.0] if t == "power" else [0.0, 1.0] for t in texts])

    centroids, vectors, labels = training.router_data(store, questions, embed)
    assert sorted(centroids) == ["commerce", "energy"] and labels == [
        "energy",
        "commerce",
    ]
    assert vectors.shape == (2, 2) and np.allclose(np.linalg.norm(vectors, axis=1), 1.0)


def test_fit_all_lists_the_parts_it_could_not_fit_with_the_reason():
    outcomes = [outcome(f"q{n}", "u", {"dense": 0.5}) for n in range(5)]
    params, skipped = training.fit_all(outcomes, [], None, None)
    assert params == {} and set(skipped) == {"gate", "relevance", "router"}
    assert "positive" in skipped["gate"] and "curated" in skipped["router"]


def test_the_top_score_of_each_retriever_is_named_like_the_retriever():
    searched = rv.Searched(
        SimpleNamespace(text="q"),
        None,
        {},
        "ok",
        [("a#0", 0.7), ("b#0", 0.4)],
        "ok",
        [],
        {"gte-modernbert-base": ("ok", [("a#0", 0.5)])},
    )
    assert rv.top_scores(searched) == {
        "dense": 0.7,
        "keyword": None,
        "dense:gte-modernbert-base": 0.5,
    }
