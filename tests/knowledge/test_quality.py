"""Retrieval quality -- state, integrity, freshness, questions, evaluation, thresholds,
acceptance, findings and the command.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder. The model is replaced by a fake (a vector
drawn from a hash of the text, word count for tokens), so a question equal to a
chunk text finds that chunk. ChromaDB and SQLite are the real libraries.
"""

from __future__ import annotations

import json
import math

import pytest

from ai.embeddings import build as eb
from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.knowledge_quality import (
    acceptance,
    evaluation,
    findings,
    freshness,
    runs,
    state,
    thresholds,
)
from ai.knowledge_quality import integrity as iq
from ai.knowledge_quality import questions as qs
from ai.knowledge_quality import run as rq
from ai.rag import keyword_fts as fts
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from ai.rag.retrieve import retrieve
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from tests.knowledge import test_embeddings as te
from tests.knowledge.test_retrieval import build_curated, build_wikipedia

pytestmark = [pytest.mark.unit, pytest.mark.ai]


@pytest.fixture(autouse=True)
def fake_model(monkeypatch):
    monkeypatch.setattr(emb, "Embedder", te.FakeEmbedder)
    monkeypatch.setattr(emb, "token_counter", lambda model_dir=None: te.words)
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(te.HASHES))
    yield
    st.clear_cache()


def model_dir(tmp_path):
    return tmp_path / "model"


def event_body(build_id="b1", collection="curated_b1"):
    return {
        "build_id": build_id,
        "collection": collection,
        "designated_on": "2026-10-14",
    }


# --- state -------------------------------------------------------------------


def test_designation_events_form_a_chain_and_a_hand_edit_is_found(tmp_path):
    assert state.designated_event(tmp_path, "curated") is None
    assert state.verify(tmp_path, "curated") == []
    first = state.designate(tmp_path, "curated", event_body("b1", "curated_b1"))
    second = state.designate(tmp_path, "curated", event_body("b2", "curated_b2"))
    current = state.read(tmp_path, "curated")
    assert current["designated"] == second and current["previous"] == first
    assert second["previous_hash"] == first["event_hash"]
    assert state.keep_collections(tmp_path, "curated") == {"curated_b1", "curated_b2"}
    assert state.verify(tmp_path, "curated") == []
    path = state.state_path(tmp_path, "curated")
    data = json.loads(path.read_text(encoding="utf-8"))
    data["history"][0]["approver"] = "somebody else"
    path.write_text(json.dumps(data), encoding="utf-8")
    assert "designation event 1 was changed by hand" in state.verify(
        tmp_path, "curated"
    )


def test_a_wrong_pointer_is_found_and_holdout_reads_are_recorded(tmp_path):
    state.designate(tmp_path, "curated", event_body())
    path = state.state_path(tmp_path, "curated")
    data = json.loads(path.read_text(encoding="utf-8"))
    data["designated"] = None
    path.write_text(json.dumps(data), encoding="utf-8")
    assert "designated pointer is not the last" in " ".join(
        state.verify(tmp_path, "curated")
    )
    assert state.holdout_read(tmp_path, "curated", "b1") is None
    state.record_holdout_read(tmp_path, "curated", {"build_id": "b1", "run_id": "r"})
    assert state.holdout_read(tmp_path, "curated", "b1")["run_id"] == "r"


# --- integrity ---------------------------------------------------------------


def test_unit_snapshots_are_compared_by_version_and_hash():
    old = [["a", 1, "h1"], ["b", 1, "h2"], ["c", 1, "h3"]]
    new = [["a", 1, "h1"], ["b", 2, "h2x"], ["d", 1, "h4"]]
    assert iq.diff_units(old, new) == (["d"], ["c"], ["b"])


def test_integrity_passes_on_a_good_curated_build(tmp_path):
    corpus_dir, _ = build_curated(tmp_path)
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert report.ok and report.problems == []
    assert report.notes == ["no build is designated yet"]


def test_integrity_fails_on_a_changed_model_file(tmp_path, monkeypatch):
    corpus_dir, _ = build_curated(tmp_path)
    changed = {**te.HASHES, "model_onnx_sha256": "e" * 64}
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(changed))
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert any("model file hash differs" in p for p in report.problems)


def test_integrity_fails_on_a_changed_unit(tmp_path):
    corpus_dir, _ = build_curated(tmp_path)
    unit = corpus_dir / "rules" / "rule.dwd.one.md"
    unit.write_text(
        unit.read_text(encoding="utf-8") + "\nA new sentence.", encoding="utf-8"
    )
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert any("the corpus cannot be read" in p for p in report.problems)


def test_integrity_fails_on_a_changed_chunk_in_the_collection(tmp_path):
    corpus_dir, _ = build_curated(tmp_path)
    record = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    client = coll.open_client(br.chroma_dir(corpus_dir.parent))
    stored = client.get_collection(record["collection"], embedding_function=None)
    stored.update(
        ids=["rule.dwd.one#0"],
        documents=["a different text"],
        embeddings=[te.fake_vector("a different text").tolist()],
    )
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert any("differ" in p for p in report.problems)


def test_integrity_fails_on_a_changed_chunk_metadata(tmp_path):
    corpus_dir, _ = build_curated(tmp_path)
    record = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    client = coll.open_client(br.chroma_dir(corpus_dir.parent))
    stored = client.get_collection(record["collection"], embedding_function=None)
    stored.update(ids=["rule.dwd.one#0"], metadatas=[{"ecosystem": "commerce"}])
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert any("differ from their units" in p for p in report.problems)


def test_integrity_fails_on_a_changed_keyword_file_and_graph_file(tmp_path):
    corpus_dir, build_id = build_curated(tmp_path)
    keyword = br.keyword_path(corpus_dir.parent, "curated", build_id)
    keyword.write_text(keyword.read_text(encoding="utf-8") + " ", encoding="utf-8")
    graph = corpus_dir.parent / "knowledge_graph" / "edges.jsonl"
    graph.write_text(graph.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    problems = iq.check("curated", corpus_dir, model_dir(tmp_path)).problems
    assert any("keyword index file differs" in p for p in problems)
    assert any("the graph differs" in p for p in problems)


def test_integrity_lists_the_units_changed_since_the_designation(tmp_path):
    corpus_dir, _ = build_curated(tmp_path)
    manifest = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)
    snapshot = iq.unit_snapshot(manifest)
    snapshot[0][2] = "0" * 64
    snapshot.append(["rule.gone", 1, "h"])
    body = {**event_body(), "unit_snapshot": snapshot}
    state.designate(corpus_dir.parent, "curated", body)
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert report.changed == [snapshot[0][0]] and report.removed == ["rule.gone"]
    assert report.added == [] and report.ok


def test_integrity_of_the_wikipedia_corpus_and_its_index_and_shards(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    assert iq.check("wikipedia", corpus_dir, model_dir(tmp_path)).ok
    fts.index_path(corpus_dir.parent, build_id).unlink()
    problems = iq.check("wikipedia", corpus_dir, model_dir(tmp_path)).problems
    assert any("no FTS index file" in p for p in problems)
    assert fts.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0
    shard = min((corpus_dir / "wikipedia" / "shards").glob("shard_*.jsonl"))
    shard.write_text(shard.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    problems = iq.check("wikipedia", corpus_dir, model_dir(tmp_path)).problems
    assert any(f"shard {shard.name} differs" in p for p in problems)


def test_integrity_reports_a_missing_vector_store(tmp_path):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    corpus_dir.mkdir(parents=True)
    report = iq.check("curated", corpus_dir, model_dir(tmp_path))
    assert report.problems == ["the vector store folder is missing"]


# --- freshness ---------------------------------------------------------------


def test_freshness_lists_units_whose_source_changed_and_unchecked_sources(tmp_path):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    (tmp_path / "src").mkdir()
    tracked = tmp_path / "src" / "a.yml"
    named = tmp_path / "src" / "b.md"
    tracked.write_text("one\n", encoding="utf-8")
    named.write_text("two\n", encoding="utf-8")
    local = tmp_path / "local.yml"
    local.write_text("logical_b: src/b.md\n", encoding="utf-8")
    sources = {
        "contract.a": [{"path": "src/a.yml", "sha256": uc.sha256_file(tracked)}],
        "rule.b": [{"name": "logical_b", "sha256": uc.sha256_file(named)}],
        "rule.c": [{"name": "unknown_name", "sha256": "d" * 64}],
        "metric.d": [{"path": "src/missing.yml", "sha256": "d" * 64}],
    }
    for unit_id, source in sources.items():
        kind = unit_id.split(".")[0]
        plan = uc.plan_unit(
            corpus_dir, unit_id, kind, "energy", source, "# T\n\nx", te.TODAY
        )
        uc.write_text(plan.path, plan.text)
    result = freshness.check("curated", corpus_dir, tmp_path, local)
    assert result.checked == 2 and result.stale == []
    assert {u["unit_id"]: u["reason"] for u in result.unchecked} == {
        "rule.c": "the name is not in the local source map",
        "metric.d": "the source file was not found",
    }
    tracked.write_text("one changed\n", encoding="utf-8")
    named.write_text("two changed\n", encoding="utf-8")
    result = freshness.check("curated", corpus_dir, tmp_path, local)
    assert sorted(item["unit_id"] for item in result.stale) == ["contract.a", "rule.b"]
    assert result.stale[0]["recorded"] != result.stale[0]["current"]
    without = freshness.check("curated", corpus_dir, tmp_path, tmp_path / "none.yml")
    assert {u["reason"] for u in without.unchecked} >= {"no local source map"}


def test_freshness_of_the_wikipedia_corpus_reports_only_the_snapshot(tmp_path):
    corpus_dir, _ = build_wikipedia(tmp_path)
    result = freshness.check("wikipedia", corpus_dir)
    assert result.snapshot == "snap" and result.stale == [] and result.checked == 0


# --- questions ---------------------------------------------------------------


def make_question(
    number, text="A question?", cls="definitional_wording", unit="rule.a"
):
    kind = None if cls == qs.NEGATIVE else "rule"
    unit_id = None if cls == qs.NEGATIVE else unit
    return qs.Question(f"q{number:03d}", text, cls, kind, unit_id)


def test_a_question_is_approved_by_content_and_stale_when_it_changes():
    question = make_question(1)
    assert question.status == "pending"
    chosen = qs.approve(
        qs.QuestionSet("curated", 1, [question]), ["q001"], "Owner", "2026-10-14"
    )
    assert chosen == [question] and question.status == "approved"
    question.text = "A changed question?"
    assert question.status == "stale"


def test_approval_raises_the_set_version_and_names_unknown_ids():
    question_set = qs.QuestionSet("curated", 1, [make_question(1), make_question(2)])
    qs.approve(question_set, ["q001"], "Owner", "2026-10-14")
    assert question_set.version == 2
    qs.approve(question_set, ["q001"], "Owner", "2026-10-14")
    assert question_set.version == 2
    with pytest.raises(ValueError, match="unknown question id"):
        qs.approve(question_set, ["q999"], "Owner", "2026-10-14")


def test_questions_are_validated(tmp_path):
    kinds = uc.KIND_DIRS
    qs.validate(make_question(1), {"rule.a"}, kinds)
    qs.validate(make_question(2, cls=qs.NEGATIVE), {"rule.a"}, kinds)
    with pytest.raises(ValueError, match="bad id or empty text"):
        qs.validate(make_question(3, text=" "), None, kinds)
    with pytest.raises(ValueError, match="unknown class"):
        qs.validate(make_question(4, cls="other"), None, kinds)
    with pytest.raises(ValueError, match="does not exist"):
        qs.validate(make_question(5, unit="rule.zzz"), {"rule.a"}, kinds)
    negative = make_question(6, cls=qs.NEGATIVE)
    negative.expected_unit_id = "rule.a"
    with pytest.raises(ValueError, match="names no unit"):
        qs.validate(negative, None, kinds)


def test_questions_round_trip_and_the_split_is_stable(tmp_path):
    items = [make_question(n, f"Question {n}?") for n in range(1, 31)]
    question_set = qs.QuestionSet("curated", 1, items)
    qs.approve(question_set, [q.id for q in items], "Owner", "2026-10-14")
    path = tmp_path / "curated.yml"
    qs.save(path, question_set)
    loaded = qs.load(path, {"rule.a"})
    assert [q.id for q in loaded.questions] == [q.id for q in items]
    assert loaded.set_hash == question_set.set_hash and loaded.version == 2
    holdout = {q.id for q in loaded.of_split(qs.HOLDOUT)}
    tune = {q.id for q in loaded.of_split(qs.TUNE)}
    assert holdout | tune == {q.id for q in items} and not holdout & tune
    assert 5 <= len(holdout) <= 15
    extra = qs.QuestionSet("curated", 1, [*items, make_question(31, "Another?")])
    assert {q.id for q in items if q.split == qs.HOLDOUT} == holdout
    assert extra.questions[-1].split in (qs.TUNE, qs.HOLDOUT)


def test_a_pending_question_is_not_evaluated_and_a_duplicate_id_is_refused(tmp_path):
    items = [make_question(1), make_question(2)]
    question_set = qs.QuestionSet("curated", 1, items)
    qs.approve(question_set, ["q001"], "Owner", "2026-10-14")
    assert [q.id for q in question_set.approved()] == ["q001"]
    path = tmp_path / "dup.yml"
    qs.save(path, qs.QuestionSet("curated", 1, [make_question(1), make_question(1)]))
    with pytest.raises(ValueError, match="appears twice"):
        qs.load(path, None)


def test_the_drafted_curated_question_set_is_valid():
    path = qs.questions_path(uc.ROOT / "ai", "curated")
    if not path.is_file():
        pytest.skip("no question set in this checkout")
    manifest = json.loads(
        (uc.CORPUS_DIR / uc.MANIFEST_NAME).read_text(encoding="utf-8")
    )
    if not any(u["unit_id"].startswith("model_card.") for u in manifest["units"]):
        pytest.skip("the model card and approval rule units are not built yet")
    question_set = qs.load(path, {u["unit_id"] for u in manifest["units"]})
    negative = [q for q in question_set.questions if q.negative]
    assert len(question_set.questions) - len(negative) >= 75 and len(negative) >= 15
    kinds = {q.kind for q in question_set.questions if not q.negative}
    assert kinds == set(uc.KIND_DIRS)
    assert {q.cls for q in question_set.questions} == {*qs.CLASSES, qs.NEGATIVE}


# --- evaluation --------------------------------------------------------------


def outcome(question_id, expected, ranks, split="tune", no_match=None, scores=None):
    return evaluation.Outcome(
        question_id=question_id,
        cls="definitional_wording" if expected else qs.NEGATIVE,
        kind="rule" if expected else None,
        expected_unit_id=expected,
        split=split,
        ranks={mode: ranks.get(mode) for mode in evaluation.MODES},
        no_match={mode: (no_match or {}).get(mode, False) for mode in evaluation.MODES},
        expected_score=(scores or {}).get("expected", {}),
        top_score=(scores or {}).get("top", {}),
    )


def test_rank_helpers_follow_the_unit_order():
    assert evaluation.unit_of("rule.a#12") == "rule.a"
    assert evaluation.units_in_order(["u.1#0", "u.2#0", "u.1#1"]) == ["u.1", "u.2"]
    assert evaluation.rank_of(["u.1", "u.2"], "u.2") == 2
    assert evaluation.rank_of(["u.1"], "u.9") is None
    assert evaluation.rank_of(["u.1"], None) is None
    pairs = [("u.1#0", 0.5), ("u.2#0", 0.9), ("u.1#1", 0.7)]
    assert evaluation.best_score(pairs, "u.1") == 0.7
    assert evaluation.best_score(pairs, "u.3") is None


def test_metrics_count_hits_ranks_and_no_match_answers():
    rows = [
        outcome("q001", "u.1", {"hybrid": 1}),
        outcome("q002", "u.2", {"hybrid": 3}),
        outcome("q003", "u.3", {"hybrid": 7}),
        outcome("q004", "u.4", {}),
        outcome("q005", None, {}, no_match={"hybrid": True}),
        outcome("q006", None, {}, no_match={"hybrid": False}),
    ]
    m = evaluation.metrics(rows, "hybrid")
    assert (m["positive"], m["recall_1"], m["recall_5"], m["recall_context"]) == (
        4,
        1,
        2,
        3,
    )
    assert m["mrr_sum"] == pytest.approx(1 + 1 / 3 + 1 / 7)
    assert m["ndcg_5_sum"] == pytest.approx(1 + 1 / math.log2(4))
    assert (m["negative"], m["no_match"]) == (2, 1)
    groups = evaluation.grouped(rows)
    assert set(groups) == {
        "all",
        "class:definitional_wording",
        "class:negative",
        "kind:rule",
    }
    assert groups["kind:rule"]["hybrid"]["positive"] == 4


def test_a_wikipedia_question_is_evaluated_for_every_retrieval_type(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    store = st.load("wikipedia", corpus_dir, model_dir(tmp_path), build_id)
    positive = qs.Question(
        "q001",
        "Lead text 3.",
        "definitional_wording",
        "wikipedia_article",
        "wikipedia.3",
    )
    negative = qs.Question("q002", "volcano lava eruption", qs.NEGATIVE, None, None)
    config = RetrievalConfig(final_k=2)
    found, nothing = evaluation.evaluate(store, [positive, negative], config)
    assert found.ranks["dense"] == 1 and found.ranks["hybrid"] == 1
    assert found.ranks["keyword"] is not None
    assert found.served["hybrid_graph"][0] == "wikipedia.3"
    assert set(found.served["hybrid_graph"]) >= {"wikipedia.3", "wikipedia.4"}
    assert found.expected_score["dense"] == pytest.approx(1.0, abs=1e-4)
    assert nothing.ranks["dense"] is None and nothing.no_match["keyword"] is True
    assert nothing.no_match["dense"] is False and nothing.top_score["keyword"] is None


def test_a_run_record_holds_the_context_the_metrics_and_every_outcome(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    store = st.load("wikipedia", corpus_dir, model_dir(tmp_path), build_id)
    question = qs.Question(
        "q001",
        "Lead text 3.",
        "definitional_wording",
        "wikipedia_article",
        "wikipedia.3",
    )
    question_set = qs.QuestionSet("wikipedia", 1, [question])
    qs.approve(question_set, ["q001"], "Owner", "2026-10-14")
    config = RetrievalConfig()
    outcomes = evaluation.evaluate(store, [question], config)
    record = evaluation.run_record(
        store, "wikipedia", "tune", outcomes, question_set, config, None
    )
    assert (
        record["run_id"].startswith("wikipedia-tune-")
        and record["build_id"] == build_id
    )
    assert record["question_set"]["hash"] == question_set.set_hash
    assert record["metrics"]["all"]["hybrid"]["positive"] == 1
    assert record["outcomes"][0]["question_id"] == "q001"
    assert record["model"]["id"] == emb.MODEL_ID and record["chunk_count"] == 40


# --- thresholds --------------------------------------------------------------


def test_the_threshold_keeps_the_share_and_is_rounded_down():
    scores = [round(0.5 + n / 100, 2) for n in range(20)]
    assert thresholds.threshold_for(scores) == 0.51
    assert thresholds.threshold_for([0.123456, 0.9]) == 0.1234
    assert thresholds.threshold_for([]) is None
    assert thresholds.threshold_for([0.7]) == 0.7
    assert thresholds.threshold_for([1.0, 0.8, 0.6], retention=0.5) == 0.8


def tuning_outcomes():
    pairs = [(0.9, 9.0), (0.8, 8.0), (0.7, 7.0), (0.6, 6.0)]
    rows = [
        outcome(
            f"q{n:03d}", f"u.{n}", {}, scores={"expected": {"dense": d, "keyword": k}}
        )
        for n, (d, k) in enumerate(pairs, 1)
    ]
    rows.append(
        outcome(
            "q010", "u.9", {}, scores={"expected": {"dense": None, "keyword": None}}
        )
    )
    for n, (dense, keyword) in enumerate([(0.3, 4.0), (0.65, 3.0)], 11):
        rows.append(
            outcome(
                f"q{n:03d}",
                None,
                {},
                scores={"top": {"dense": dense, "keyword": keyword}},
            )
        )
    return rows


def test_thresholds_report_the_hits_kept_and_the_negatives_stopped():
    found = thresholds.establish(tuning_outcomes())
    dense = found["dense"]
    assert dense["threshold"] == 0.6 and dense["expected_hits"] == 4
    assert (dense["hits_kept"], dense["negatives"], dense["negatives_no_match"]) == (
        4,
        2,
        1,
    )
    assert dense["negative_rate"] == 0.5 and dense["conflict"] is True
    keyword = found["keyword"]
    assert keyword["threshold"] == 6.0 and keyword["negatives_no_match"] == 2
    assert keyword["conflict"] is False


def test_config_versions_are_written_never_edited_and_the_latest_is_read(tmp_path):
    ai_dir = tmp_path / "ai"
    assert thresholds.latest(ai_dir, "curated") == (None, RetrievalConfig())
    found = thresholds.establish(tuning_outcomes())
    first, data = thresholds.write_version(
        ai_dir, "curated", RetrievalConfig(), found, {"tuning_run_id": "r1"}
    )
    second, _ = thresholds.write_version(
        ai_dir, "curated", RetrievalConfig(final_k=4), found, {}
    )
    assert (first, second) == (1, 2) and thresholds.versions(ai_dir, "curated") == [
        1,
        2,
    ]
    assert data["config"]["dense_min_score"] == 0.6 and data["tuning_run_id"] == "r1"
    number, config = thresholds.latest(ai_dir, "curated")
    assert number == 2 and config.final_k == 4 and config.keyword_min_score == 6.0
    assert thresholds.read_version(ai_dir, "curated", 1)["config"]["final_k"] == 5
    with pytest.raises(TypeError):
        RetrievalConfig.from_dict({"unknown": 1})


# --- acceptance --------------------------------------------------------------


def holdout_run(positive_hits, positive, negative_no_match, negative, set_hash="h"):
    outcomes = []
    for number in range(positive):
        rank = None if number >= positive_hits else 1
        outcomes.append(
            {
                "question_id": f"q{number:03d}",
                "expected_unit_id": f"u.{number}",
                "ranks": {acceptance.MODE: rank},
            }
        )
    metrics = {
        "positive": positive,
        "recall_5": positive_hits,
        "negative": negative,
        "no_match": negative_no_match,
    }
    return {
        "question_set": {"hash": set_hash},
        "metrics": {"all": {acceptance.MODE: metrics}},
        "outcomes": outcomes,
    }


def test_the_gate_passes_a_candidate_that_meets_every_count():
    decision = acceptance.decide(holdout_run(23, 25, 4, 5), None, [])
    assert decision.ok
    states = {c["check"]: c["passed"] for c in decision.checks}
    assert states["additional misses against the previous build"] is None


@pytest.mark.parametrize(
    ("run", "failed"),
    [
        (holdout_run(22, 25, 5, 5), "positive questions in the top 5"),
        (holdout_run(25, 25, 3, 5), "negative questions answered no_match"),
    ],
)
def test_the_gate_refuses_a_candidate_below_a_count(run, failed):
    decision = acceptance.decide(run, None, [])
    assert not decision.ok
    assert [c["check"] for c in decision.checks if c["passed"] is False] == [failed]


def test_the_gate_scales_the_counts_to_the_holdout_size():
    assert acceptance.decide(holdout_run(3, 3, 1, 1), None, []).ok
    assert not acceptance.decide(holdout_run(2, 3, 1, 1), None, []).ok


def test_the_gate_limits_additional_misses_against_the_previous_build():
    previous = holdout_run(25, 30, 5, 5)
    previous["outcomes"] = [
        {**o, "ranks": {acceptance.MODE: 1}} for o in previous["outcomes"]
    ]
    now = holdout_run(29, 30, 5, 5)
    assert acceptance.misses(now) == {"q029"}
    assert acceptance.decide(now, previous, []).ok
    worse = holdout_run(28, 30, 5, 5)
    worse_decision = acceptance.decide(worse, previous, [])
    assert not worse_decision.ok
    assert "2 additional miss(es)" in worse_decision.checks[2]["detail"]
    changed = {**previous, "question_set": {"hash": "other"}}
    assert acceptance.decide(now, changed, []).checks[2]["passed"] is None


def test_the_gate_fails_when_an_integrity_check_fails():
    decision = acceptance.decide(holdout_run(25, 25, 5, 5), None, ["the graph differs"])
    assert not decision.ok and "the graph differs" in decision.checks[-1]["detail"]


# --- findings and run records -----------------------------------------------


def test_findings_render_without_any_record_and_never_hold_a_local_path(tmp_path):
    text = findings.render(tmp_path / "ai", "curated")
    assert text.startswith("# CURATED RETRIEVAL QUALITY FINDINGS")
    for line in ("No check recorded.", "No run recorded.", "No build is designated."):
        assert line in text
    assert str(tmp_path) not in text


def test_a_check_and_the_run_records_are_stored_and_found_again(tmp_path):
    ai_dir = tmp_path / "ai"
    report = iq.IntegrityReport("curated", problems=["x"], added=["u.1"])
    fresh = freshness.Freshness("curated", checked=3)
    runs.save_check(ai_dir, "curated", report, fresh)
    saved = runs.load_check(ai_dir, "curated")
    assert (
        saved["integrity"]["problems"] == ["x"] and saved["freshness"]["checked"] == 3
    )
    older = {"run_id": "curated-tune-20261001T000000Z", "corpus_id": "curated"}
    newer = {"run_id": "curated-tune-20261002T000000Z", "corpus_id": "curated"}
    runs.save_run(ai_dir, older)
    runs.save_run(ai_dir, newer)
    assert runs.latest_run(ai_dir, "curated", "tune") == newer
    assert runs.latest_run(ai_dir, "curated", "holdout") is None
    assert runs.load_run(ai_dir, "curated", older["run_id"]) == older


# --- the command, end to end on the Wikipedia fixture ------------------------


NEGATIVE_TEXTS = [
    "volcano lava eruption",
    "pizza dough oven",
    "violin orchestra concert",
    "glacier iceberg arctic",
    "telescope galaxy nebula",
    "submarine ocean trench",
    "marathon runner stamina",
    "tulip garden spring",
]


NEAR_TEXTS = [f"lead text of topic {word}" for word in NEGATIVE_TEXTS]


def write_wikipedia_questions(corpus_dir, negatives=None):
    items = [
        qs.Question(f"q{n:03d}", f"Lead text {n}.", "definitional_wording",
                    "wikipedia_article", f"wikipedia.{n}")
        for n in range(1, 21)
    ]  # fmt: skip
    # Ids chosen so that four negative questions are tuning and four are holdout.
    ids = [21, 22, 23, 24, 30, 31, 34, 35]
    items += [
        qs.Question(f"q{n:03d}", text, qs.NEGATIVE, None, None)
        for n, text in zip(ids, negatives or NEGATIVE_TEXTS, strict=True)
    ]
    qs.save(
        qs.questions_path(corpus_dir.parent, "wikipedia"),
        qs.QuestionSet("wikipedia", 1, items),
    )


def command(corpus_dir, tmp_path, *words):
    argv = [
        words[0],
        "--corpus", "wikipedia",
        "--corpus-dir", str(corpus_dir),
        "--model-dir", str(model_dir(tmp_path)),
        "--root", str(tmp_path),
        *words[1:],
    ]  # fmt: skip
    return rq.main(argv)


def test_the_whole_quality_flow_designates_a_build_and_production_retrieval_follows(
    tmp_path, capsys
):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    assert (
        command(
            corpus_dir,
            tmp_path,
            "approve-questions",
            "--approver",
            "Owner",
            "--all-pending",
            "--apply",
        )
        == 0
    )
    assert command(corpus_dir, tmp_path, "check", "--apply") == 0
    assert command(corpus_dir, tmp_path, "evaluate", "--split", "tune", "--apply") == 0
    assert command(corpus_dir, tmp_path, "thresholds", "--apply") == 0
    number, config = thresholds.latest(corpus_dir.parent, "wikipedia")
    assert number == 1 and config.dense_min_score is not None
    assert (
        command(corpus_dir, tmp_path, "accept", "--approver", "Owner", "--apply") == 1
    )
    assert "has no holdout run" in capsys.readouterr().out
    assert (
        command(corpus_dir, tmp_path, "evaluate", "--split", "holdout", "--dry-run")
        == 1
    )
    assert (
        command(corpus_dir, tmp_path, "evaluate", "--split", "holdout", "--apply") == 0
    )
    assert state.holdout_read(corpus_dir.parent, "wikipedia", build_id) is not None
    capsys.readouterr()
    assert (
        command(corpus_dir, tmp_path, "evaluate", "--split", "holdout", "--apply") == 1
    )
    assert "once per candidate" in capsys.readouterr().out
    assert command(corpus_dir, tmp_path, "accept", "--apply") == 1
    assert (
        command(corpus_dir, tmp_path, "accept", "--approver", "Owner", "--apply") == 0
    )
    event = state.designated_event(corpus_dir.parent, "wikipedia")
    assert event["build_id"] == build_id and event["approver"] == "Owner"
    assert event["config_version"] == 1 and event["checks"][-1]["passed"] is True
    assert state.verify(corpus_dir.parent, "wikipedia") == []
    text = (
        tmp_path / "src" / "findings" / "knowledge_findings" / "wikipedia_quality.md"
    ).read_text(encoding="utf-8")
    for line in (
        "## holdout questions",
        "## thresholds",
        f"Designated: build {build_id}",
    ):
        assert line in text
    assert str(tmp_path) not in text
    assert command(corpus_dir, tmp_path, "status") == 0

    result = retrieve(
        "Lead text 3.",
        corpus="wikipedia",
        corpus_dir=corpus_dir,
        model_dir=model_dir(tmp_path),
    )
    assert result.status == "ok" and result.hits[0].chunk_id == "wikipedia.3#0"
    assert result.config["dense_min_score"] == config.dense_min_score
    nothing = retrieve(
        "volcano lava eruption",
        corpus="wikipedia",
        corpus_dir=corpus_dir,
        model_dir=model_dir(tmp_path),
    )
    assert nothing.status == "no_match"


def test_a_candidate_that_fails_a_count_cannot_be_designated(tmp_path, capsys):
    corpus_dir, _ = build_wikipedia(tmp_path)
    # Words that are in the corpus, so both retrievers answer: no `no_match`.
    write_wikipedia_questions(corpus_dir, NEAR_TEXTS)
    command(
        corpus_dir,
        tmp_path,
        "approve-questions",
        "--approver",
        "Owner",
        "--all-pending",
        "--apply",
    )
    command(corpus_dir, tmp_path, "evaluate", "--split", "holdout", "--apply")
    capsys.readouterr()
    assert (
        command(corpus_dir, tmp_path, "accept", "--approver", "Owner", "--apply") == 1
    )
    output = capsys.readouterr().out
    assert "FAIL negative questions answered no_match" in output
    assert "The build cannot be designated." in output
    assert state.designated_event(corpus_dir.parent, "wikipedia") is None


def test_the_question_set_must_not_change_between_the_holdout_run_and_the_gate(
    tmp_path, capsys
):
    corpus_dir, _ = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    command(
        corpus_dir,
        tmp_path,
        "approve-questions",
        "--approver",
        "Owner",
        "--all-pending",
        "--apply",
    )
    command(corpus_dir, tmp_path, "thresholds", "--apply")
    command(corpus_dir, tmp_path, "evaluate", "--split", "holdout", "--apply")
    path = qs.questions_path(corpus_dir.parent, "wikipedia")
    question_set = qs.load(path)
    question_set.questions[0].text = "Lead text 1. Changed."
    qs.save(path, question_set)
    capsys.readouterr()
    assert (
        command(corpus_dir, tmp_path, "accept", "--approver", "Owner", "--apply") == 1
    )
    assert "question set changed" in capsys.readouterr().out


def test_retrieval_without_a_designation_or_with_a_build_that_is_not_kept_stops(
    tmp_path,
):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    with pytest.raises(ValueError, match="no build of wikipedia is designated"):
        retrieve(
            "Lead text 3.",
            corpus="wikipedia",
            corpus_dir=corpus_dir,
            model_dir=model_dir(tmp_path),
        )
    state.designate(
        corpus_dir.parent, "wikipedia", {**event_body("0" * 12), "config": {}}
    )
    with pytest.raises(ValueError, match="is not kept"):
        retrieve(
            "Lead text 3.",
            corpus="wikipedia",
            corpus_dir=corpus_dir,
            model_dir=model_dir(tmp_path),
        )
    assert build_id != "0" * 12


def test_production_serves_a_designated_build_that_is_no_longer_the_recorded_one(
    tmp_path,
):
    pytest.importorskip("rank_bm25")
    corpus_dir, old_id = build_curated(tmp_path)
    te.make_units(
        corpus_dir,
        [("rule.dwd.one", "rule", "# Rule: one\n\nA changed rule.", te.SOURCE)],
    )
    argv = ["build", "--corpus", "curated", *te.args_for(corpus_dir, "--apply")]
    assert eb.main(argv) == 0
    recorded = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    assert recorded["build_id"] != old_id
    old = br.read_build_record(corpus_dir.parent, "curated", old_id)
    state.designate(
        corpus_dir.parent,
        "curated",
        {**event_body(old_id, old["collection"]), "config": {}},
    )
    result = retrieve(
        "price",
        production=True,
        corpus_dir=corpus_dir,
        model_dir=model_dir(tmp_path),
    )
    assert result.build_id == old_id
    kept = iq.check_kept("curated", corpus_dir, model_dir(tmp_path), old_id)
    assert kept.ok and kept.notes
    graph = br.build_graph_dir(corpus_dir.parent, "curated", old_id) / "edges.jsonl"
    graph.write_text(graph.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    again = iq.check_kept("curated", corpus_dir, model_dir(tmp_path), old_id)
    assert any("graph copy" in p for p in again.problems)


def test_production_uses_the_designated_build_and_refuses_an_explicit_one(tmp_path):
    corpus_dir, build_id = build_curated(tmp_path)
    with pytest.raises(ValueError, match="production uses the designated build"):
        retrieve(
            "price",
            build_id=build_id,
            production=True,
            corpus_dir=corpus_dir,
            model_dir=model_dir(tmp_path),
        )
    with pytest.raises(ValueError, match="no build of curated is designated"):
        retrieve(
            "price",
            production=True,
            corpus_dir=corpus_dir,
            model_dir=model_dir(tmp_path),
        )


def test_a_failed_integrity_check_blocks_the_designated_build(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    for words in (
        ("approve-questions", "--approver", "Owner", "--all-pending", "--apply"),
        ("thresholds", "--apply"),
        ("evaluate", "--split", "holdout", "--apply"),
        ("accept", "--approver", "Owner", "--apply"),
    ):
        command(corpus_dir, tmp_path, *words)
    assert (
        state.designated_event(corpus_dir.parent, "wikipedia")["build_id"] == build_id
    )
    shard = min((corpus_dir / "wikipedia" / "shards").glob("shard_*.jsonl"))
    shard.write_text(shard.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="integrity check failed"):
        retrieve(
            "Lead text 3.",
            corpus="wikipedia",
            corpus_dir=corpus_dir,
            model_dir=model_dir(tmp_path),
        )


def test_a_build_keeps_the_designated_and_the_previous_collection(tmp_path):
    corpus_dir, build_id = build_curated(tmp_path)
    record = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    state.designate(
        corpus_dir.parent, "curated", event_body(build_id, record["collection"])
    )
    for text in ("changed once", "changed twice"):
        unit = corpus_dir / "rules" / "rule.dwd.one.md"
        header, _ = uc.parse_unit(unit.read_text(encoding="utf-8"))
        uc.write_text(unit, uc.render_unit(header, f"# Rule: one\n\n{text}"))
        au.approve_chosen(corpus_dir, uc.scan_units(corpus_dir), "Owner")
        assert (
            eb.main(
                ["build", "--corpus", "curated", *te.args_for(corpus_dir, "--apply")]
            )
            == 0
        )
    client = coll.open_client(br.chroma_dir(corpus_dir.parent))
    assert record["collection"] in coll.names(client, "curated")


def test_the_parent_command_runs_every_step_and_designates_the_build(tmp_path, capsys):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    assert command(corpus_dir, tmp_path, "all", "--approver", "Owner", "--apply") == 0
    output = capsys.readouterr().out
    for name in (
        "check",
        "approve-questions",
        "evaluate (tune)",
        "thresholds",
        "evaluate (holdout)",
        "accept",
    ):
        assert f"== step: {name} ==" in output
    event = state.designated_event(corpus_dir.parent, "wikipedia")
    assert event["build_id"] == build_id and event["approver"] == "Owner"
    assert command(corpus_dir, tmp_path, "all", "--approver", "Owner", "--apply") == 0
    assert "its run is used" in capsys.readouterr().out
    assert len(state.read(corpus_dir.parent, "wikipedia")["history"]) == 2


def test_the_parent_command_stops_while_no_question_is_approved(tmp_path, capsys):
    corpus_dir, _ = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    assert command(corpus_dir, tmp_path, "all", "--apply") == 1
    output = capsys.readouterr().out
    assert "wait for approval; no approver name" in output
    assert "Stopped at step evaluate (tune)" in output
    assert state.designated_event(corpus_dir.parent, "wikipedia") is None


def test_the_parent_dry_run_writes_no_config_and_does_not_read_the_holdout(
    tmp_path, capsys
):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    command(
        corpus_dir,
        tmp_path,
        "approve-questions",
        "--approver",
        "Owner",
        "--all-pending",
        "--apply",
    )
    assert command(corpus_dir, tmp_path, "all", "--dry-run") == 0
    assert "the holdout and the acceptance are not run" in capsys.readouterr().out
    assert thresholds.versions(corpus_dir.parent, "wikipedia") == []
    assert state.holdout_read(corpus_dir.parent, "wikipedia", build_id) is None


def test_without_a_command_the_whole_flow_runs(tmp_path, capsys):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    argv = [
        "--corpus", "wikipedia",
        "--corpus-dir", str(corpus_dir),
        "--model-dir", str(model_dir(tmp_path)),
        "--root", str(tmp_path),
        "--approver", "Owner",
        "--apply",
    ]  # fmt: skip
    assert rq.main(argv) == 0
    assert "== step: accept ==" in capsys.readouterr().out
    assert (
        state.designated_event(corpus_dir.parent, "wikipedia")["build_id"] == build_id
    )


def test_an_owner_value_replaces_the_rule_and_ends_the_conflict():
    rows = tuning_outcomes()
    found = thresholds.establish(rows, {"dense": 0.31})
    dense = found["dense"]
    assert dense["threshold"] == 0.31 and dense["computed_threshold"] == 0.6
    assert dense["set_by"] == "owner" and dense["conflict"] is False
    assert (dense["hits_kept"], dense["negatives_no_match"]) == (4, 1)
    assert found["keyword"]["set_by"] == "rule"
    assert thresholds.establish(rows)["dense"]["conflict"] is True


def test_the_threshold_step_takes_a_value_from_the_owner(tmp_path):
    corpus_dir, _ = build_wikipedia(tmp_path)
    write_wikipedia_questions(corpus_dir)
    command(
        corpus_dir,
        tmp_path,
        "approve-questions",
        "--approver",
        "Owner",
        "--all-pending",
        "--apply",
    )
    assert (
        command(
            corpus_dir, tmp_path, "thresholds", "--dense-min-score", "0.5", "--apply"
        )
        == 0
    )
    number, config = thresholds.latest(corpus_dir.parent, "wikipedia")
    data = thresholds.read_version(corpus_dir.parent, "wikipedia", number)
    assert config.dense_min_score == 0.5 and config.keyword_min_score is not None
    assert data["thresholds"]["dense"]["set_by"] == "owner"
    assert data["thresholds"]["keyword"]["set_by"] == "rule"
