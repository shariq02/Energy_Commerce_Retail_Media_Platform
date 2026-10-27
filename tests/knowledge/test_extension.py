"""The extension of the knowledge work -- config, reranker cache, judge steps, units,
questions, thresholds, acceptance and retrieval with a second dense retriever.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Nothing is sent over the network; the judge, the Databricks connection and the chunk
store are replaced by small fakes.
"""

from __future__ import annotations

import argparse
import json
import textwrap
from types import SimpleNamespace

import pytest

from ai.embeddings import embedder as emb
from ai.embeddings import extra
from ai.knowledge_quality import acceptance, evaluation, thresholds
from ai.knowledge_quality import questions as qs
from ai.rag import rerank as rr
from ai.rag import retrieve as rv
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import build_units as bu
from scripts.knowledge import rerank_pairs as rp

pytestmark = [pytest.mark.unit, pytest.mark.ai]


# --- config ---------------------------------------------------------------------


def test_the_extension_switches_are_off_and_old_configs_stay_valid():
    config = RetrievalConfig()
    assert config.embedders == () and not config.rerank
    assert not (config.judge_filter or config.judge_gate or config.judge_relevance)
    old = {
        k: v
        for k, v in config.as_dict().items()
        if k
        in (
            "dense_k",
            "keyword_k",
            "rrf_constant",
            "final_k",
            "related_max",
            "dense_min_score",
            "keyword_min_score",
            "expansion_terms_max",
        )
    }
    assert RetrievalConfig.from_dict(old) == config


def test_a_config_round_trips_through_json_with_a_list_of_embedders():
    config = RetrievalConfig(
        embedders=("gte-modernbert-base",),
        extra_min_scores={"gte-modernbert-base": 0.3},
    )
    back = RetrievalConfig.from_dict(json.loads(json.dumps(config.as_dict())))
    assert back == config and back.embedders == ("gte-modernbert-base",)
    with pytest.raises(ValueError, match="rerank_candidates"):
        RetrievalConfig(rerank=True, rerank_candidates=2, final_k=5)


# --- reranker cache -------------------------------------------------------------


def test_reranker_scores_reorder_the_fused_top_and_a_missing_score_keeps_the_order(
    tmp_path,
):
    fused = [("a#0", 0.03, {}), ("b#0", 0.02, {}), ("c#0", 0.01, {})]
    texts = {"a#0": "text a", "b#0": "text b", "c#0": "text c"}
    scores = {
        rr.pair_key("m", "q", texts[c]): s
        for c, s in (("a#0", 0.1), ("b#0", 0.9), ("c#0", 0.5))
    }
    ordered, status, missing = rr.reorder("m", "q", fused, texts, scores, 2)
    assert (
        [row[0] for row in ordered] == ["b#0", "c#0"]
        and status == rr.RERANKED
        and missing == 0
    )
    del scores[rr.pair_key("m", "q", "text c")]
    ordered, status, missing = rr.reorder("m", "q", fused, texts, scores, 2)
    assert [row[0] for row in ordered] == ["a#0", "b#0"]
    assert (status, missing) == (rr.MISSING, 1)
    path = tmp_path / "scores.jsonl"
    rr.save_scores(path, scores)
    assert rr.load_scores(path) == scores and rr.load_scores(tmp_path / "none") == {}
    assert rr.pair_key("m", "q", "t") != rr.pair_key("other", "q", "t")


# --- the judge steps of retrieval ------------------------------------------------


class FakeJudge:
    """Answers of a judge: a relevance, an answerable value and an ecosystem filter."""

    def __init__(self, relevance=None, answerable=None, area=None):
        self._relevance, self._answerable, self._area = relevance, answerable, area

    def relevance(self, query, text, rerank_score):
        return self._relevance

    def answerable(self, query, context, top_scores):
        return self._answerable

    def area_filter(self, query, minimum):
        return self._area


def test_judge_steps_keep_hits_without_an_answer_and_drop_or_block_on_a_low_value():
    config = RetrievalConfig(judge_relevance=True, judge_gate=True)
    hits = [
        SimpleNamespace(text="one", chunk_id="a#0"),
        SimpleNamespace(text="two", chunk_id="b#0"),
    ]
    kept, related, notes = rv.judge_after(None, config, "q", hits, [])
    assert kept == hits and notes["judge"]["status"].startswith("skipped")
    kept, _, notes = rv.judge_after(FakeJudge(relevance=None), config, "q", hits, [])
    assert kept == hits
    low = FakeJudge(relevance=0.1, answerable=0.9)
    kept, _, notes = rv.judge_after(low, config, "q", hits, [])
    assert kept == [] and notes["judge_relevance"] == {"kept": 0, "of": 2}
    high = FakeJudge(relevance=0.9, answerable=0.1)
    kept, related, notes = rv.judge_after(high, config, "q", hits, ["r"])
    assert kept == [] and related == [] and notes["judge_gate"]["blocked"] is True


def test_the_domain_filter_of_the_judge_is_added_unless_the_caller_gave_one():
    config = RetrievalConfig(judge_filter=True)
    filters, notes = rv.judge_filters(None, config, "q", {})
    assert filters == {} and "skipped" in notes["judge_filter"]["status"]
    judge = FakeJudge(area=["energy", "shared"])
    filters, notes = rv.judge_filters(judge, config, "q", {})
    assert filters == {"ecosystem": ["energy", "shared"]}
    given = {"ecosystem": ["commerce"]}
    assert rv.judge_filters(judge, config, "q", given) == (given, {})
    filters, notes = rv.judge_filters(FakeJudge(area=None), config, "q", {})
    assert filters == {} and notes["judge_filter"]["status"] == "no answer"


# --- units: model cards and approval rules --------------------------------------

CARD = textwrap.dedent(
    """\
    # Model card: ccpp.output

    _model `tabicl_v2_in_context`, dataset `ccpp`, frozen version 0, primary metric `mae` (lower is better). Generated: 2026-10-26T17:52Z_

    ## Decision

    - decision: `approved_with_conditions`
    - rules applied: R10, R13

    ## Purpose and task

    - target: `target_net_output_mw`

    ## Intended use

    - estimates the net electrical output

    ## Data

    - frame: 1447 rows, columns 8

    ## Training

    - model family: tabicl; MLflow run `a1d3a2b1b3f9434788b65c8788ca2a04`
    - processor type: not recorded

    ## Metrics

    | metric | tabicl_v2_in_context held-out | tabicl_v2_in_context validation | train_mean held-out |
    |---|---|---|---|
    | mae | 1.882 | 1.927 | 15.03 |
    | rmse | 2.804 | 2.989 | 17.28 |
    | skill_primary | 0.8748 |  |  |

    ## Flags

    - stored model does not reproduce its validation result

    ## Revalidation trigger

    - a new frozen dataset version
    """
)


def test_a_model_card_unit_keeps_the_decision_and_leaves_out_run_details():
    task, body = bu.model_card_body(CARD)
    assert task == "ccpp.output"
    assert body.startswith(
        "# Model card: ccpp.output\n\n_model `tabicl_v2_in_context`, primary metric `mae` (lower is better)_"
    )
    for kept in (
        "## Decision",
        "approved_with_conditions",
        "## Intended use",
        "## Flags",
        "## Revalidation trigger",
    ):
        assert kept in body
    for dropped in (
        "MLflow",
        "processor",
        "1447 rows",
        "Generated",
        "rmse",
        "## Training",
        "## Data",
    ):
        assert dropped not in body
    assert (
        "- mae: tabicl_v2_in_context held-out 1.882; tabicl_v2_in_context validation 1.927; train_mean held-out 15.03"
        in body
    )
    assert "- skill_primary: tabicl_v2_in_context held-out 0.8748" in body
    with pytest.raises(ValueError, match="no title"):
        bu.model_card_body("no card here")


def test_the_approval_rules_are_read_without_running_the_library(tmp_path):
    path = tmp_path / "rules.py"
    path.write_text(
        "APPROVAL_RULES = (\n"
        '    ("R01", "selection", "one model per task", "", ""),\n'
        '    ("R05", "blocking", ("evidence " "floor"), "min_rows", "30"),\n'
        '    ("R05", "blocking", "evidence floor", "min_blocks", "5"),\n'
        ")\n",
        encoding="utf-8",
    )
    rules = bu.read_approval_rules(path)
    assert [r["rule_id"] for r in rules] == ["R01", "R05"]
    assert rules[1]["parameters"] == ["min_rows 30", "min_blocks 5"]
    body = bu.approval_rule_body(rules[1])
    assert (
        "# Approval rule: R05" in body
        and "**Parameters:** min_rows 30, min_blocks 5" in body
    )
    other = tmp_path / "other.py"
    other.write_text("X = 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no APPROVAL_RULES"):
        bu.read_approval_rules(other)


def test_the_real_rule_table_gives_one_unit_per_rule_id():
    rules = bu.read_approval_rules(uc.ROOT / bu.APPROVAL_RULES_FILE)
    ids = [r["rule_id"] for r in rules]
    assert ids == sorted(set(ids)) and ids[0] == "R01" and "R14" in ids
    assert {"model_card", "approval_rule"} <= set(uc.KIND_DIRS)
    assert {"model_card", "approval_rule"} <= set(uc.DERIVED_KINDS)


# --- questions, thresholds, acceptance -------------------------------------------


def write_questions(path, holdout_cohort=True):
    rows = []
    for number in range(1, 13):
        rows.append(
            {
                "id": f"q{number:03d}",
                "class": "definitional_wording",
                "kind": "metric",
                "expected_unit_id": "metric.a",
                "text": f"question {number}?",
                "approval": {},
                **({"cohort": "extension"} if number > 6 else {}),
            }
        )
    import yaml

    data = {"corpus": "curated", "version": 1, "questions": rows}
    if holdout_cohort:
        data["holdout_cohort"] = "extension"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def test_only_the_holdout_cohort_can_be_holdout_and_the_cohort_is_not_in_the_hash(
    tmp_path,
):
    path = tmp_path / "curated.yml"
    write_questions(path)
    loaded = qs.load(path, {"metric.a"})
    base = [q for q in loaded.questions if q.cohort == "base"]
    new = [q for q in loaded.questions if q.cohort == "extension"]
    assert (
        all(q.split == qs.TUNE for q in base) and loaded.holdout_cohort == "extension"
    )
    assert {q.split for q in new} <= {qs.TUNE, qs.HOLDOUT}
    plain = tmp_path / "plain.yml"
    write_questions(plain, holdout_cohort=False)
    legacy = qs.load(plain, {"metric.a"})
    for question in legacy.questions:
        by_hash = int(uc.sha256_text(question.id)[:8], 16) % 3 == 2
        assert question.split == (qs.HOLDOUT if by_hash else qs.TUNE)
    assert loaded.questions[7].content_hash == legacy.questions[7].content_hash
    qs.save(tmp_path / "saved.yml", loaded)
    again = qs.load(tmp_path / "saved.yml", {"metric.a"})
    assert [q.cohort for q in again.questions] == [q.cohort for q in loaded.questions]
    assert again.holdout_cohort == "extension"


def outcome(question_id, cohort="base", rank=1, expected="u1", scores=None):
    return evaluation.Outcome(
        question_id,
        "definitional_wording",
        "metric",
        expected,
        "tune",
        ranks={"dense": rank, "dense:gte-modernbert-base": rank, "hybrid_graph": rank},
        no_match={
            "dense": False,
            "dense:gte-modernbert-base": False,
            "hybrid_graph": False,
        },
        expected_score=scores
        or {"dense": 0.5, "keyword": 2.0, "dense:gte-modernbert-base": 0.4},
        top_score={"dense": 0.6, "keyword": 3.0, "dense:gte-modernbert-base": 0.45},
        cohort=cohort,
    )


def test_metrics_follow_the_types_of_the_run_and_thresholds_cover_each_dense_retriever(
    tmp_path,
):
    outcomes = [outcome(f"q{n:03d}") for n in range(1, 6)]
    groups = evaluation.grouped(outcomes)
    assert set(groups["all"]) == {"dense", "dense:gte-modernbert-base", "hybrid_graph"}
    found = thresholds.establish(outcomes)
    assert set(found) == {"dense", "keyword", "dense:gte-modernbert-base"}
    assert found["dense:gte-modernbert-base"]["threshold"] == 0.4
    number, data = thresholds.write_version(
        tmp_path,
        "curated",
        RetrievalConfig(embedders=("gte-modernbert-base",)),
        found,
        {},
    )
    stored = RetrievalConfig.from_dict(data["config"])
    assert stored.extra_min_scores == {"gte-modernbert-base": 0.4}
    assert stored.dense_min_score == 0.5 and number == 1


def run_with_misses(misses, total=4):
    rows = []
    for n in range(total):
        rank = 9 if f"q{n}" in misses else 1
        rows.append(
            {
                "question_id": f"q{n}",
                "expected_unit_id": "u",
                "ranks": {acceptance.MODE: rank},
            }
        )
    return {
        "outcomes": rows,
        "metrics": {
            "all": {
                acceptance.MODE: {
                    "positive": total,
                    "recall_5": total - len(misses),
                    "negative": 0,
                    "no_match": 0,
                }
            }
        },
        "question_set": {"hash": "h"},
    }


def test_the_old_question_check_allows_one_additional_miss(tmp_path):
    holdout = run_with_misses(set(), 25)
    holdout["metrics"]["all"][acceptance.MODE].update(negative=5, no_match=5)
    baselines = (run_with_misses({"q0"}),)
    one = acceptance.decide(holdout, None, [], run_with_misses({"q0", "q1"}), baselines)
    check = next(c for c in one.checks if c["check"].startswith("old questions"))
    assert check["passed"] is True and "seen data" in check["detail"]
    two = acceptance.decide(
        holdout, None, [], run_with_misses({"q0", "q1", "q2"}), baselines
    )
    assert (
        next(c for c in two.checks if c["check"].startswith("old questions"))["passed"]
        is False
    )
    first = acceptance.decide(holdout, None, [], run_with_misses(set()), ())
    assert (
        next(c for c in first.checks if c["check"].startswith("old questions"))[
            "passed"
        ]
        is None
    )


# --- retrieval with a second dense retriever --------------------------------------

META = {
    "corpus": "curated",
    "section": "s",
    "version": 1,
    "source": "src",
    "last_updated": "2026-10-27",
    "content_hash": "h",
    "piece": 0,
}


class StubCollection:
    def __init__(self, texts):
        self.texts = texts

    def get(self, ids=None, where=None, include=None):
        ids = [i for i in ids if i in self.texts]
        return {
            "ids": ids,
            "documents": [self.texts[i] for i in ids],
            "metadatas": [{**META, "unit_id": i.rsplit("#", 1)[0]} for i in ids],
        }


def stub_store(texts):
    return SimpleNamespace(
        collection=StubCollection(texts),
        graph=SimpleNamespace(neighbours=lambda s, e: []),
        mode="approved",
    )


def searched(dense, keyword, extra_hits, extra_status="ok"):
    return rv.Searched(
        SimpleNamespace(text="query"),
        None,
        {},
        "ok",
        dense,
        "ok",
        keyword,
        {"gte-modernbert-base": (extra_status, extra_hits)},
    )


def test_the_second_dense_ranking_joins_the_fusion_and_every_retriever_must_answer():
    texts = {"a#0": "alpha", "b#0": "beta", "c#0": "gamma"}
    config = RetrievalConfig(
        embedders=("gte-modernbert-base",), final_k=2, related_max=0
    )
    found = searched(
        [("a#0", 0.9), ("b#0", 0.8)], [("b#0", 3.0)], [("c#0", 0.7), ("b#0", 0.6)]
    )
    direct, related, _status = rv.serve(stub_store(texts), found, config)
    assert [h.chunk_id for h in direct] == ["b#0", "a#0"] and related == []
    assert set(direct[0].ranks) == {"dense", "keyword", "dense:gte-modernbert-base"}
    empty = searched([("a#0", 0.9)], [("a#0", 1.0)], [])
    assert rv.serve(stub_store(texts), empty, config) == ([], [], "no_seeds")
    skipped = searched(
        [("a#0", 0.9)], [("a#0", 1.0)], [], extra_status="query_too_long"
    )
    assert [h.chunk_id for h in rv.serve(stub_store(texts), skipped, config)[0]] == [
        "a#0"
    ]


def test_the_reranker_reorders_by_cached_scores_and_reports_missing_scores():
    texts = {"a#0": "alpha", "b#0": "beta"}
    config = RetrievalConfig(rerank=True, rerank_candidates=5, final_k=2, related_max=0)
    found = searched(
        [("a#0", 0.9), ("b#0", 0.8)],
        [("a#0", 3.0), ("b#0", 2.0)],
        [("a#0", 0.5), ("b#0", 0.4)],
    )
    found.extra = {}
    scores = {
        rr.pair_key(config.rerank_model, "query", texts[c]): s
        for c, s in (("a#0", 0.1), ("b#0", 0.9))
    }
    direct, _, _ = rv.serve(stub_store(texts), found, config, scores)
    assert [h.chunk_id for h in direct] == ["b#0", "a#0"] and found.notes["rerank"][
        "status"
    ] == rr.RERANKED
    scores.popitem()
    direct, _, _ = rv.serve(stub_store(texts), found, config, scores)
    assert (
        found.notes["rerank"]["status"] == rr.MISSING
        and found.notes["rerank"]["missing"] == 1
    )
    with pytest.raises(ValueError, match="no score cache"):
        rv.serve(stub_store(texts), found, config, None)


def test_a_second_collection_needs_its_record_and_the_same_primary_build(tmp_path):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    corpus_dir.mkdir(parents=True)
    record = {"build_id": "p1"}
    manifest = {
        "embedding": record,
        "embedding_extra": {
            "gte-modernbert-base": {"primary_build_id": "p1", "collection": "c.g_1"}
        },
    }
    (corpus_dir / uc.MANIFEST_NAME).write_text(json.dumps(manifest), encoding="utf-8")
    found = st.extra_record_for("curated", corpus_dir, "gte-modernbert-base", record)
    assert found["collection"] == "c.g_1"
    with pytest.raises(ValueError, match="another primary build"):
        st.extra_record_for(
            "curated", corpus_dir, "gte-modernbert-base", {"build_id": "p2"}
        )
    with pytest.raises(ValueError, match="unknown extra embedder"):
        st.extra_record_for("curated", corpus_dir, "minilm", record)
    with pytest.raises(ValueError, match="no second dense collection"):
        st.extra_record_for("wikipedia", corpus_dir, "gte-modernbert-base", record)
    assert (
        extra.scoped_id("curated", emb.GTE_MODERNBERT) == "curated.gte-modernbert-base"
    )


# --- reranker pairs ---------------------------------------------------------------


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query):
        self.query = query

    def fetchall(self):
        return self.rows


class FakeConnection(FakeCursor):
    def cursor(self):
        return FakeCursor(self.rows)


def test_fetched_reranker_scores_are_merged_into_the_cache(tmp_path, monkeypatch):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    corpus_dir.mkdir(parents=True)
    rr.save_scores(rr.cache_path(tmp_path / "ai"), {"old": 0.5})
    monkeypatch.setattr(rp.wf, "connection_settings", lambda env=None: {"host": "h"})
    args = argparse.Namespace(corpus_dir=corpus_dir, apply=True, dry_run=False)

    def connect(settings):
        return FakeConnection([("old", 0.5), ("new", 1.5)])

    assert rp.cmd_fetch(args, connect=connect) == 0
    assert rr.load_scores(rr.cache_path(tmp_path / "ai")) == {"old": 0.5, "new": 1.5}
