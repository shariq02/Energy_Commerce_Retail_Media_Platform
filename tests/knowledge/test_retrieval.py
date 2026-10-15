"""Retrieval -- query processing, retrievers, fusion, graph, assembly, retrieve().

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder. The model is replaced by a fake (a vector
drawn from a hash of the text, word count for tokens), so a query equal to a chunk
text finds that chunk at cosine 1. ChromaDB, rank_bm25 and SQLite are the real
libraries. The Wikipedia fixture has 20 articles of two sections each.
"""

from __future__ import annotations

from hashlib import sha256

import pytest

from ai.embeddings import build as eb
from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import keyword_index as ki
from ai.embeddings.chunks import Chunk
from ai.rag import assembler as asm
from ai.rag import filters as flt
from ai.rag import fusion
from ai.rag import keyword_fts as fts
from ai.rag import query as qy
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from ai.rag.retrieve import retrieve
from ai.rag.schema import DIRECT, RELATED, Citation, Hit, citation_from
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from scripts.knowledge import wikipedia_corpus as wc
from tests.knowledge import test_embeddings as te

pytestmark = [pytest.mark.unit, pytest.mark.ai]

OTHER_SOURCE = [{"name": "ga4_export", "sha256": "e" * 64}]
SPECS = [
    (
        "contract.dwd",
        "contract",
        "energy",
        te.SOURCE,
        "# Contract: dwd\n\nWeather data.",
    ),
    (
        "rule.dwd.one",
        "rule",
        "energy",
        te.SOURCE,
        "# Rule: one\n\nQuarantine a row with an unknown cloud code.",
    ),
    ("metric.a", "metric", "energy", te.SOURCE, "# Metric: a\n\nAverage price a day."),
    ("metric.b", "metric", "energy", te.SOURCE, "# Metric: b\n\nPeak price a day."),
    (
        "metric.ga4_sessions",
        "metric",
        "commerce",
        OTHER_SOURCE,
        "# Metric: sessions\n\nCount of web sessions.",
    ),
    (
        "glossary.weather",
        "glossary",
        "energy",
        te.SOURCE,
        "# Glossary: weather\n\n- `QN_*`: DWD quality level, old scheme.\n",
    ),
]


@pytest.fixture(autouse=True)
def fake_model(monkeypatch):
    monkeypatch.setattr(emb, "Embedder", te.FakeEmbedder)
    monkeypatch.setattr(emb, "token_counter", lambda model_dir=None: te.words)
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(te.HASHES))
    yield
    st.clear_cache()


def build_curated(tmp_path):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    for unit_id, kind, ecosystem, source, body in SPECS:
        plan = uc.plan_unit(
            corpus_dir, unit_id, kind, ecosystem, source, body, te.TODAY
        )
        uc.write_text(plan.path, plan.text)
    au.approve_chosen(corpus_dir, uc.scan_units(corpus_dir), "Owner")
    argv = ["build", "--corpus", "curated", *te.args_for(corpus_dir, "--apply")]
    assert eb.main(argv) == 0
    record = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    return corpus_dir, record["build_id"]


def build_wikipedia(tmp_path, index=True):
    wiki = te.wikipedia_ready(tmp_path)
    corpus_dir = wiki.parent
    argv = ["build", "--corpus", "wikipedia", *te.args_for(corpus_dir, "--apply")]
    assert eb.main(argv) == 0
    if index:
        assert fts.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0
    record = te.read_manifest(wiki / wc.MANIFEST_NAME)[br.RECORD_KEY]
    return corpus_dir, record["build_id"]


def ask(query, corpus_dir, build_id, corpus="curated", **options):
    return retrieve(
        query, corpus=corpus, build_id=build_id, corpus_dir=corpus_dir, **options
    )


def test_config_holds_the_defaults_and_rejects_bad_values():
    config = RetrievalConfig()
    assert (config.dense_k, config.keyword_k, config.rrf_constant) == (20, 20, 60)
    assert (config.final_k, config.related_max) == (5, 3)
    assert config.dense_min_score is None and config.as_dict()["final_k"] == 5
    with pytest.raises(ValueError, match="final_k"):
        RetrievalConfig(final_k=0)
    with pytest.raises(ValueError, match="must not be negative"):
        RetrievalConfig(related_max=-1)


def test_citation_text_and_completeness():
    meta = {
        "corpus": "curated",
        "unit_id": "rule.dwd.one",
        "section": "Rule: one",
        "version": 2,
        "source": "src/x.yml",
        "last_updated": "2026-10-07",
        "content_hash": "a" * 64,
    }
    citation = citation_from("rule.dwd.one#0", meta)
    assert citation.text() == "[rule.dwd.one@2 §Rule: one]"
    assert citation.licence == ""
    for name in ("section", "content_hash", "version"):
        broken = {key: value for key, value in meta.items() if key != name}
        with pytest.raises(ValueError, match=f"citation field {name} is empty"):
            citation_from("rule.dwd.one#0", broken)


def test_filters_are_checked_per_corpus_and_become_where_clauses():
    assert flt.normalise({"kind": ["rule", "metric", "rule"]}, "curated") == {
        "kind": ["metric", "rule"]
    }
    with pytest.raises(ValueError, match="not allowed for curated"):
        flt.normalise({"title": "x"}, "curated")
    assert flt.normalise({"title": "x"}, "wikipedia") == {"title": ["x"]}
    with pytest.raises(ValueError, match="no value"):
        flt.normalise({"kind": []}, "curated")
    assert flt.to_where({}) is None
    assert flt.to_where({"kind": ["rule"]}) == {"kind": "rule"}
    both = flt.to_where({"kind": ["a", "b"], "ecosystem": ["energy"]})
    assert both == {"$and": [{"ecosystem": "energy"}, {"kind": {"$in": ["a", "b"]}}]}
    assert flt.matches({"kind": "a", "x": 1}, {"kind": ["a", "b"]})
    assert not flt.matches({"kind": "c"}, {"kind": ["a", "b"]})


def test_fusion_scores_by_reciprocal_rank_and_breaks_ties_by_chunk_id():
    fused = fusion.fuse({"dense": ["a", "b"], "keyword": ["b", "c"]}, 60, 5)
    assert [chunk_id for chunk_id, _, _ in fused] == ["b", "a", "c"]
    assert fused[0][1] == pytest.approx(1 / 62 + 1 / 61)
    assert fused[0][2] == {"dense": 2, "keyword": 1}
    tie = fusion.fuse({"keyword": ["y"], "dense": ["x"]}, 60, 5)
    assert [chunk_id for chunk_id, _, _ in tie] == ["x", "y"]
    assert len(fusion.fuse({"dense": ["a", "b", "c"], "keyword": []}, 60, 2)) == 2


def test_query_processing_normalises_detects_identifiers_and_expands_terms():
    assert qy.normalise("  Wind  power \n now ") == "Wind power now"
    ids = {"rule.dwd.one", "metric.ga4_sessions"}
    found = qy.process("What is  rule.dwd.one?", ids, [], 8)
    assert found.text == "What is rule.dwd.one?"
    assert found.identifiers == ("rule.dwd.one",)
    assert {"rule", "dwd", "one"} <= set(found.terms)
    by_key = qy.process("GA4_Sessions trend", ids, [], 8)
    assert by_key.identifiers == ("metric.ga4_sessions",)
    assert {"ga4_sessions", "trend", "metric", "ga4", "sessions"} <= set(by_key.terms)
    assert qy.process("unknown.thing", ids, [], 8).identifiers == ()
    glossary = qy.glossary_entries(["- `QN_*`: DWD quality level, old scheme.\nplain"])
    expanded = qy.process("what is qn_8", ids, glossary, 8)
    assert expanded.expansions == ("dwd", "quality", "level")
    assert qy.process("what is qn_8", ids, glossary, 2).expansions == ("dwd", "quality")
    assert qy.process("hello", ids, glossary, 8).expansions == ()
    assert qy.process("   ", ids, glossary, 8).terms == ()


def test_keyword_file_is_read_as_a_stream(tmp_path, monkeypatch):
    chunks = [
        Chunk("a#0", "Wind power plant", {}),
        Chunk("b#0", "Grid price", {}),
        Chunk("c#0", "Weather", {}),
    ]
    path = tmp_path / "k.json"
    ki.write_index(path, chunks)
    monkeypatch.setattr(fts, "READ_BLOCK", 7)
    items = list(fts.read_keyword_file(path))
    assert items[0] == ["a#0", "b#0", "c#0"]
    assert items[1:] == [["wind", "power", "plant"], ["grid", "price"], ["weather"]]
    path.write_text('{"chunk_ids":["a#0"],"tokens":[["wind"', encoding="utf-8")
    with pytest.raises(ValueError, match="file ends inside a list"):
        list(fts.read_keyword_file(path))
    path.write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(ValueError, match="not a keyword file"):
        list(fts.read_keyword_file(path))


def test_curated_keyword_retrieval_returns_cited_chunks_and_is_deterministic(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    first = ask("unknown cloud code", corpus_dir, build_id)
    again = ask("unknown cloud code", corpus_dir, build_id)
    assert first.to_dict() == again.to_dict()
    assert first.status == "ok" and first.hits[0].chunk_id == "rule.dwd.one#0"
    top = first.hits[0]
    assert top.role == DIRECT and "keyword" in top.ranks and "dense" in top.ranks
    assert top.citation.text() == "[rule.dwd.one@1 §Rule: one]"
    assert top.citation.corpus == "curated" and len(top.citation.content_hash) == 64
    assert first.build_id == build_id and first.corpus == "curated"
    assert first.config == RetrievalConfig().as_dict()
    assert first.retrievers["dense"] == {"status": "ok", "hits": 6}
    assert first.retrievers["keyword"] == {"status": "ok", "hits": 1}
    assert first.processed["terms"][:3] == ["unknown", "cloud", "code"]
    assert f"citation={top.citation.text()}" in first.context


def test_a_keyword_hit_needs_a_matching_term(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    result = ask("zebra", corpus_dir, build_id)
    assert result.retrievers["keyword"] == {"status": "ok", "hits": 0}
    assert result.retrievers["dense"]["hits"] == 6
    assert result.status == "no_match" and result.hits == []


def test_filters_apply_before_ranking_in_every_retriever(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    metrics = ask("price", corpus_dir, build_id, filters={"kind": "metric"})
    assert {hit.chunk_id.split("#")[0][:7] for hit in metrics.hits} == {"metric."}
    assert metrics.retrievers["dense"]["hits"] == 3
    assert metrics.retrievers["keyword"]["hits"] == 2
    commerce = ask("sessions", corpus_dir, build_id, filters={"ecosystem": "commerce"})
    assert [hit.chunk_id for hit in commerce.hits] == ["metric.ga4_sessions#0"]
    assert commerce.filters == {"ecosystem": ["commerce"]}
    with pytest.raises(ValueError, match="not allowed for curated"):
        ask("price", corpus_dir, build_id, filters={"title": "x"})


def test_no_hit_from_any_retriever_gives_no_match(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    none = ask("price", corpus_dir, build_id, filters={"kind": "governance"})
    assert none.status == "no_match" and none.hits == [] and none.context == ""
    empty = ask("   ", corpus_dir, build_id)
    assert empty.status == "no_match"
    assert empty.retrievers["dense"]["status"] == "empty_query"
    assert empty.retrievers["keyword"]["status"] == "no_terms"


def test_a_threshold_discards_weak_hits(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    config = RetrievalConfig(dense_min_score=2.0, keyword_min_score=1e9)
    result = ask("unknown cloud code", corpus_dir, build_id, config=config)
    assert result.status == "no_match"
    assert result.retrievers["dense"]["hits"] == 0
    assert result.retrievers["keyword"]["hits"] == 0


def test_a_long_query_stops_the_dense_retriever_only(tmp_path, monkeypatch):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    monkeypatch.setattr(emb, "MAX_TOKENS", 3)
    result = ask("unknown cloud code quarantine", corpus_dir, build_id)
    assert result.retrievers["dense"] == {"status": "query_too_long", "hits": 0}
    assert result.retrievers["keyword"]["hits"] >= 1
    assert result.hits[0].chunk_id == "rule.dwd.one#0" and result.status == "ok"


def test_graph_adds_linked_units_as_related_hits(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    config = RetrievalConfig(final_k=1)
    result = ask("unknown cloud code", corpus_dir, build_id, config=config)
    direct = [hit for hit in result.hits if hit.role == DIRECT]
    related = [hit for hit in result.hits if hit.role == RELATED]
    assert [hit.chunk_id for hit in direct] == ["rule.dwd.one#0"]
    assert [hit.chunk_id for hit in related] == [
        "contract.dwd#0",
        "glossary.weather#0",
        "metric.a#0",
    ]
    assert related[0].link_type == "rule_of" and related[0].via_unit == "rule.dwd.one"
    assert related[0].fused_score is None and related[0].link_weight == 1.0
    assert result.retrievers["graph"] == {"status": "ok", "hits": 3}
    no_graph = RetrievalConfig(final_k=1, related_max=0)
    off = ask("unknown cloud code", corpus_dir, build_id, config=no_graph)
    assert off.retrievers["graph"]["status"] == "disabled" and len(off.hits) == 1
    kinds = ask(
        "unknown cloud code",
        corpus_dir,
        build_id,
        config=config,
        filters={"kind": ["rule", "metric"]},
    )
    assert [hit.chunk_id for hit in kinds.hits if hit.role == RELATED] == [
        "metric.a#0",
        "metric.b#0",
    ]


def test_identifier_and_glossary_terms_reach_the_keyword_retriever(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    result = ask("metric.ga4_sessions", corpus_dir, build_id)
    assert result.processed["identifiers"] == ["metric.ga4_sessions"]
    assert "ga4" in result.processed["terms"]
    assert "metric.ga4_sessions#0" in [hit.chunk_id for hit in result.hits]
    glossary = ask("what is qn_8", corpus_dir, build_id)
    assert glossary.processed["expansions"] == ["dwd", "quality", "level"]


def test_retrieval_refuses_another_build_and_a_missing_build_id(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    with pytest.raises(ValueError, match="not the recorded build"):
        ask("price", corpus_dir, "0" * 12)
    with pytest.raises(ValueError, match="no build of curated is designated"):
        ask("price", corpus_dir, None)
    with pytest.raises(FileNotFoundError, match="run the corpus build first"):
        ask("price", tmp_path / "nowhere", build_id)


def test_retrieval_checks_the_model_files_the_keyword_file_and_the_graph(
    tmp_path, monkeypatch
):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    changed = {**te.HASHES, "tokenizer_sha256": "f" * 64}
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(changed))
    with pytest.raises(ValueError, match="model files differ from the build record"):
        ask("price", corpus_dir, build_id)
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(te.HASHES))
    graph = corpus_dir.parent / "knowledge_graph" / "edges.jsonl"
    graph.write_text(graph.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="the graph differs"):
        ask("price", corpus_dir, build_id)
    keyword = br.keyword_path(corpus_dir.parent, "curated")
    keyword.write_text(keyword.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="keyword index file differs"):
        ask("price", corpus_dir, build_id)


def test_a_chunk_without_a_complete_citation_is_an_error(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    record = te.read_manifest(corpus_dir / uc.MANIFEST_NAME)[br.RECORD_KEY]
    client = coll.open_client(br.chroma_dir(corpus_dir.parent))
    stored = client.get_collection(record["collection"], embedding_function=None)
    meta = stored.get(ids=["rule.dwd.one#0"], include=["metadatas"])["metadatas"][0]
    stored.update(ids=["rule.dwd.one#0"], metadatas=[{**meta, "section": ""}])
    with pytest.raises(ValueError, match="citation field section is empty"):
        ask("unknown cloud code", corpus_dir, build_id)


def test_retrieval_does_not_change_the_stored_files(tmp_path):
    pytest.importorskip("rank_bm25")
    corpus_dir, build_id = build_curated(tmp_path)
    files = [
        br.keyword_path(corpus_dir.parent, "curated"),
        corpus_dir.parent / "knowledge_graph" / "nodes.jsonl",
        corpus_dir.parent / "knowledge_graph" / "edges.jsonl",
        corpus_dir / uc.MANIFEST_NAME,
    ]
    before = [path.read_bytes() for path in files]
    result = ask("unknown cloud code", corpus_dir, build_id)
    assert [path.read_bytes() for path in files] == before
    assert result.collection == f"curated_{build_id}"


def test_the_assembler_merges_one_unit_and_keeps_every_chunk_id_and_hash():
    def hit(chunk_id, piece, text, role=DIRECT):
        unit = chunk_id.split("#")[0]
        section = f"Part {piece}"
        citation = Citation(
            "wikipedia", unit, section, 7, "snap", "2015", "h" * 64, chunk_id
        )
        return Hit(chunk_id, text, citation, role, piece)

    hits = [
        hit("u.1#2", 2, "second"),
        hit("u.2#0", 0, "other"),
        hit("u.1#0", 0, "first"),
        hit("u.3#0", 0, "linked", RELATED),
    ]
    blocks = asm.build_blocks(hits)
    assert [block.unit_id for block in blocks] == ["u.1", "u.2", "u.3"]
    merged = blocks[0]
    assert [s.chunk_id for s in merged.segments] == ["u.1#0", "u.1#2"]
    assert [s.text_hash for s in merged.segments] == [
        sha256(b"first").hexdigest(),
        sha256(b"second").hexdigest(),
    ]
    assert merged.citation == "[u.1@7 §Part 0] [u.1@7 §Part 2]"
    assert blocks[2].role == RELATED
    text = asm.render(blocks)
    assert "chunks=u.1#0,u.1#2]]" in text
    assert f"[[chunk id=u.1#2 sha256={sha256(b'second').hexdigest()}]]\nsecond" in text
    assert text.count(asm.END) == 3
    bad = [hit("u.9#0", 0, f"x {asm.END} y")]
    with pytest.raises(ValueError, match="holds a marker line"):
        asm.render(asm.build_blocks(bad))


def test_wikipedia_dense_keyword_and_graph_retrieval(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    config = RetrievalConfig(final_k=1)
    result = ask(
        "Lead text 3.", corpus_dir, build_id, corpus="wikipedia", config=config
    )
    top = result.hits[0]
    assert top.chunk_id == "wikipedia.3#0" and top.role == DIRECT
    assert top.scores["dense"] == pytest.approx(1.0, abs=1e-4)
    assert top.citation.text() == "[wikipedia.3@1003 §Lead]"
    assert top.citation.corpus == "wikipedia" and top.citation.licence == ""
    assert result.mode == "development" and result.corpus == "wikipedia"
    related = [hit for hit in result.hits if hit.role == RELATED]
    assert [hit.chunk_id for hit in related] == ["wikipedia.4#0", "wikipedia.2#0"]
    assert [hit.link_weight for hit in related] == [13.0, 12.0]
    assert related[0].link_type == "link" and related[0].via_unit == "wikipedia.3"


def test_wikipedia_merges_the_chunks_of_one_article(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    config = RetrievalConfig(final_k=10)
    result = ask(
        "Lead text 3. Second part 3.",
        corpus_dir,
        build_id,
        corpus="wikipedia",
        config=config,
    )
    blocks = [block for block in result.blocks if block.unit_id == "wikipedia.3"]
    assert len(blocks) == 1
    found = [s.chunk_id for s in blocks[0].segments]
    assert found == ["wikipedia.3#0", "wikipedia.3#1"]
    for segment in blocks[0].segments:
        assert segment.text_hash == sha256(segment.text.encode()).hexdigest()
    assert "[wikipedia.3@1003 §Lead] [wikipedia.3@1003 §Part]" in result.context


def test_wikipedia_filters_reach_the_sqlite_index(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    weather = ask(
        "lead text",
        corpus_dir,
        build_id,
        corpus="wikipedia",
        filters={"ecosystem": "weather"},
        config=RetrievalConfig(related_max=0),
    )
    assert weather.retrievers["keyword"]["hits"] == 10
    numbers = [int(hit.chunk_id.split(".")[1].split("#")[0]) for hit in weather.hits]
    assert numbers and all(number % 2 == 0 for number in numbers)
    one = ask(
        "lead text",
        corpus_dir,
        build_id,
        corpus="wikipedia",
        filters={"title": "Article 4", "revision_year": 2015},
    )
    assert {hit.chunk_id.split("#")[0] for hit in one.hits if hit.role == DIRECT} == {
        "wikipedia.4"
    }
    assert one.retrievers["keyword"]["hits"] == 1


def test_wikipedia_needs_its_index_and_refuses_production_use(tmp_path):
    corpus_dir, build_id = build_wikipedia(tmp_path, index=False)
    with pytest.raises(ValueError, match="no FTS index file"):
        ask("lead", corpus_dir, build_id, corpus="wikipedia")
    assert fts.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0
    assert ask("lead", corpus_dir, build_id, corpus="wikipedia").status == "ok"
    with pytest.raises(ValueError, match="development corpus: refused in production"):
        ask("lead", corpus_dir, build_id, corpus="wikipedia", production=True)


def test_the_index_command_plans_skips_and_checks_its_input(tmp_path, capsys):
    corpus_dir, _ = build_wikipedia(tmp_path, index=False)
    path = fts.index_path(corpus_dir.parent)
    assert fts.main(["--corpus-dir", str(corpus_dir), "--dry-run"]) == 0
    assert not path.exists() and "new build" in capsys.readouterr().out
    assert fts.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0
    assert path.is_file() and "(40 chunks)" in capsys.readouterr().out
    assert fts.read_meta(path)["chunk_count"] == "40"
    assert fts.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0
    assert "Nothing to build." in capsys.readouterr().out
    keyword = br.keyword_path(corpus_dir.parent, "wikipedia")
    keyword.write_text(keyword.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="keyword file differs from the build record"):
        fts.main(["--corpus-dir", str(corpus_dir), "--apply"])


def test_the_index_command_needs_a_build_record(tmp_path):
    wiki = te.wikipedia_ready(tmp_path)
    with pytest.raises(ValueError, match="has no build record"):
        fts.main(["--corpus-dir", str(wiki.parent), "--apply"])


def test_a_result_is_served_only_when_every_retriever_that_ran_finds_something(
    tmp_path, monkeypatch
):
    corpus_dir, build_id = build_wikipedia(tmp_path)
    agreed = ask("Lead text 3.", corpus_dir, build_id, corpus="wikipedia")
    assert agreed.status == "ok" and agreed.hits
    disagreed = ask("volcano lava", corpus_dir, build_id, corpus="wikipedia")
    assert disagreed.retrievers["dense"] == {"status": "ok", "hits": 20}
    assert disagreed.retrievers["keyword"] == {"status": "ok", "hits": 0}
    assert disagreed.status == "no_match" and disagreed.hits == []
    assert disagreed.retrievers["graph"]["hits"] == 0
    monkeypatch.setattr(emb, "MAX_TOKENS", 3)
    long_query = ask("lead text 3 part", corpus_dir, build_id, corpus="wikipedia")
    assert long_query.retrievers["dense"]["status"] == "query_too_long"
    assert long_query.status == "ok" and long_query.hits
