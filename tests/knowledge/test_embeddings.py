"""Embedding build -- chunker, readers, indexes, graph, build record and lifecycle.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder. The model is replaced by a fake (a
vector drawn from a hash of the text, word count for tokens); ChromaDB is the real
library. The Wikipedia chunk file is written here as notebook 05 would write it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil

import numpy as np
import pytest

from ai.embeddings import build as eb
from ai.embeddings import build_record as br
from ai.embeddings import chunks as ch
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import keyword_index as ki
from ai.embeddings import knowledge_graph as kg
from ai.embeddings import progress as pg
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from scripts.knowledge import run_all as ra
from scripts.knowledge import wikipedia_corpus as wc
from tests.knowledge.test_wikipedia_corpus import make_corpus

pytestmark = [pytest.mark.unit, pytest.mark.ai]

REAL_MODEL_HASHES = emb.model_hashes

TODAY = "2026-10-13"
SOURCE = [{"path": "src/schemas/contracts/dwd.yml", "sha256": "a" * 64}]
HASHES = {
    "archive_sha256": None,
    "model_onnx_sha256": "b" * 64,
    "tokenizer_sha256": "c" * 64,
}


def words(text: str) -> int:
    return len(text.split())


def fake_vector(text: str) -> np.ndarray:
    seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
    vector = np.random.default_rng(seed).standard_normal(emb.DIMENSION)
    return (vector / np.linalg.norm(vector)).astype(np.float32)


class FakeEmbedder:
    def __init__(self, model_dir=None):
        pass

    def embed(self, texts):
        return np.array([fake_vector(text) for text in texts], dtype=np.float32)

    def vectors_for(self, chunks):
        return self.embed([chunk.text for chunk in chunks])


@pytest.fixture(autouse=True)
def fake_model(monkeypatch):
    monkeypatch.setattr(emb, "Embedder", FakeEmbedder)
    monkeypatch.setattr(emb, "token_counter", lambda model_dir=None: words)
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(HASHES))


def make_units(corpus_dir, specs, approve=True):
    for unit_id, kind, body, source in specs:
        plan = uc.plan_unit(corpus_dir, unit_id, kind, "energy", source, body, TODAY)
        uc.write_text(plan.path, plan.text)
    units = uc.scan_units(corpus_dir)
    if approve:
        au.approve_chosen(corpus_dir, units, "Owner")
    else:
        uc.write_manifest(corpus_dir)
    return units


SPECS = [
    ("contract.dwd", "contract", "# Contract: dwd\n\nWeather data.", SOURCE),
    ("rule.dwd.one", "rule", "# Rule: one\n\nA first rule.", SOURCE),
    (
        "rule.mastr.two",
        "rule",
        "# Rule: two\n\nNo contract.",
        [{"name": "x", "sha256": "d" * 64}],
    ),
    ("metric.a", "metric", "# Metric: a\n\n**Grain:** day x area", SOURCE),
    ("metric.b", "metric", "# Metric: b\n\n**Grain:** day x area", SOURCE),
]


@pytest.fixture
def curated(tmp_path):
    corpus_dir = tmp_path / "ai" / "knowledge_corpus"
    make_units(corpus_dir, SPECS)
    return corpus_dir


def args_for(corpus_dir, *extra):
    return ["--corpus-dir", str(corpus_dir), *extra]


def read_manifest(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_pack_joins_parts_that_fit_and_never_overlaps(monkeypatch):
    monkeypatch.setattr(ch, "MAX_TOKENS", 4)
    dropped = []
    pieces = ch.pack(["a b", "c d", "e f g h i j", "k"], " ", words, dropped.append)
    assert pieces == ["a b c d", "k"]
    assert dropped == ["e f g h i j"]


def test_curated_unit_that_fits_is_one_chunk_with_metadata(curated):
    unit = uc.scan_units(curated)[0]
    (chunk,) = ch.curated_chunks(unit, words)
    assert chunk.chunk_id == "contract.dwd#0" and chunk.text == unit.body
    assert chunk.metadata["section"] == "Contract: dwd"
    assert chunk.metadata["source"] == "src/schemas/contracts/dwd.yml"
    assert chunk.metadata["content_hash"] == unit.content_hash


def test_long_curated_unit_splits_at_paragraphs_and_a_long_paragraph_fails(
    curated, monkeypatch
):
    monkeypatch.setattr(ch, "MAX_TOKENS", 6)
    unit = uc.scan_units(curated)[0]
    unit.body = "# T\n\none two three\n\nfour five six\n\nseven eight nine"
    pieces = ch.curated_chunks(unit, words)
    assert [p.text for p in pieces] == [
        "# T\n\none two three",
        "four five six\n\nseven eight nine",
    ]
    assert [p.chunk_id for p in pieces] == ["contract.dwd#0", "contract.dwd#1"]
    unit.body = "# T\n\n" + " ".join(["w"] * 9)
    with pytest.raises(ValueError, match="a paragraph has 9 tokens"):
        ch.curated_chunks(unit, words)


def test_read_curated_stops_on_an_unapproved_unit_or_a_manifest_mismatch(tmp_path):
    pending = tmp_path / "pending"
    make_units(pending, SPECS, approve=False)
    with pytest.raises(ValueError, match="not approved"):
        ch.read_curated(pending, words)
    approved = tmp_path / "approved"
    make_units(approved, SPECS)
    corpus = ch.read_curated(approved, words)
    assert corpus.unit_count == 5 and len(corpus.chunks) == 5
    manifest = read_manifest(approved / uc.MANIFEST_NAME)
    manifest["fingerprint"] = "0" * 64
    (approved / uc.MANIFEST_NAME).write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="differ from the manifest"):
        ch.read_curated(approved, words)


def test_keyword_index_round_trip(tmp_path):
    assert ki.tokenize("Wind_Onshore, QN_8: 12.5!") == [
        "wind_onshore",
        "qn_8",
        "12",
        "5",
    ]
    pytest.importorskip("rank_bm25")
    chunks = [
        ch.Chunk("a#0", "wind power plant", {}),
        ch.Chunk("b#0", "solar panel roof", {}),
        ch.Chunk("c#0", "grid price day", {}),
        ch.Chunk("d#0", "hourly weather station", {}),
    ]
    path = tmp_path / "keyword" / "x.json"
    version = ki.write_index(path, chunks)
    assert version == uc.sha256_text(path.read_text(encoding="utf-8"))
    ids, index = ki.load_index(path)
    scores = index.get_scores(ki.tokenize("solar"))
    assert ids[int(np.argmax(scores))] == "b#0"


def test_curated_graph_edges_follow_the_headers(curated):
    nodes, edges, orphans = kg.curated_graph(uc.scan_units(curated))
    assert len(nodes) == 5
    found = {(e["source"], e["target"], e["type"]) for e in edges}
    assert ("rule.dwd.one", "contract.dwd", "rule_of") in found
    assert ("metric.a", "metric.b", "same_grain") in found
    assert ("contract.dwd", "rule.dwd.one", "same_source") in found
    assert orphans == ["rule.mastr.two"]
    written = kg.write_graph(curated.parent / "knowledge_graph", nodes, edges)
    assert written["version"] == kg.graph_version(curated.parent / "knowledge_graph")


def test_dry_run_writes_nothing_and_does_not_load_the_model(curated, capsys):
    assert (
        eb.main(["build", "--corpus", "curated", *args_for(curated, "--dry-run")]) == 0
    )
    assert not (curated.parent / "vector_store").exists()
    assert not (curated.parent / "knowledge_graph").exists()
    assert "new build" in capsys.readouterr().out


def test_build_verify_and_rebuild(curated, capsys):
    argv = ["--corpus", "curated", *args_for(curated, "--apply")]
    assert eb.main(["build", *argv]) == 0
    manifest = read_manifest(curated / uc.MANIFEST_NAME)
    record = manifest[br.RECORD_KEY]
    assert record["chunk_count"] == 5 and record["last_known_good"] is None
    assert record["model"]["model_onnx_sha256"] == HASHES["model_onnx_sha256"]
    assert manifest["unit_count"] == 5
    client = coll.open_client(br.chroma_dir(curated.parent))
    assert coll.names(client, "curated") == [record["collection"]]
    assert eb.main(["verify", "--corpus", "curated", *args_for(curated)]) == 0

    capsys.readouterr()
    assert eb.main(["build", *argv]) == 0
    assert "Nothing to build." in capsys.readouterr().out

    make_units(
        curated,
        [("rule.dwd.one", "rule", "# Rule: one\n\nA changed rule.", SOURCE)],
    )
    assert eb.main(["build", *argv]) == 0
    newer = read_manifest(curated / uc.MANIFEST_NAME)[br.RECORD_KEY]
    assert newer["collection"] != record["collection"]
    assert newer["last_known_good"] == record["collection"]
    assert len(coll.names(client, "curated")) == 2
    ai_dir = curated.parent
    for kept in (record, newer):
        folder = br.build_dir(ai_dir, "curated", kept["build_id"])
        assert kept["keyword_index"]["file"] == br.keyword_file(
            "curated", kept["build_id"]
        )
        assert br.read_build_record(ai_dir, "curated", kept["build_id"]) == kept
        assert (folder / br.KEYWORD_FILE).is_file()
        assert (folder / "graph" / kg.EDGES_FILE).is_file()


def test_older_build_folders_are_pruned_with_their_collections(curated):
    argv = ["--corpus", "curated", *args_for(curated, "--apply")]
    ids = []
    for text in ("A second rule.", "A third rule.", "A fourth rule."):
        make_units(
            curated, [("rule.dwd.one", "rule", f"# Rule: one\n\n{text}", SOURCE)]
        )
        assert eb.main(["build", *argv]) == 0
        record = read_manifest(curated / uc.MANIFEST_NAME)[br.RECORD_KEY]
        ids.append(record["build_id"])
    root = br.builds_root(curated.parent, "curated")
    assert sorted(p.name for p in root.iterdir()) == sorted(ids[1:])


def test_migrate_moves_a_build_made_before_the_build_folders(curated, capsys):
    argv = ["--corpus", "curated", *args_for(curated, "--apply")]
    assert eb.main(["build", *argv]) == 0
    manifest_file = curated / uc.MANIFEST_NAME
    record = read_manifest(manifest_file)[br.RECORD_KEY]
    ai_dir = curated.parent
    folder = br.build_dir(ai_dir, "curated", record["build_id"])
    old = ai_dir / "vector_store" / "data" / "keyword" / "curated.json"
    old.parent.mkdir(parents=True)
    shutil.move(folder / br.KEYWORD_FILE, old)
    shutil.rmtree(folder)
    legacy = "vector_store/data/keyword/curated.json"
    old_index = {**record["keyword_index"], "file": legacy}
    br.write_manifest_record(manifest_file, {**record, "keyword_index": old_index})
    with pytest.raises(ValueError, match="build migrate"):
        eb.main(["build", *argv])
    assert eb.main(["migrate", *argv]) == 0
    assert read_manifest(manifest_file)[br.RECORD_KEY] == record
    assert (folder / br.KEYWORD_FILE).is_file() and not old.exists()
    assert br.read_build_record(ai_dir, "curated", record["build_id"]) == record
    assert eb.main(["verify", "--corpus", "curated", *args_for(curated)]) == 0
    capsys.readouterr()
    assert eb.main(["migrate", *argv]) == 0
    assert "Nothing to move" in capsys.readouterr().out


def test_verify_finds_a_changed_index_and_a_missing_record(curated, capsys):
    assert eb.main(["verify", "--corpus", "curated", *args_for(curated)]) == 1
    assert "no build record" in capsys.readouterr().out
    eb.main(["build", "--corpus", "curated", *args_for(curated, "--apply")])
    record = read_manifest(curated / uc.MANIFEST_NAME)[br.RECORD_KEY]
    keyword = br.keyword_path(curated.parent, "curated", record["build_id"])
    keyword.write_text(keyword.read_text(encoding="utf-8") + " ", encoding="utf-8")
    assert eb.main(["verify", "--corpus", "curated", *args_for(curated)]) == 1
    assert "keyword index file differs" in capsys.readouterr().out


def test_changed_model_files_stop_the_build_unless_asked(curated, monkeypatch):
    argv = ["--corpus", "curated", *args_for(curated, "--apply")]
    eb.main(["build", *argv])
    changed = {**HASHES, "model_onnx_sha256": "e" * 64}
    monkeypatch.setattr(emb, "model_hashes", lambda model_dir=None: dict(changed))
    with pytest.raises(ValueError, match="model files differ from the manifest"):
        eb.main(["build", *argv])
    assert eb.main(["build", *argv, "--new-model"]) == 0
    record = read_manifest(curated / uc.MANIFEST_NAME)[br.RECORD_KEY]
    assert record["model"]["model_onnx_sha256"] == "e" * 64


def test_a_failed_fill_leaves_no_partial_collection(curated):
    client = coll.open_client(br.chroma_dir(curated.parent))
    corpus = ch.read_curated(curated, words)

    class Failing:
        def vectors_for(self, chunks):
            raise RuntimeError("model failed")

    with pytest.raises(RuntimeError):
        coll.fill(
            client, "curated_x", corpus.chunks, Failing(), progress=lambda _: None
        )
    assert coll.names(client, "curated") == []


def wikipedia_ready(tmp_path, approve=True):
    wiki = make_corpus(tmp_path / "ai" / "knowledge_corpus")
    lines = [
        {"click_count": 10 + n, "source_id": n, "target_id": n + 1} for n in range(1, 6)
    ]
    text = "".join(json.dumps(line, sort_keys=True) + "\n" for line in lines)
    uc.write_text(wiki / wc.EDGES_NAME, text)
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    if approve:
        wc.main(["approve", "--approver", "Owner", "--wiki-dir", str(wiki), "--apply"])
        write_chunk_file(wiki)
    return wiki


def write_chunk_file(
    wiki, vector=fake_vector, model=HASHES, content_hash=None, leave_out=()
):
    """One chunk per section, as notebook 05 writes them, in file order."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    articles, _ = wc.scan_shards(wiki / wc.SHARD_DIR_NAME)
    rows = []
    for article in sorted(articles, key=lambda a: int(a["unit_id"].split(".")[1])):
        if article["unit_id"] in leave_out:
            continue
        for piece, section in enumerate(article["sections"]):
            text = section["text"]
            rows.append(
                {
                    "chunk_id": f"{article['unit_id']}#{piece}",
                    "article_id": int(article["unit_id"].split(".")[1]),
                    "piece": piece,
                    "section": section["section"],
                    "section_index": section["index"],
                    "text": text,
                    "token_count": words(text),
                    "text_hash": uc.sha256_text(text),
                    "article_content_hash": content_hash or article["content_hash"],
                    "model_onnx_sha256": model["model_onnx_sha256"],
                    "tokenizer_sha256": model["tokenizer_sha256"],
                    "vector": vector(text).tolist(),
                }
            )
    pq.write_table(pa.Table.from_pylist(rows), wiki / wc.CHUNKS_NAME)


def test_wikipedia_build_keeps_its_record_through_a_manifest_rebuild(tmp_path):
    wiki = wikipedia_ready(tmp_path)
    corpus_dir = wiki.parent
    argv = ["--corpus", "wikipedia", *args_for(corpus_dir, "--apply")]
    assert eb.main(["build", *argv]) == 0
    record = read_manifest(wiki / wc.MANIFEST_NAME)[br.RECORD_KEY]
    assert record["unit_count"] == 20 and record["chunk_count"] == 40
    assert record["mode"] == "development"
    assert record["vectors"]["made_in"] == "databricks"
    assert record["vectors"]["parity_lowest_cosine"] == pytest.approx(1.0)
    assert record["graph"] == {
        "nodes": 20,
        "edges": 5,
        "file": wc.EDGES_NAME,
        "version": uc.sha256_file(wiki / wc.EDGES_NAME),
    }
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    assert read_manifest(wiki / wc.MANIFEST_NAME)[br.RECORD_KEY] == record
    assert eb.main(["verify", "--corpus", "wikipedia", *args_for(corpus_dir)]) == 0


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"content_hash": "0" * 64}, "not cut from the approved article text"),
        ({"model": {**HASHES, "model_onnx_sha256": "f" * 64}}, "other model files"),
        ({"vector": lambda text: fake_vector("other " + text)}, "disagree"),
    ],
)
def test_wikipedia_chunks_must_match_the_shards_and_the_model(
    tmp_path, change, message
):
    wiki = wikipedia_ready(tmp_path)
    write_chunk_file(wiki, **change)
    argv = ["build", "--corpus", "wikipedia", *args_for(wiki.parent, "--apply")]
    with pytest.raises(ValueError, match=message):
        eb.main(argv)
    assert not br.chroma_dir(wiki.parent.parent).exists()


def test_stored_vectors_must_follow_the_chunk_order(tmp_path):
    wiki = wikipedia_ready(tmp_path)
    corpus = ch.read_wikipedia(wiki)
    vectors = ch.StoredVectors(corpus.vector_file)
    first = vectors.vectors_for(corpus.chunks[:3])
    assert first.shape == (3, emb.DIMENSION)
    assert np.allclose(first[0], fake_vector(corpus.chunks[0].text))
    with pytest.raises(ValueError, match="not in chunk order"):
        vectors.vectors_for(corpus.chunks[:1])


def test_an_unapproved_wikipedia_corpus_stops_the_build(tmp_path):
    wiki = wikipedia_ready(tmp_path, approve=False)
    assert (
        eb.not_ready("wikipedia", wiki.parent) == "the Wikipedia corpus is not approved"
    )
    with pytest.raises(ValueError, match="not approved"):
        eb.main(["build", "--corpus", "wikipedia", *args_for(wiki.parent, "--apply")])


def test_not_ready_names_the_missing_approval(tmp_path):
    pending = tmp_path / "pending"
    make_units(pending, SPECS, approve=False)
    assert eb.not_ready("curated", pending) == "units wait for approval"
    assert eb.not_ready("wikipedia", pending) == "there is no Wikipedia manifest"
    approved = tmp_path / "approved"
    make_units(approved, SPECS)
    assert eb.not_ready("curated", approved) is None


def test_model_hashes_check_the_archive_and_hash_the_files(tmp_path):
    folder = tmp_path / "model" / "onnx"
    folder.mkdir(parents=True)
    (folder / emb.MODEL_FILE).write_bytes(b"model")
    (folder / emb.TOKENIZER_FILE).write_bytes(b"tokens")
    found = REAL_MODEL_HASHES(folder)
    assert found["archive_sha256"] is None
    assert found["model_onnx_sha256"] == hashlib.sha256(b"model").hexdigest()
    (tmp_path / "model" / emb.ARCHIVE_FILE).write_bytes(b"not the archive")
    with pytest.raises(ValueError, match="differs from the pinned archive"):
        REAL_MODEL_HASHES(folder)


def test_run_all_skips_corpora_that_wait_for_approval(tmp_path, capsys):
    args = argparse.Namespace(corpus_dir=tmp_path, tokenizer=None, apply=False)
    assert ra.step_embeddings(args) == 0
    output = capsys.readouterr().out
    assert "curated: there are no units, so the embedding build is skipped." in output
    assert "wikipedia: there is no Wikipedia manifest" in output


def test_progress_line_shows_share_time_and_rate(monkeypatch):
    monkeypatch.setattr(pg.time, "monotonic", lambda: 160.0)
    line = pg.progress_line("wikipedia", 2000, 8000, 100.0)
    assert line == (
        "  [wikipedia] 2000/8000 chunks (25.0%) | 1.0 min elapsed | "
        "about 3.0 min left | 33 chunks/s"
    )


def test_fill_reports_progress_for_every_batch(curated):
    client = coll.open_client(br.chroma_dir(curated.parent))
    corpus = ch.read_curated(curated, words)
    lines = []
    coll.fill(
        client,
        "curated_t",
        corpus.chunks,
        FakeEmbedder(),
        label="curated",
        progress=lines.append,
    )
    assert len(lines) == 1 and "[curated] 5/5 chunks (100.0%)" in lines[0]


def test_run_all_dry_pass_does_not_prepare_a_ready_corpus(
    tmp_path, monkeypatch, capsys
):
    make_units(tmp_path, SPECS)

    def never(argv):
        raise AssertionError("the build must not run in the dry pass")

    monkeypatch.setattr(ra.eb, "main", never)
    args = argparse.Namespace(corpus_dir=tmp_path, tokenizer=None, apply=False)
    assert ra.step_embeddings(args) == 0
    assert (
        "curated: ready; planned and built after you answer y."
        in capsys.readouterr().out
    )


def test_an_article_without_a_chunk_is_counted_not_an_error(tmp_path):
    wiki = wikipedia_ready(tmp_path)
    write_chunk_file(wiki, leave_out={"wikipedia.3"})
    argv = ["build", "--corpus", "wikipedia", *args_for(wiki.parent, "--apply")]
    assert eb.main(argv) == 0
    record = read_manifest(wiki / wc.MANIFEST_NAME)[br.RECORD_KEY]
    assert record["articles_without_chunks"] == 1
    assert record["chunk_count"] == 38


def test_wikipedia_waits_for_its_chunks_and_verify_reports_a_missing_file(
    tmp_path, capsys
):
    wiki = wikipedia_ready(tmp_path)
    eb.main(["build", "--corpus", "wikipedia", *args_for(wiki.parent, "--apply")])
    (wiki / wc.CHUNKS_NAME).unlink()
    assert eb.not_ready("wikipedia", wiki.parent).startswith(
        "its chunks are not fetched"
    )
    capsys.readouterr()
    assert eb.main(["verify", "--corpus", "wikipedia", *args_for(wiki.parent)]) == 1
    assert "the corpus cannot be read" in capsys.readouterr().out


def test_keyword_file_streams_the_same_bytes_as_one_json_dump(tmp_path):
    chunks = [ch.Chunk("a#0", "Wind power", {}), ch.Chunk("b#0", "Grid", {})]
    path = tmp_path / "k.json"
    version = ki.write_index(path, chunks)
    expected = (
        json.dumps(
            {"chunk_ids": ["a#0", "b#0"], "tokens": [["wind", "power"], ["grid"]]},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )
    assert path.read_text(encoding="utf-8") == expected
    assert version == hashlib.sha256(expected.encode()).hexdigest()


def test_stored_sample_reads_row_group_by_row_group(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    rows = [
        {"chunk_id": f"wikipedia.{n}#0", "vector": fake_vector(str(n)).tolist()}
        for n in range(1, 31)
    ]
    path = tmp_path / "chunks.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=4)
    assert pq.ParquetFile(path).metadata.num_row_groups == 8
    found = ch.stored_sample(
        path, ["wikipedia.2#0", "wikipedia.29#0", "wikipedia.99#0"]
    )
    assert set(found) == {"wikipedia.2#0", "wikipedia.29#0"}
    assert np.allclose(found["wikipedia.29#0"], fake_vector("29"))


def test_sample_positions_are_even_and_cover_a_small_corpus():
    assert br.sample_positions(5) == [0, 1, 2, 3, 4]
    positions = br.sample_positions(209100)
    assert len(positions) == br.SAMPLE_SIZE and positions[0] == 0
    assert positions == sorted(set(positions)) and positions[-1] < 209100


def test_verify_checks_a_sample_and_prints_every_step(tmp_path, capsys):
    wiki = wikipedia_ready(tmp_path)
    corpus_dir = wiki.parent
    eb.main(["build", "--corpus", "wikipedia", *args_for(corpus_dir, "--apply")])
    capsys.readouterr()
    assert eb.main(["verify", "--corpus", "wikipedia", *args_for(corpus_dir)]) == 0
    output = capsys.readouterr().out
    for line in (
        "Verifying corpus wikipedia",
        "[shards]",
        "[chunk file]",
        "[chunk ids]",
        "collection holds 40 chunks",
        "[wikipedia sample] 40/40",
        "[keyword file]",
        "step: graph",
        "wikipedia: OK",
    ):
        assert line in output
    record = read_manifest(wiki / wc.MANIFEST_NAME)[br.RECORD_KEY]
    client = coll.open_client(br.chroma_dir(corpus_dir.parent))
    collection = client.get_collection(record["collection"], embedding_function=None)
    collection.delete(ids=["wikipedia.1#0"])
    assert eb.main(["verify", "--corpus", "wikipedia", *args_for(corpus_dir)]) == 1
    output = capsys.readouterr().out
    assert "holds 39 chunks, not 40" in output
    assert "1 of 40 sampled chunks are missing or differ" in output


def test_verify_finds_a_changed_shard_file(tmp_path, capsys):
    wiki = wikipedia_ready(tmp_path)
    eb.main(["build", "--corpus", "wikipedia", *args_for(wiki.parent, "--apply")])
    shard = min((wiki / wc.SHARD_DIR_NAME).glob("shard_*.jsonl"))
    shard.write_text(shard.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert eb.main(["verify", "--corpus", "wikipedia", *args_for(wiki.parent)]) == 1
    assert f"shard {shard.name} differs from the manifest" in capsys.readouterr().out
