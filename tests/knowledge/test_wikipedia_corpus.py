"""Wikipedia corpus -- shard checks, manifest, corpus-level approval.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder, on a fixture of 20 articles.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_prepare as wp

pytestmark = [pytest.mark.unit, pytest.mark.ai]

TERMS_TEXT = (
    "version: 1\ncap_per_term: 5\nterms:\n  energy:\n    - wind power\n"
    "  weather:\n    - storm\n"
)


def make_units(terms, count=20, offset=0):
    units = []
    for index in range(count):
        number = offset + index + 1
        pair = ("energy", "wind power") if index % 2 == 0 else ("weather", "storm")
        row = {
            "id": number,
            "title": f"Article {number}",
            "revisionId": 1000 + number,
            "revisionTimestamp": datetime(2015, 1, 1, tzinfo=UTC),
        }
        sections = [
            {"index": 0, "section": "Lead", "text": f"Lead text {number}."},
            {"index": 1, "section": "Part", "text": f"Second part {number}."},
        ]
        units.append(wp.article_unit(row, sections, *pair, terms, "snap"))
    return units


def make_corpus(tmp_path, units=None, terms_text=TERMS_TEXT, per_shard=8):
    wiki = tmp_path / "wikipedia"
    (wiki / wc.SHARD_DIR_NAME).mkdir(parents=True)
    (wiki / wc.TERMS_NAME).write_text(terms_text, encoding="utf-8")
    terms = wp.load_terms(wiki / wc.TERMS_NAME)
    units = units if units is not None else make_units(terms)
    for start in range(0, len(units), per_shard):
        number = start // per_shard + 1
        shard = wiki / wc.SHARD_DIR_NAME / wp.shard_name(number)
        wp.write_shard(shard, units[start : start + per_shard])
    return wiki


def manifest(wiki):
    return json.loads((wiki / wc.MANIFEST_NAME).read_text(encoding="utf-8"))


def test_build_writes_manifest_after_apply(tmp_path):
    wiki = make_corpus(tmp_path)
    assert wc.main(["build", "--wiki-dir", str(wiki), "--apply"]) == 0
    data = manifest(wiki)
    assert data["article_count"] == 20 and len(data["shards"]) == 3
    assert data["mode"] == "development" and data["corpus_version"] == 1
    assert data["articles_by_term"] == {"storm": 10, "wind power": 10}
    assert data["approval"]["status"] == "pending"


def test_dry_run_and_no_terminal_write_nothing(tmp_path, capsys):
    wiki = make_corpus(tmp_path)
    assert wc.main(["build", "--wiki-dir", str(wiki), "--dry-run"]) == 0
    assert wc.main(["build", "--wiki-dir", str(wiki)]) == 0
    assert not (wiki / wc.MANIFEST_NAME).exists()
    assert "Nothing written" in capsys.readouterr().out


def test_approve_then_rule_change_makes_it_stale(tmp_path):
    wiki = make_corpus(tmp_path)
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    wc.main(["approve", "--approver", "Owner", "--wiki-dir", str(wiki), "--apply"])
    assert manifest(wiki)["approval"]["status"] == "approved"
    assert manifest(wiki)["approval"]["approver"] == "Owner"
    (wiki / wc.TERMS_NAME).write_text(TERMS_TEXT + "    - gale\n", encoding="utf-8")
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    data = manifest(wiki)
    assert data["approval"]["status"] == "stale"
    assert data["corpus_version"] == 2


def test_new_article_list_makes_approval_stale(tmp_path):
    wiki = make_corpus(tmp_path)
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    wc.main(["approve", "--approver", "Owner", "--wiki-dir", str(wiki), "--apply"])
    terms = wp.load_terms(wiki / wc.TERMS_NAME)
    shard = wiki / wc.SHARD_DIR_NAME / wp.shard_name(9)
    wp.write_shard(shard, make_units(terms, count=1, offset=50))
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    assert manifest(wiki)["approval"]["status"] == "stale"


def test_approve_without_manifest_or_approver_fails(tmp_path):
    wiki = make_corpus(tmp_path)
    with pytest.raises(FileNotFoundError, match="build command first"):
        wc.main(["approve", "--approver", "Owner", "--wiki-dir", str(wiki), "--apply"])
    with pytest.raises(SystemExit):
        wc.main(["approve", "--wiki-dir", str(wiki), "--apply"])


def test_scan_rejects_changed_text_duplicates_and_rule_mismatch(tmp_path):
    wiki = make_corpus(tmp_path)
    shard = wiki / wc.SHARD_DIR_NAME / wp.shard_name(1)
    lines = shard.read_text(encoding="utf-8").splitlines()
    first = json.loads(lines[0])
    first["sections"][0]["text"] += " edited"
    shard.write_text(
        "\n".join([json.dumps(first), *lines[1:]]) + "\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="content hash"):
        wc.build_manifest_data(wiki, {})

    wiki = make_corpus(tmp_path / "dup", units=None)
    terms = wp.load_terms(wiki / wc.TERMS_NAME)
    wp.write_shard(
        wiki / wc.SHARD_DIR_NAME / wp.shard_name(9), make_units(terms, count=1)
    )
    with pytest.raises(ValueError, match="twice"):
        wc.build_manifest_data(wiki, {})

    wiki = make_corpus(tmp_path / "rule")
    (wiki / wc.TERMS_NAME).write_text(
        TERMS_TEXT.replace("version: 1", "version: 2"), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="rule version"):
        wc.build_manifest_data(wiki, {})


def test_no_shards_is_an_error(tmp_path):
    wiki = tmp_path / "wikipedia"
    (wiki / wc.SHARD_DIR_NAME).mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="no shard files"):
        wc.scan_shards(wiki / wc.SHARD_DIR_NAME)


def test_production_retrieval_refuses_a_development_corpus(tmp_path):
    wiki = make_corpus(tmp_path)
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    with pytest.raises(ValueError, match="development corpus"):
        wc.require_production(manifest(wiki))
    wc.require_production({"mode": "production", "corpus_id": "wikipedia"})


def write_edges(wiki, pairs):
    lines = [
        json.dumps({"source_id": a, "target_id": b, "click_count": 12})
        for a, b in pairs
    ]
    (wiki / wc.EDGES_NAME).write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_manifest_records_the_edge_file(tmp_path):
    wiki = make_corpus(tmp_path)
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    assert manifest(wiki)["edges"] is None
    write_edges(wiki, [(1, 2), (2, 3)])
    wc.main(["build", "--wiki-dir", str(wiki), "--apply"])
    data = manifest(wiki)
    assert data["edges"]["edges"] == 2 and data["edges"]["file"] == "edges.jsonl"
    assert data["corpus_version"] == 2


@pytest.mark.parametrize(
    ("pairs", "message"),
    [
        ([(1, 99)], "not a selected article"),
        ([(2, 2)], "self-loop"),
        ([(1, 2), (1, 2)], "twice"),
    ],
)
def test_bad_edges_stop_the_build(tmp_path, pairs, message):
    wiki = make_corpus(tmp_path)
    write_edges(wiki, pairs)
    with pytest.raises(ValueError, match=message):
        wc.build_manifest_data(wiki, {})


def test_edge_file_with_wrong_keys_or_count_is_refused(tmp_path):
    wiki = make_corpus(tmp_path)
    (wiki / wc.EDGES_NAME).write_text(
        json.dumps({"source_id": 1, "target_id": 2, "click_count": 0}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="bad edge"):
        wc.build_manifest_data(wiki, {})
