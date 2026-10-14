"""Wikipedia article units -- terms, unit hash, validation and shard files.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime

import pytest

from scripts.knowledge import wikipedia_prepare as wp

pytestmark = [pytest.mark.unit, pytest.mark.ai]

SECTIONS = [
    {"index": 0, "section": "Lead", "text": "Wind power makes electricity."},
    {"index": 1, "section": "History", "text": "Early mills were used."},
]


def write_terms(path, version=1, cap=2, terms=None):
    terms = terms or {"energy": ["wind power", "solar"], "weather": ["storm"]}
    lines = [f"version: {version}", f"cap_per_term: {cap}", "terms:"]
    for ecosystem, items in terms.items():
        lines.append(f"  {ecosystem}:")
        lines.extend(f"    - {item}" for item in items)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def row(identifier=1, title="Wind power"):
    return {
        "id": identifier,
        "title": title,
        "revisionId": 900 + identifier,
        "revisionTimestamp": datetime(2015, 9, 1, 12, 0, 0, tzinfo=UTC),
    }


def unit(terms, identifier=1):
    return wp.article_unit(
        row(identifier), [dict(s) for s in SECTIONS], "energy", "wind power", terms, "s"
    )


def test_article_unit_fields_and_validation(tmp_path):
    terms = wp.load_terms(write_terms(tmp_path / "t.yml", version=3))
    built = wp.article_unit(row(7), SECTIONS, "energy", "wind power", terms, "snap")
    assert built["unit_id"] == "wikipedia.7"
    assert built["version"] == 907
    assert built["revision_timestamp"] == "2015-09-01T12:00:00+00:00"
    assert built["rule_version"] == 3 and built["snapshot"] == "snap"
    wp.validate_article(built, "test")
    built["sections"][0]["text"] += " changed"
    with pytest.raises(ValueError, match="content hash"):
        wp.validate_article(built, "test")


def test_validate_article_rejects_bad_id_and_missing_keys(tmp_path):
    terms = wp.load_terms(write_terms(tmp_path / "t.yml"))
    built = unit(terms)
    with pytest.raises(ValueError, match="required keys"):
        wp.validate_article({"unit_id": "wikipedia.1"}, "test")
    built["unit_id"] = "wikipedia.../x"
    with pytest.raises(ValueError, match="bad kind or unit id"):
        wp.validate_article(built, "test")


def test_validate_article_rejects_empty_sections(tmp_path):
    terms = wp.load_terms(write_terms(tmp_path / "t.yml"))
    built = unit(terms)
    built["sections"] = []
    with pytest.raises(ValueError, match="no sections"):
        wp.validate_article(built, "test")


def test_load_terms_errors(tmp_path):
    terms = wp.load_terms(write_terms(tmp_path / "t.yml"))
    assert terms.cap_per_term == 2 and re.fullmatch(r"[a-f0-9]{64}", terms.file_hash)
    with pytest.raises(ValueError, match="repeated"):
        wp.load_terms(write_terms(tmp_path / "d.yml", terms={"a": ["x"], "b": ["X"]}))
    with pytest.raises(ValueError, match="positive integers"):
        wp.load_terms(write_terms(tmp_path / "c.yml", cap=0))


def test_write_shard_is_deterministic_and_readable(tmp_path):
    terms = wp.load_terms(write_terms(tmp_path / "t.yml"))
    units = [unit(terms, 1), unit(terms, 2)]
    first = wp.write_shard(tmp_path / wp.shard_name(1), units)
    second = wp.write_shard(tmp_path / "again.jsonl", units)
    assert first == second
    lines = (tmp_path / wp.shard_name(1)).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["unit_id"] for line in lines] == [
        "wikipedia.1",
        "wikipedia.2",
    ]
    assert wp.shard_name(12) == "shard_00012.jsonl"
