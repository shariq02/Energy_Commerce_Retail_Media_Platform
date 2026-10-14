"""Wikipedia text library (Databricks notebook library) -- cleaning, sections, terms.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The notebook library is loaded the way %run loads it. All tests write only to a
temporary folder.
"""

from __future__ import annotations

import re

import pytest

from scripts.knowledge import wikipedia_prepare as wp
from tests._notebook_loader import load_wikipedia_text

pytestmark = [pytest.mark.unit, pytest.mark.ai]

LIB = load_wikipedia_text()
TEXT = (
    "'''Wind power''' is the use of [[wind|moving air]]<ref>note</ref> to make "
    "electricity.{{Infobox|a=b}} == History == Early mills were used."
    "[[Category:Energy]] == See also == [[File:Mill.jpg|thumb]]"
)


def write_terms(path, version=1, cap=2, terms=None):
    terms = terms or {"energy": ["wind power", "solar"], "weather": ["storm"]}
    lines = [f"version: {version}", f"cap_per_term: {cap}", "terms:"]
    for ecosystem, items in terms.items():
        lines.append(f"  {ecosystem}:")
        lines.extend(f"    - {item}" for item in items)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_clean_wikitext_removes_markup():
    cleaned = LIB["clean_wikitext"](TEXT)
    for marker in ("[[", "{{", "<ref", "'''", "Category", "File:"):
        assert marker not in cleaned
    assert "moving air" in cleaned


def test_split_sections_lead_headings_and_empty_dropped():
    sections = LIB["split_sections"](TEXT)
    assert [s["section"] for s in sections] == ["Lead", "History"]
    assert [s["index"] for s in sections] == [0, 1]
    assert sections[0]["text"].startswith("Wind power is the use of moving air")
    assert sections[1]["text"] == "Early mills were used."


def test_split_sections_without_headings_is_one_lead():
    assert LIB["split_sections"]("Plain text only.") == [
        {"index": 0, "section": "Lead", "text": "Plain text only."}
    ]


def test_redirect_and_template_only_text():
    assert LIB["is_redirect"]("#REDIRECT [[Other]]")
    assert not LIB["is_redirect"]("Not a redirect")
    assert LIB["split_sections"]("{{only a template}}") == []


def test_term_pattern_is_whole_word_and_case_free():
    pattern = re.compile(LIB["term_pattern"]("power plant"))
    assert pattern.search("A Power Plant opened")
    assert not pattern.search("empowerment plants")


def test_load_terms_order_hash_and_errors(tmp_path):
    terms = LIB["load_terms"](write_terms(tmp_path / "t.yml"))
    assert terms.pairs == (
        ("energy", "wind power"),
        ("energy", "solar"),
        ("weather", "storm"),
    )
    assert terms.cap_per_term == 2
    with pytest.raises(ValueError, match="repeated"):
        LIB["load_terms"](
            write_terms(tmp_path / "d.yml", terms={"a": ["x"], "b": ["X"]})
        )
    with pytest.raises(ValueError, match="positive integers"):
        LIB["load_terms"](write_terms(tmp_path / "c.yml", cap=0))


def test_both_term_loaders_agree_and_ignore_line_endings(tmp_path):
    path = write_terms(tmp_path / "t.yml")
    notebook, local = LIB["load_terms"](path), wp.load_terms(path)
    assert (notebook.version, notebook.pairs, notebook.file_hash) == (
        local.version,
        local.pairs,
        local.file_hash,
    )
    windows = tmp_path / "w.yml"
    windows.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    assert wp.load_terms(windows).file_hash == local.file_hash
    assert LIB["load_terms"](windows).file_hash == local.file_hash
