"""Wikipedia fetch -- table check, files and the dry run.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

A fake connection stands in for Databricks; nothing is read from the network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_fetch as wf
from scripts.knowledge import wikipedia_prepare as wp

pytestmark = [pytest.mark.unit, pytest.mark.ai]

TERMS_TEXT = (
    "version: 1\ncap_per_term: 5\nterms:\n  energy:\n    - wind power\n"
    "  weather:\n    - storm\n"
)


class Row:
    def __init__(self, data):
        self.data = data

    def asDict(self):
        return dict(self.data)


class FakeCursor:
    def __init__(self, check, articles):
        self.check, self.articles = check, list(articles)
        self.query = ""

    def execute(self, query):
        self.query = query

    def fetchone(self):
        return Row(self.check)

    def fetchmany(self, size):
        chunk, self.articles = self.articles[:size], self.articles[size:]
        return [Row(item) for item in chunk]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def article_rows(count):
    return [
        {
            "article_id": number,
            "title": f"Article {number}",
            "revision_id": 500 + number,
            "revision_timestamp": datetime(2015, 5, 1, tzinfo=UTC),
            "term_index": number % 2,
            "snapshot": "snap",
            "sections_json": json.dumps(
                [{"index": 0, "section": "Lead", "text": f"Lead {number}."}]
            ),
        }
        for number in range(1, count + 1)
    ]


def setup(tmp_path, count=5, hash_override=None, articles=None):
    wiki = tmp_path / "wikipedia"
    wiki.mkdir()
    (wiki / wc.TERMS_NAME).write_text(TERMS_TEXT, encoding="utf-8")
    terms = wp.load_terms(wiki / wc.TERMS_NAME)
    check = {
        "articles": count,
        "hashes": 1,
        "terms_hash": hash_override or terms.file_hash,
    }
    rows = article_rows(count) if articles is None else articles
    cursor = FakeCursor(check, rows)
    return wiki, (lambda: FakeConnection(cursor))


def test_fetch_writes_files_and_the_corpus_accepts_them(tmp_path):
    wiki, connect = setup(tmp_path)
    argv = ["--wiki-dir", str(wiki), "--apply"]
    assert wf.main(argv, connect=connect) == 0
    shard_dir = wiki / wc.SHARD_DIR_NAME
    assert len(list(shard_dir.glob("shard_*.jsonl"))) == 1
    assert wc.main(["build", "--wiki-dir", str(wiki), "--apply"]) == 0
    manifest = json.loads((wiki / wc.MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["article_count"] == 5
    assert manifest["articles_by_term"] == {"storm": 3, "wind power": 2}


def test_articles_are_split_into_files_and_old_files_are_removed(tmp_path, monkeypatch):
    monkeypatch.setattr(wf, "ARTICLES_PER_FILE", 2)
    wiki, connect = setup(tmp_path, count=5)
    shard_dir = wiki / wc.SHARD_DIR_NAME
    shard_dir.mkdir()
    (shard_dir / "shard_00099.jsonl").write_text("old\n", encoding="utf-8")
    wf.main(["--wiki-dir", str(wiki), "--apply"], connect=connect)
    names = sorted(path.name for path in shard_dir.glob("shard_*.jsonl"))
    assert names == [wp.shard_name(1), wp.shard_name(2), wp.shard_name(3)]


def test_table_built_with_other_terms_is_refused(tmp_path):
    wiki, connect = setup(tmp_path, hash_override="0" * 64)
    with pytest.raises(ValueError, match="other selection terms"):
        wf.main(["--wiki-dir", str(wiki), "--apply"], connect=connect)
    assert not (wiki / wc.SHARD_DIR_NAME).exists()


def test_empty_table_is_refused(tmp_path):
    wiki, connect = setup(tmp_path, count=0)
    with pytest.raises(ValueError, match="empty"):
        wf.main(["--wiki-dir", str(wiki), "--apply"], connect=connect)


def test_short_read_is_an_error(tmp_path):
    wiki, connect = setup(tmp_path, count=5, articles=article_rows(3))
    with pytest.raises(ValueError, match="were written"):
        wf.main(["--wiki-dir", str(wiki), "--apply"], connect=connect)


def test_dry_run_and_no_terminal_write_nothing(tmp_path, capsys):
    wiki, connect = setup(tmp_path)
    assert wf.main(["--wiki-dir", str(wiki), "--dry-run"], connect=connect) == 0
    assert wf.main(["--wiki-dir", str(wiki)], connect=connect) == 0
    assert not (wiki / wc.SHARD_DIR_NAME).exists()
    assert "Nothing written" in capsys.readouterr().out


def test_connection_settings_need_all_three_values():
    full = {
        "DATABRICKS_HOST": "https://x.cloud.databricks.com",
        "DATABRICKS_TOKEN_SQL": "t",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
    }
    assert wf.connection_settings(full) == full
    assert wf.connection_settings({**full, "DATABRICKS_TOKEN_SQL": " "}) is None
    assert wf.connection_settings({}) is None


def test_missing_settings_stop_with_a_message(tmp_path, monkeypatch, capsys):
    wiki = tmp_path / "wikipedia"
    wiki.mkdir()
    (wiki / wc.TERMS_NAME).write_text(TERMS_TEXT, encoding="utf-8")
    monkeypatch.setattr(wf, "connection_settings", lambda env=None: None)
    assert wf.main(["--wiki-dir", str(wiki), "--apply"]) == 1
    assert "DATABRICKS_HTTP_PATH" in capsys.readouterr().out


def test_connector_error_prints_its_context_and_no_setting_values(tmp_path, capsys):
    exc = pytest.importorskip("databricks.sql.exc")
    wiki, _ = setup(tmp_path)

    def connect():
        raise exc.RequestError(
            "Error during request to server",
            {
                "http-code": 401,
                "error-message": "Invalid access token",
                "method": "OpenSession",
            },
        )

    assert wf.main(["--wiki-dir", str(wiki), "--apply"], connect=connect) == 1
    output = capsys.readouterr().out
    assert "http-code: 401" in output and "Invalid access token" in output
    assert "DATABRICKS_HTTP_PATH" in output
