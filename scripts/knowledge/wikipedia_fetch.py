"""Fetch the selected Wikipedia articles from Databricks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Reads knowledge.wikipedia_selected through the Databricks SQL connector and
writes the article units as JSON Lines files in ai/knowledge_corpus/wikipedia/shards/.
It checks first that the table was built with the local selection terms, prints the
plan and asks before writing (y or yes); --apply writes without asking, --dry-run
never writes. Settings come from .env: DATABRICKS_HOST, DATABRICKS_TOKEN_SQL and
DATABRICKS_HTTP_PATH (the HTTP path of a SQL warehouse).

Usage (repository root):
    python -m scripts.knowledge.wikipedia_fetch
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_corpus as wc
from scripts.knowledge import wikipedia_prepare as wp

try:
    from databricks.sql.exc import Error as DatabricksError
except ImportError:  # the connector is only needed for a real fetch
    DatabricksError = ()  # catches nothing

CATALOG = "energy_commerce_retail_media"
SELECTED_TABLE = f"{CATALOG}.knowledge.wikipedia_selected"
ARTICLES_PER_FILE = 1000
FETCH_SIZE = 1000
ENV_KEYS = ("DATABRICKS_HOST", "DATABRICKS_TOKEN_SQL", "DATABRICKS_HTTP_PATH")
CHECK_QUERY = (
    "SELECT COUNT(*) AS articles, COUNT(DISTINCT terms_hash) AS hashes, "
    f"MIN(terms_hash) AS terms_hash FROM {SELECTED_TABLE}"
)
ARTICLE_QUERY = (
    "SELECT article_id, title, revision_id, revision_timestamp, term_index, "
    "snapshot, to_json(sections) AS sections_json "
    f"FROM {SELECTED_TABLE} ORDER BY article_id"
)


def connection_settings(env=None) -> dict | None:
    """The connector settings from the environment (.env is loaded first)."""
    if env is None:
        try:
            from dotenv import load_dotenv

            load_dotenv(uc.ROOT / ".env")
        except ImportError:
            pass
        env = os.environ
    values = {key: (env.get(key) or "").strip() for key in ENV_KEYS}
    return values if all(values.values()) else None


def open_connection(settings: dict):
    from databricks import sql

    host = settings["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
    return sql.connect(
        server_hostname=host,
        http_path=settings["DATABRICKS_HTTP_PATH"],
        access_token=settings["DATABRICKS_TOKEN_SQL"],
    )


CONTEXT_KEYS = (
    "method",
    "http-code",
    "error-message",
    "original-exception",
    "no-retry-reason",
    "attempt",
    "elapsed-seconds",
)


def describe_error(error: Exception) -> list[str]:
    """The message and the context of a connector error; no setting values."""
    context = getattr(error, "context", None) or {}
    lines = [f"Databricks request failed: {error}"]
    lines += [f"  {key}: {context[key]}" for key in CONTEXT_KEYS if key in context]
    lines.append(
        "Check DATABRICKS_HOST (server hostname only), DATABRICKS_HTTP_PATH, "
        "DATABRICKS_TOKEN_SQL and that the SQL warehouse is running."
    )
    return lines


def check_table(cursor, terms: wp.Terms) -> int:
    """The table must exist, hold articles and come from the local terms file."""
    cursor.execute(CHECK_QUERY)
    row = cursor.fetchone().asDict()
    if not row["articles"]:
        raise ValueError(f"{SELECTED_TABLE} is empty: run the Gold notebook first")
    if row["hashes"] != 1 or row["terms_hash"] != terms.file_hash:
        raise ValueError(
            f"{SELECTED_TABLE} was built with other selection terms than the local "
            "file: run 02_wikipedia_gold again"
        )
    return int(row["articles"])


def row_to_unit(row: dict, terms: wp.Terms) -> dict:
    ecosystem, term = terms.pairs[row["term_index"]]
    meta = {
        "id": row["article_id"],
        "title": row["title"],
        "revisionId": row["revision_id"],
        "revisionTimestamp": row["revision_timestamp"],
    }
    sections = json.loads(row["sections_json"])
    return wp.article_unit(meta, sections, ecosystem, term, terms, row["snapshot"])


def clear_files(shard_dir: Path) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    for path in shard_dir.glob("shard_*.jsonl"):
        path.unlink()


def write_articles(cursor, terms: wp.Terms, shard_dir: Path) -> list[dict]:
    """Stream the article rows into numbered files; returns one entry per file."""
    clear_files(shard_dir)
    entries, buffer = [], []

    def flush() -> None:
        name = wp.shard_name(len(entries) + 1)
        digest = wp.write_shard(shard_dir / name, buffer)
        entries.append({"name": name, "sha256": digest, "articles": len(buffer)})
        buffer.clear()

    cursor.execute(ARTICLE_QUERY)
    while rows := cursor.fetchmany(FETCH_SIZE):
        for row in rows:
            buffer.append(row_to_unit(row.asDict(), terms))
            if len(buffer) == ARTICLES_PER_FILE:
                flush()
    if buffer:
        flush()
    return entries


def main(argv: list[str] | None = None, connect=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--wiki-dir", type=Path, default=wc.WIKI_DIR)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)

    terms = wp.load_terms(args.wiki_dir / wc.TERMS_NAME)
    if connect is None:
        settings = connection_settings()
        if settings is None:
            print(f"Set {', '.join(ENV_KEYS)} in .env, then run this again.")
            return 1

        def connect():
            return open_connection(settings)

    shard_dir = args.wiki_dir / wc.SHARD_DIR_NAME
    try:
        with connect() as connection, connection.cursor() as cursor:
            count = check_table(cursor, terms)
            print(f"{SELECTED_TABLE}: {count} articles, rule version {terms.version}")
            print(f"Target: {shard_dir} (the existing files there are replaced)")
            if not args.apply and (args.dry_run or not uc.confirm_apply()):
                print(uc.NOT_WRITTEN)
                return 0
            entries = write_articles(cursor, terms, shard_dir)
    except DatabricksError as error:
        print("\n".join(describe_error(error)))
        return 1
    written = sum(entry["articles"] for entry in entries)
    if written != count:
        raise ValueError(f"table has {count} articles, {written} were written")
    print(f"Written: {written} articles in {len(entries)} file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
