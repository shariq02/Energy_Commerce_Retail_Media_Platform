"""Fetch the selected Wikipedia articles and their edges from Databricks.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Reads knowledge.wikipedia_selected and knowledge.wikipedia_edges through the
Databricks SQL connector. The articles go to JSON Lines files in
ai/knowledge_corpus/wikipedia/shards/, the edges to edges.jsonl beside them.
--part chooses articles, edges or both (default both); each part replaces its own
files only. It checks first that the tables were built with the local selection
terms, prints the plan and asks before writing (y or yes); --apply writes without
asking, --dry-run never writes. Settings come from .env: DATABRICKS_HOST,
DATABRICKS_TOKEN_SQL and DATABRICKS_HTTP_PATH (the HTTP path of a SQL warehouse).

Usage (repository root):
    python -m scripts.knowledge.wikipedia_fetch [--part articles] [--part edges]
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
EDGES_TABLE = f"{CATALOG}.knowledge.wikipedia_edges"
PARTS = ("articles", "edges")
ARTICLES_PER_FILE = 1000
FETCH_SIZE = 1000
ENV_KEYS = ("DATABRICKS_HOST", "DATABRICKS_TOKEN_SQL", "DATABRICKS_HTTP_PATH")
CHECK_QUERY = (
    "SELECT COUNT(*) AS n, COUNT(DISTINCT terms_hash) AS hashes, "
    f"MIN(terms_hash) AS terms_hash FROM {SELECTED_TABLE}"
)
EDGE_CHECK_QUERY = (
    "SELECT COUNT(*) AS n, COUNT(DISTINCT selection_terms_hash) AS hashes, "
    f"MIN(selection_terms_hash) AS terms_hash FROM {EDGES_TABLE}"
)
ARTICLE_QUERY = (
    "SELECT article_id, title, revision_id, revision_timestamp, term_index, "
    "snapshot, to_json(sections) AS sections_json "
    f"FROM {SELECTED_TABLE} ORDER BY article_id"
)
EDGE_QUERY = (
    "SELECT prev_article_id, curr_article_id, click_count "
    f"FROM {EDGES_TABLE} ORDER BY prev_article_id, curr_article_id"
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


def check_table(cursor, terms: wp.Terms, query: str, table: str) -> int:
    """The table must exist, hold rows and come from the local terms file."""
    cursor.execute(query)
    row = cursor.fetchone().asDict()
    if not row["n"]:
        raise ValueError(f"{table} is empty: run its notebook first")
    if row["hashes"] != 1 or row["terms_hash"] != terms.file_hash:
        raise ValueError(
            f"{table} was built with other selection terms than the local file: "
            "run the Gold notebooks again"
        )
    return int(row["n"])


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


def write_edges(cursor, path: Path) -> int:
    """Stream the edge rows into one JSON Lines file; returns the edge count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    temp = path.with_suffix(".tmp")
    cursor.execute(EDGE_QUERY)
    with temp.open("w", encoding="utf-8", newline="\n") as handle:
        while rows := cursor.fetchmany(FETCH_SIZE):
            for row in rows:
                data = row.asDict()
                edge = {
                    "source_id": int(data["prev_article_id"]),
                    "target_id": int(data["curr_article_id"]),
                    "click_count": int(data["click_count"]),
                }
                handle.write(json.dumps(edge, sort_keys=True) + "\n")
                count += 1
    temp.replace(path)
    return count


def main(argv: list[str] | None = None, connect=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--wiki-dir", type=Path, default=wc.WIKI_DIR)
    parser.add_argument("--part", action="append", choices=PARTS, default=None)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    parts = args.part or list(PARTS)

    terms = wp.load_terms(args.wiki_dir / wc.TERMS_NAME)
    if connect is None:
        settings = connection_settings()
        if settings is None:
            print(f"Set {', '.join(ENV_KEYS)} in .env, then run this again.")
            return 1

        def connect():
            return open_connection(settings)

    shard_dir = args.wiki_dir / wc.SHARD_DIR_NAME
    edges_path = args.wiki_dir / wc.EDGES_NAME
    counts, written = {}, {}
    try:
        with connect() as connection, connection.cursor() as cursor:
            if "articles" in parts:
                counts["articles"] = check_table(
                    cursor, terms, CHECK_QUERY, SELECTED_TABLE
                )
                print(f"{SELECTED_TABLE}: {counts['articles']} articles")
                print(f"Target: {shard_dir} (the existing article files are replaced)")
            if "edges" in parts:
                counts["edges"] = check_table(
                    cursor, terms, EDGE_CHECK_QUERY, EDGES_TABLE
                )
                print(f"{EDGES_TABLE}: {counts['edges']} edges")
                print(f"Target: {edges_path} (replaced)")
            print(f"Rule version {terms.version}")
            if not args.apply and (args.dry_run or not uc.confirm_apply()):
                print(uc.NOT_WRITTEN)
                return 0
            if "articles" in parts:
                entries = write_articles(cursor, terms, shard_dir)
                written["articles"] = sum(entry["articles"] for entry in entries)
                print(
                    f"Written: {written['articles']} articles in {len(entries)} file(s)"
                )
            if "edges" in parts:
                written["edges"] = write_edges(cursor, edges_path)
                print(f"Written: {written['edges']} edges")
    except DatabricksError as error:
        print("\n".join(describe_error(error)))
        return 1
    for part, count in counts.items():
        if written[part] != count:
            raise ValueError(f"{part}: table has {count}, {written[part]} were written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
