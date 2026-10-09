"""Wikipedia corpus manifest and corpus-level approval.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Reads the shards copied from Databricks into ai/knowledge_corpus/wikipedia/shards/,
checks every article, writes the corpus manifest and records the approval of the
selection rule and the selected-id list. A change of the rule or the list makes
the approval stale. It prints the plan first and asks before writing (y or yes);
--apply writes without asking, --dry-run never writes.

Usage (repository root):
    python -m scripts.knowledge.wikipedia_corpus build
    python -m scripts.knowledge.wikipedia_corpus approve --approver NAME
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_prepare as wp

CORPUS_ID = "wikipedia"
MODE = "development"
WIKI_DIR = uc.CORPUS_DIR / "wikipedia"
SHARD_DIR_NAME = "shards"
TERMS_NAME = "selection_terms.yml"
MANIFEST_NAME = "manifest.json"


def read_shard(path: Path) -> list[dict]:
    articles = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        unit = json.loads(line)
        wp.validate_article(unit, f"{path.name}:{number}")
        articles.append(unit)
    return articles


def scan_shards(shard_dir: Path) -> tuple[list[dict], list[dict]]:
    """Return every article and one entry per shard (name, SHA-256, count)."""
    articles, entries = [], []
    for path in sorted(shard_dir.glob("shard_*.jsonl")):
        shard = read_shard(path)
        articles.extend(shard)
        entries.append(
            {"name": path.name, "sha256": uc.sha256_file(path), "articles": len(shard)}
        )
    if not articles:
        raise FileNotFoundError(f"no shard files with articles in {shard_dir}")
    ids = [unit["unit_id"] for unit in articles]
    if len(set(ids)) != len(ids):
        raise ValueError("an article id appears twice in the shards")
    snapshots = {unit["snapshot"] for unit in articles}
    if len(snapshots) != 1:
        raise ValueError(
            f"shards come from more than one snapshot: {sorted(snapshots)}"
        )
    return articles, entries


def id_list_hash(articles: list[dict]) -> str:
    lines = sorted(
        f"{unit['unit_id']}\t{unit['version']}\t{unit['content_hash']}"
        for unit in articles
    )
    return uc.sha256_text("\n".join(lines))


def current_selection(wiki_dir: Path, articles: list[dict]) -> dict:
    terms = wp.load_terms(wiki_dir / TERMS_NAME)
    versions = {unit["rule_version"] for unit in articles}
    if versions != {terms.version}:
        raise ValueError(
            f"shards were built with rule version(s) {sorted(versions)}, "
            f"the terms file is version {terms.version}"
        )
    return {
        "rule_version": terms.version,
        "terms_hash": terms.file_hash,
        "cap_per_term": terms.cap_per_term,
        "id_list_hash": id_list_hash(articles),
        "article_count": len(articles),
    }


def approval_status(approval: dict | None, selection: dict) -> str:
    """pending: never approved. stale: approved, but the rule or the list changed."""
    if not approval or approval.get("status") != "approved":
        return "pending"
    same = all(
        approval.get(key) == selection[key]
        for key in ("rule_version", "terms_hash", "id_list_hash")
    )
    return "approved" if same else "stale"


def build_manifest_data(wiki_dir: Path, previous: dict) -> dict:
    articles, shards = scan_shards(wiki_dir / SHARD_DIR_NAME)
    selection = current_selection(wiki_dir, articles)
    fingerprint = uc.sha256_text(
        "\n".join([s["sha256"] for s in shards] + [selection["terms_hash"]])
    )
    version = previous.get("corpus_version", 0)
    if fingerprint != previous.get("fingerprint"):
        version += 1
    record = dict(previous.get("approval") or {})
    record["status"] = approval_status(record, selection)
    return {
        "corpus_id": CORPUS_ID,
        "mode": MODE,
        "snapshot": articles[0]["snapshot"],
        "corpus_version": version,
        "fingerprint": fingerprint,
        "article_count": len(articles),
        "shards": shards,
        "selection": selection,
        "articles_by_term": dict(
            sorted(Counter(unit["selection_term"] for unit in articles).items())
        ),
        "approval": record,
    }


def read_previous(wiki_dir: Path) -> dict:
    path = wiki_dir / MANIFEST_NAME
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def write_manifest(wiki_dir: Path, data: dict) -> None:
    text = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    uc.write_text(wiki_dir / MANIFEST_NAME, text)


def approved_manifest(previous: dict, data: dict, approver: str, today: str) -> dict:
    """The manifest with the approval record of the current rule and list."""
    if not previous:
        raise FileNotFoundError("no manifest yet: run the build command first")
    selection = data["selection"]
    record = {
        "status": "approved",
        "mode": MODE,
        "approver": approver,
        "approved_at": today,
        "rule_version": selection["rule_version"],
        "terms_hash": selection["terms_hash"],
        "id_list_hash": selection["id_list_hash"],
        "article_count": selection["article_count"],
    }
    return {**data, "approval": record}


def require_production(manifest: dict) -> None:
    """A production retrieval refuses a development corpus."""
    if manifest.get("mode") != "production":
        raise ValueError(
            f"corpus {manifest.get('corpus_id')!r} is a {manifest.get('mode')} corpus"
        )


def print_summary(data: dict, old_status: str | None) -> None:
    print(
        f"Wikipedia corpus {data['corpus_id']} ({data['mode']}), snapshot "
        f"{data['snapshot']}, corpus_version {data['corpus_version']}"
    )
    print(f"Articles: {data['article_count']} in {len(data['shards'])} shard(s)")
    for term, count in data["articles_by_term"].items():
        print(f"  {term}: {count}")
    selection = data["selection"]
    print(
        f"Rule version {selection['rule_version']}, "
        f"id list {selection['id_list_hash'][:8]}"
    )
    print(f"Approval: {old_status or 'none'} -> {data['approval']['status']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("build", "approve"))
    parser.add_argument("--approver")
    parser.add_argument("--wiki-dir", type=Path, default=WIKI_DIR)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    if args.command == "approve" and not (args.approver or "").strip():
        parser.error("approve needs --approver")

    previous = read_previous(args.wiki_dir)
    data = build_manifest_data(args.wiki_dir, previous)
    if args.command == "approve":
        today = datetime.now(UTC).date().isoformat()
        data = approved_manifest(previous, data, args.approver.strip(), today)
    print_summary(data, (previous.get("approval") or {}).get("status"))
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    write_manifest(args.wiki_dir, data)
    print(f"Manifest written: {args.wiki_dir / MANIFEST_NAME}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
