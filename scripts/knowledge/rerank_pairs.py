"""Reranker pairs: export them, send them to Databricks, fetch the scores.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The cross-encoder `bge-reranker-v2-m3` runs only in a Databricks notebook
(`databricks/knowledge/extension/01_rerank_scores`). `export` lists the pairs (an
approved question and a candidate chunk of the fused top `rerank_candidates`) that
have no stored score and writes them to `pairs.jsonl`; `upload` sends the file to the
volume; `fetch` reads the scores table into the local cache that retrieval reads.
Each command prints the number of questions and pairs first and asks before writing
(y or yes); --apply writes without asking, --dry-run never writes.

Usage (repository root):
    python -m scripts.knowledge.rerank_pairs export [--candidates 20]
    python -m scripts.knowledge.rerank_pairs upload
    python -m scripts.knowledge.rerank_pairs fetch
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ai.embeddings import embedder as emb
from ai.knowledge_quality import questions as qs
from ai.knowledge_quality import stack as qstack
from ai.knowledge_quality import thresholds
from ai.rag import fusion
from ai.rag import rerank as rr
from ai.rag import retrieve as rv
from ai.rag import store as st
from ai.rag.config import RetrievalConfig
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_fetch as wf
from scripts.knowledge import wikipedia_model_upload as mu

PAIRS_NAME = "pairs.jsonl"
SCORES_TABLE = f"{wf.CATALOG}.knowledge.rerank_scores"
REMOTE_DIR = f"/Volumes/{wf.CATALOG}/knowledge/rerank"
VOLUME = f"{wf.CATALOG}.knowledge.rerank"


def pairs_path(ai_dir: Path) -> Path:
    return rr.cache_path(ai_dir).with_name(PAIRS_NAME)


def question_pairs(store, question_set, config, scores: dict) -> tuple[int, list[dict]]:
    """(questions used, the pairs that have no stored score), one row per pair."""
    out, seen = [], set()
    chosen = question_set.approved()
    for question in chosen:
        searched = rv.search_all(store, question.text, {}, config)
        rankings = {
            rv.DENSE: [c for c, _ in searched.dense],
            rv.KEYWORD: [c for c, _ in searched.keyword],
        }
        for slug, (_s, hits) in searched.extra.items():
            rankings[rv.extra_name(slug)] = [c for c, _ in hits]
        fused = fusion.fuse(rankings, config.rrf_constant, config.rerank_candidates)
        texts = rv._fetch(store, [c for c, _, _ in fused])
        for chunk_id, _score, _ranks in fused:
            text = texts[chunk_id][0]
            key = rr.pair_key(config.rerank_model, searched.processed.text, text)
            if key in scores or key in seen:
                continue
            seen.add(key)
            out.append({"key": key, "query": searched.processed.text, "text": text})
    return len(chosen), out


def pending_pairs(args) -> tuple[int, list[dict]]:
    """(approved questions, the pairs without a stored score) for the stack of the
    corpus, with the reranker on."""
    ai_dir = args.corpus_dir.parent
    _, latest = thresholds.latest(ai_dir, "curated")
    stacked = qstack.apply(latest, qstack.load(ai_dir, "curated"))
    config = RetrievalConfig.from_dict({**stacked.as_dict(), "rerank": True})
    record = st.read_record("curated", args.corpus_dir)
    store = st.load(
        "curated", args.corpus_dir, args.model_dir, record["build_id"], config.embedders
    )
    question_set = qs.load(qs.questions_path(ai_dir, "curated"))
    scores = rr.load_scores(rr.cache_path(ai_dir))
    return question_pairs(store, question_set, config, scores)


def cmd_export(args) -> int:
    ai_dir = args.corpus_dir.parent
    count, pairs = pending_pairs(args)
    print(f"{count} approved question(s); {len(pairs)} pair(s) without a stored score")
    if not pairs:
        return 0
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    text = "".join(
        json.dumps(p, sort_keys=True, ensure_ascii=False) + "\n" for p in pairs
    )
    uc.write_text(pairs_path(ai_dir), text)
    print(f"Written: {pairs_path(ai_dir)}")
    return 0


def cmd_upload(args, api=None, make_volume=None) -> int:
    ai_dir = args.corpus_dir.parent
    path = pairs_path(ai_dir)
    if not path.is_file():
        print("No pairs file: run export first.")
        return 1
    if api is None:
        settings = mu.file_settings()
        if settings is None:
            print(
                "Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env, then run this again."
            )
            return 1
        api = mu.FilesApi(settings)
    print(f"Send {path.name} ({path.stat().st_size / 1e6:.1f} MB) to {REMOTE_DIR}")
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    (make_volume or create_volume)()
    api.make_dir(REMOTE_DIR)
    with path.open("rb") as handle:
        api.upload(f"{REMOTE_DIR}/{PAIRS_NAME}", handle)
    print(f"Sent: {PAIRS_NAME}")
    return 0


def create_volume() -> None:
    settings = wf.connection_settings()
    if settings is None:
        raise ValueError(f"Set {', '.join(wf.ENV_KEYS)} in .env to create the volume")
    with wf.open_connection(settings) as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")


def cmd_fetch(args, connect=None) -> int:
    ai_dir = args.corpus_dir.parent
    settings = wf.connection_settings()
    if settings is None and connect is None:
        print(f"Set {', '.join(wf.ENV_KEYS)} in .env, then run this again.")
        return 1
    model = RetrievalConfig().rerank_model
    query = f"SELECT key, score FROM {SCORES_TABLE} WHERE model = '{model}'"
    try:
        with (
            (connect or wf.open_connection)(settings) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(query)
            rows = cursor.fetchall()
    except Exception as error:  # the table does not exist before the first notebook run
        if "TABLE_OR_VIEW_NOT_FOUND" in str(error):
            print("The scores table does not exist yet: the notebook has not run.")
            return 0
        raise
    scores = rr.load_scores(rr.cache_path(ai_dir))
    new = {r[0]: float(r[1]) for r in rows if r[0] not in scores}
    print(f"{len(rows)} score(s) in the table; {len(new)} new for the local cache")
    if not new:
        return 0
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    rr.save_scores(rr.cache_path(ai_dir), {**scores, **new})
    print(f"Cache written: {rr.cache_path(ai_dir)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("export", "upload", "fetch"))
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--model-dir", type=Path, default=emb.DEFAULT_MODEL_DIR)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    return {"export": cmd_export, "upload": cmd_upload, "fetch": cmd_fetch}[
        args.command
    ](args)


if __name__ == "__main__":
    sys.exit(main())
