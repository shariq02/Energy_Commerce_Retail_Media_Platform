"""Prepare the reranker scores of the curated corpus: one command, the next missing step.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The cross-encoder runs only in a Databricks notebook. This command looks at what is
there and does the next step that is missing, asking before each write (y or yes):
read the new scores from Databricks into the local cache; list the pairs that still
have no score; upload the reranker weights when they are not in the volume; send the
pairs. Then it says what to run in Databricks and stops. Run it again after the
notebook. When no pair is missing it says that the reranker is ready. The stack file
decides which retrievers are in the stack; whether the reranker is used in retrieval
is the switch `rerank` of that file.

Usage (repository root):
    python -m scripts.knowledge.run_rerank
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ai.embeddings import embedder as emb
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import rerank_pairs as rp
from scripts.knowledge import wikipedia_fetch as wf
from scripts.knowledge import wikipedia_model_upload as mu
from scripts.models import foundation_model_upload as fmu

WEIGHTS_KEY = "bge_reranker_v2_m3"
NOTEBOOK = "databricks/knowledge/extension/01_rerank_scores"


def weights_to_send(session=None, api=None) -> int | None:
    """Bytes of the reranker files that are not in the volume yet; None when unknown."""
    model = next(m for m in fmu.load_config() if m["key"] == WEIGHTS_KEY)
    if not model.get("revision"):
        return None
    plan = fmu.plan_model(session or fmu.hub_session(), model, api)
    return sum(f["size"] for f in plan["todo"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--corpus-dir", type=Path, default=uc.CORPUS_DIR)
    parser.add_argument("--model-dir", type=Path, default=emb.DEFAULT_MODEL_DIR)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    if wf.connection_settings() is None or mu.file_settings() is None:
        print(
            "Set the Databricks settings in .env (see .env.example), then run this again."
        )
        return 1

    print("== step: scores from Databricks ==")
    code = rp.cmd_fetch(args)
    if code:
        return code
    print("== step: pairs without a score ==")
    count, pairs = rp.pending_pairs(args)
    print(f"{count} approved question(s); {len(pairs)} pair(s) without a stored score")
    if not pairs:
        print("Every pair has a stored score: the reranker is ready.")
        return 0

    print("== step: reranker weights in the volume ==")
    api = mu.FilesApi(mu.file_settings())
    missing = weights_to_send(None, api)
    if missing is None:
        print("The reranker has no pinned revision in foundation_models.yml.")
        return 1
    if missing:
        print(f"{missing / 1e6:.0f} MB of the weights are not in the volume.")
        code = fmu.main(["--model", WEIGHTS_KEY, *write_mode(args)], api=api)
        if code:
            return code
    else:
        print("The weights are in the volume.")

    print("== step: export and send the pairs ==")
    for command in (rp.cmd_export, rp.cmd_upload):
        code = command(args)
        if code:
            return code
    print(f"Now run the notebook {NOTEBOOK} on serverless GPU compute in Databricks,")
    print("then run this command again to read the scores.")
    return 0


def write_mode(args: argparse.Namespace) -> list[str]:
    return ["--apply"] if args.apply else (["--dry-run"] if args.dry_run else [])


if __name__ == "__main__":
    sys.exit(main())
