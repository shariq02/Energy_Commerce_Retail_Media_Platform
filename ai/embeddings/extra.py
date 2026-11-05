"""Build and verify the second dense collection of a corpus (another embedder).

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The collection is built from the same approved units and the same chunks as the
primary (MiniLM) build, so that a comparison isolates the embedder. It has its own
name, `<corpus>.<embedder>_<build id>`, its own build folder and its own record;
the manifest key `embedding_extra` holds the current record of each embedder. The
keyword file and the graph stay with the primary build, which the record names.
Only the curated corpus is built here; the Wikipedia part comes after the curated
result is read. Library only; `ai.embeddings.build` calls it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ai.embeddings import build_record as br
from ai.embeddings import collection as coll
from ai.embeddings import embedder as emb
from ai.embeddings import progress
from ai.embeddings.chunks import CURATED_ID, Corpus, read_curated
from ai.knowledge_quality import state as qstate
from scripts.knowledge import _unit_common as uc

EXTRA_KEY = "embedding_extra"


def scoped_id(corpus_id: str, profile: emb.Profile) -> str:
    """The corpus id of the second embedder: its collections and build folders."""
    return f"{corpus_id}.{profile.slug}"


def extra_record(manifest: dict, profile: emb.Profile) -> dict | None:
    return (manifest.get(EXTRA_KEY) or {}).get(profile.slug)


def write_extra_record(path: Path, profile: emb.Profile, record: dict) -> None:
    """Set the record of this embedder in the manifest; the other keys stay."""
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.setdefault(EXTRA_KEY, {})[profile.slug] = record
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    uc.write_text(path, text)


def primary_problem(corpus: Corpus, primary_hashes: dict) -> str | None:
    """Why the primary build is not the base of a second one, else None."""
    record = corpus.manifest.get(br.RECORD_KEY)
    if not record:
        return "the primary (MiniLM) build does not exist: build it first"
    if br.build_id(corpus, primary_hashes) != record["build_id"]:
        return "the chunks differ from the primary build: rebuild the primary first"
    return None


def make_extra_record(
    corpus: Corpus, profile: emb.Profile, hashes: dict, new_id: str, previous: dict
) -> dict:
    scoped = scoped_id(corpus.corpus_id, profile)
    name = coll.collection_name(scoped, new_id)
    old = previous.get("collection")
    good = old if old and old != name else previous.get("last_known_good")
    primary = corpus.manifest[br.RECORD_KEY]
    return {
        "build_id": new_id,
        "built_on": datetime.now(UTC).date().isoformat(),
        "collection": name,
        "last_known_good": good,
        "corpus_id": scoped,
        "base_corpus_id": corpus.corpus_id,
        "mode": corpus.mode,
        "corpus_version": corpus.manifest["corpus_version"],
        "corpus_fingerprint": corpus.manifest["fingerprint"],
        "embedder": {
            "id": profile.id,
            "dimension": profile.dimension,
            "max_tokens": profile.max_tokens,
            "pooling": profile.pooling,
            "query_prefix": profile.query_prefix,
            "document_prefix": profile.document_prefix,
            "source": profile.source,
            "revision": profile.revision,
            "similarity": "cosine",
            **hashes,
        },
        "chunk_count": len(corpus.chunks),
        "chunks_digest": br.chunks_hash(corpus.chunks),
        "vectors": {"made_in": "local"},
        "primary_build_id": primary["build_id"],
        "keyword_index": {"build_id": primary["build_id"]},
        "graph": {"build_id": primary["build_id"]},
    }


def verify_extra(
    corpus: Corpus, profile: emb.Profile, hashes: dict, client, ai_dir: Path, say
) -> list[str]:
    """The differences between the record of this embedder and what is on disk."""
    record = extra_record(corpus.manifest, profile)
    if not record:
        return [f"the manifest has no {profile.slug} build record"]
    problems = []
    say("  step: corpus fingerprint, chunks and model files")
    if record["corpus_fingerprint"] != corpus.manifest["fingerprint"]:
        problems.append("the corpus changed since the build")
    if record["chunks_digest"] != br.chunks_hash(corpus.chunks):
        problems.append("the chunks differ from the build")
    for key, value in hashes.items():
        if record["embedder"].get(key) != value:
            problems.append(f"model file hash differs from the record: {key}")
    primary = corpus.manifest.get(br.RECORD_KEY) or {}
    if record["primary_build_id"] != primary.get("build_id"):
        problems.append("the primary build changed since this build")
    if br.read_build_record(ai_dir, record["corpus_id"], record["build_id"]) != record:
        problems.append("the record in the build folder differs from the manifest")
    name = record["collection"]
    say(f"  step: collection {name}")
    try:
        collection = client.get_collection(name, embedding_function=None)
    except Exception:  # any client error means the collection is unusable
        return [*problems, f"collection {name} is missing"]
    count = collection.count()
    say(f"  collection holds {count} chunks, the record says {record['chunk_count']}")
    if count != record["chunk_count"]:
        problems.append(f"collection {name} holds {count}, not {record['chunk_count']}")
    data = br.input_from_corpus(corpus, ai_dir)
    return problems + br.check_sample(collection, data.sample, corpus.corpus_id, say)


def prepare(name: str, profile: emb.Profile, args, ai_dir: Path) -> dict:
    """Read the curated corpus with the primary tokenizer (the same chunks), hash
    the files of the embedder and decide if a build is needed."""
    if name != CURATED_ID:
        raise ValueError(
            f"{name}: only the curated corpus has a second collection for now; the "
            "Wikipedia part follows after the curated result is read (it is built "
            "in Databricks)"
        )
    reason = _not_ready(name, args)
    if reason:
        raise ValueError(f"{name}: {reason}")
    progress.show(f"Preparing corpus {name} for {profile.id}")
    corpus = read_curated(args.corpus_dir, emb.token_counter(args.model_dir))
    primary_hashes = emb.model_hashes(args.model_dir)
    problem = primary_problem(corpus, primary_hashes)
    if problem:
        raise ValueError(f"{name}: {problem}")
    progress.show(f"  step: hash the {profile.id} model files")
    hashes = emb.profile_hashes(profile, args.embedder_dir)
    new_id = br.make_build_id(
        corpus.manifest["fingerprint"], hashes, br.chunks_hash(corpus.chunks), None
    )
    previous = extra_record(corpus.manifest, profile) or {}
    unchanged = False
    chroma = br.chroma_dir(ai_dir)
    if chroma.exists() and previous.get("build_id") == new_id:
        progress.show("  step: the build id is unchanged; verify the stored build")
        unchanged = not verify_extra(
            corpus, profile, hashes, coll.open_client(chroma), ai_dir, progress.show
        )
    return {
        "corpus": corpus,
        "profile": profile,
        "hashes": hashes,
        "new_id": new_id,
        "previous": previous,
        "unchanged": unchanged,
    }


def _not_ready(name: str, args) -> str | None:
    from ai.embeddings import build as eb

    return eb.not_ready(name, args.corpus_dir)


def print_plan(item: dict) -> None:
    corpus, profile = item["corpus"], item["profile"]
    scoped = scoped_id(corpus.corpus_id, profile)
    print(f"Corpus {corpus.corpus_id}, second embedder {profile.id}")
    print(f"  chunks {len(corpus.chunks)} (the chunks of the primary build)")
    print(
        f"  dimension {profile.dimension}, token limit {profile.max_tokens}, "
        f"pooling {profile.pooling}, revision {profile.revision}"
    )
    print(
        f"  build id {item['new_id']}, collection "
        f"{coll.collection_name(scoped, item['new_id'])}"
    )
    print(
        "  "
        + ("unchanged: the build record verifies" if item["unchanged"] else "new build")
    )


def build_extra(item: dict, args, ai_dir: Path) -> None:
    corpus, profile = item["corpus"], item["profile"]
    scoped = scoped_id(corpus.corpus_id, profile)
    client = coll.open_client(br.chroma_dir(ai_dir))
    name = coll.collection_name(scoped, item["new_id"])
    if name in coll.names(client, scoped):
        client.delete_collection(name)
    print(f"Building {name}")
    source = emb.profile_embedder(profile, args.embedder_dir)
    coll.fill(client, name, corpus.chunks, source, label=scoped)
    record = make_extra_record(
        corpus, profile, item["hashes"], item["new_id"], item["previous"]
    )
    write_extra_record(corpus.manifest_path, profile, record)
    br.save_build_record(ai_dir, record)
    keep = {record["collection"], record["last_known_good"]} - {None}
    keep |= {
        n
        for n in qstate.keep_collections(ai_dir, corpus.corpus_id)
        if n.startswith(f"{scoped}_")
    }
    removed = coll.prune(client, scoped, keep)
    prefix = coll.collection_name(scoped, "")
    folders = br.prune_builds(ai_dir, scoped, {n.removeprefix(prefix) for n in keep})
    print(f"Record written: {corpus.manifest_path} ({EXTRA_KEY}.{profile.slug})")
    print(
        f"  current {record['collection']}, last-known-good {record['last_known_good']}"
    )
    if removed:
        print(f"  removed older collections: {removed}")
    if folders:
        print(f"  removed older build folders: {folders}")


def run_build(names, profile: emb.Profile, args, ai_dir: Path) -> int:
    items = [prepare(name, profile, args, ai_dir) for name in names]
    for item in items:
        print_plan(item)
    todo = [item for item in items if not item["unchanged"]]
    if not todo:
        print("Nothing to build.")
        return 0
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    for item in todo:
        build_extra(item, args, ai_dir)
    return 0


def run_verify(names, profile: emb.Profile, args, ai_dir: Path) -> int:
    failed = 0
    client = coll.open_client(br.chroma_dir(ai_dir))
    for name in names:
        progress.show(f"Verifying corpus {name} for {profile.id}")
        try:
            if name != CURATED_ID:
                raise ValueError("only the curated corpus has a second collection")
            corpus = read_curated(args.corpus_dir, emb.token_counter(args.model_dir))
            hashes = emb.profile_hashes(profile, args.embedder_dir)
            problems = verify_extra(
                corpus, profile, hashes, client, ai_dir, progress.show
            )
        except (FileNotFoundError, ValueError) as error:
            problems = [f"the corpus cannot be read: {error}"]
        print(f"{name}: " + ("OK" if not problems else f"{len(problems)} problem(s)"))
        for problem in problems:
            print(f"  FAIL {problem}")
        failed += bool(problems)
    return 1 if failed else 0


def check_for_designation(
    corpus_dir: Path, slug: str, model_dir: Path, say
) -> list[str]:
    """Problems of the recorded second collection of an embedder, for the gate."""
    profile = emb.PROFILES[slug]
    ai_dir = corpus_dir.parent
    try:
        corpus = read_curated(corpus_dir, emb.token_counter(model_dir))
        hashes = emb.profile_hashes(profile)
        client = coll.open_client(br.chroma_dir(ai_dir))
        return verify_extra(corpus, profile, hashes, client, ai_dir, say)
    except (FileNotFoundError, ValueError) as error:
        return [f"{slug}: {error}"]
