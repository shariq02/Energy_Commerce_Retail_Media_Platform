"""Download the foundation model files and upload them to the Databricks volume.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Reads foundation_models.yml, downloads each model at a pinned revision from the
Hugging Face Hub, or from GitHub when `source: github` (files that match `include`),
hashes every file, sends the files to
/Volumes/<catalog>/energy_ml_models/foundation_models/<key>/<revision>/ through the
Databricks Files API and writes foundation_manifest.json there. A model whose
revision is not pinned is skipped; --pin writes the latest revision of its `ref`
into foundation_models.yml first (after y or yes). A licence written as `hub` is read
from the Hub model card and the command stops when the card names none. A file is sent only when it is
missing there or its size differs. It prints the plan and asks before writing (y or
yes); --apply writes without asking, --dry-run never writes. Settings come from .env:
DATABRICKS_HOST and DATABRICKS_TOKEN for the files; DATABRICKS_TOKEN_SQL and
DATABRICKS_HTTP_PATH for the volume; HF_TOKEN only for a gated repository.

Usage (repository root):
    python -m scripts.models.foundation_model_upload [--model KEY] [--pin]
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import sys
from pathlib import Path

import yaml

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_fetch as wf
from scripts.knowledge.wikipedia_model_upload import FilesApi, file_settings

CONFIG = Path(__file__).with_name("foundation_models.yml")
HUB = "https://huggingface.co"
GITHUB_API = "https://api.github.com"
GITHUB_RAW = "https://raw.githubusercontent.com"
VOLUME = f"{wf.CATALOG}.energy_ml_models.foundation_models"
ROOT = f"/Volumes/{wf.CATALOG}/energy_ml_models/foundation_models"
MANIFEST_NAME = "foundation_manifest.json"
CACHE = Path.home() / ".cache" / "ecrmap" / "foundation"
TIMEOUT = (30, 900)
BLOCK = 8 * 1024 * 1024


def load_config(path: Path = CONFIG) -> list[dict]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    models = data.get("models") or []
    keys = [m["key"] for m in models]
    if len(keys) != len(set(keys)):
        raise ValueError("a model key appears twice")
    for m in models:
        missing = [
            f for f in ("key", "repo_id", "ref", "licence", "include") if f not in m
        ]
        if missing:
            raise ValueError(f"{m.get('key')}: missing {missing}")
    return models


def save_config(models: list[dict], path: Path = CONFIG) -> None:
    text = yaml.safe_dump({"models": models}, sort_keys=False, allow_unicode=True)
    uc.write_text(path, text)


def select_files(tree: list[dict], include: list[str]) -> list[dict]:
    """The files of a repository listing that match an include pattern."""
    chosen = [
        {"path": item["path"], "size": int(item.get("size", 0))}
        for item in tree
        if item.get("type") == "file"
        and any(fnmatch.fnmatch(item["path"], p) for p in include)
    ]
    return sorted(chosen, key=lambda f: f["path"])


def remote_dir(key: str, revision: str) -> str:
    return f"{ROOT}/{key}/{revision}"


def merge_manifest(old: dict | None, entry: dict) -> dict:
    """The manifest with `entry` set for its model key; other models stay."""
    models = dict((old or {}).get("models", {}))
    models[entry["model_key"]] = entry
    return {"models": models}


def manifest_entry(model: dict, revision: str, hashes: dict) -> dict:
    return {
        "model_key": model["key"],
        "repo_id": model["repo_id"],
        "revision": revision,
        "licence": model["licence"],
        "licence_restriction": model.get("licence_restriction", "none"),
        "pretraining_data": model.get("pretraining_data", ""),
        "files": dict(sorted(hashes.items())),
    }


def hub_session(env=None):
    import requests

    env = os.environ if env is None else env
    session = requests.Session()
    token = (env.get("HF_TOKEN") or "").strip()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    return session


_PLAIN: dict = {}


def plain_session():
    """A session without the Hub token: the token must never reach another host."""
    import requests

    return _PLAIN.setdefault("session", requests.Session())


def source_session(session, source: str):
    return plain_session() if source == "github" else session


def latest_revision(session, repo: str, ref: str, source: str = "hub") -> str:
    if source == "github":
        url = f"{GITHUB_API}/repos/{repo}/commits/{ref}"
    else:
        url = f"{HUB}/api/models/{repo}/revision/{ref}"
    response = source_session(session, source).get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()["sha"]


def list_tree(session, repo: str, revision: str, source: str = "hub") -> list[dict]:
    if source == "github":
        response = plain_session().get(
            f"{GITHUB_API}/repos/{repo}/git/trees/{revision}",
            params={"recursive": "1"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        return [
            {"type": "file", "path": item["path"], "size": item.get("size", 0)}
            for item in response.json()["tree"]
            if item.get("type") == "blob"
        ]
    response = session.get(
        f"{HUB}/api/models/{repo}/tree/{revision}",
        params={"recursive": "true"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def hub_licence(session, repo: str) -> str:
    """The licence on the Hub model card; empty when the card names none."""
    response = session.get(f"{HUB}/api/models/{repo}", timeout=TIMEOUT)
    response.raise_for_status()
    body = response.json()
    found = (body.get("cardData") or {}).get("license")
    if not found:
        found = next(
            (
                t.split(":", 1)[1]
                for t in body.get("tags", [])
                if t.startswith("license:")
            ),
            "",
        )
    return str(found or "")


def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def download(
    session, repo: str, revision: str, file: dict, folder: Path, source: str = "hub"
) -> Path:
    """The file in the local cache; downloaded only when it is missing or its size
    differs."""
    target = folder / file["path"]
    if target.is_file() and target.stat().st_size == file["size"]:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    if source == "github":
        url = f"{GITHUB_RAW}/{repo}/{revision}/{file['path']}"
    else:
        url = f"{HUB}/{repo}/resolve/{revision}/{file['path']}"
    with source_session(session, source).get(
        url, stream=True, timeout=TIMEOUT
    ) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for block in response.iter_content(BLOCK):
                handle.write(block)
    return target


def create_volume() -> None:
    settings = wf.connection_settings()
    if settings is None:
        raise ValueError(f"Set {', '.join(wf.ENV_KEYS)} in .env to create the volume")
    with wf.open_connection(settings) as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")


def pin_revisions(session, models: list[dict]) -> list[dict]:
    """Models whose revision is not pinned, with the latest revision of their ref."""
    found = []
    for m in models:
        if m.get("revision"):
            continue
        try:
            latest = latest_revision(
                session, m["repo_id"], m["ref"], m.get("source", "hub")
            )
        except Exception as exc:
            raise ValueError(
                f"{m['key']}: cannot read {m['repo_id']}@{m['ref']}: {exc}"
            ) from exc
        found.append({**m, "latest": latest})
    return found


def plan_model(session, model: dict, api: FilesApi | None) -> dict:
    revision = model["revision"]
    source = model.get("source", "hub")
    if model["licence"] == "hub":
        licence = hub_licence(session, model["repo_id"])
        if not licence:
            raise ValueError(f"{model['key']}: the Hub card names no licence")
        model = {**model, "licence": licence}
    files = select_files(
        list_tree(session, model["repo_id"], revision, source), model["include"]
    )
    if not files:
        raise ValueError(f"{model['key']}: no file matches {model['include']}")
    if source == "hub" and all(f["path"].endswith(".json") for f in files):
        raise ValueError(f"{model['key']}: only json files match, no weight file")
    todo = files
    if api is not None:
        folder = remote_dir(model["key"], revision)
        todo = [f for f in files if api.size(f"{folder}/{f['path']}") != f["size"]]
    return {"model": model, "files": files, "todo": todo}


def send(session, api: FilesApi, plan: dict, old_manifest: dict | None) -> dict:
    model = plan["model"]
    revision = model["revision"]
    cache = CACHE / model["key"] / revision
    folder = remote_dir(model["key"], revision)
    hashes = {}
    todo = {f["path"] for f in plan["todo"]}
    for file in plan["files"]:
        local = download(
            session,
            model["repo_id"],
            revision,
            file,
            cache,
            model.get("source", "hub"),
        )
        hashes[file["path"]] = sha256_file(local)
        if file["path"] in todo:
            api.make_dir(f"{folder}/{os.path.dirname(file['path'])}".rstrip("/"))
            with local.open("rb") as handle:
                api.upload(f"{folder}/{file['path']}", handle)
            print(f"Sent: {model['key']}/{file['path']}")
    return merge_manifest(old_manifest, manifest_entry(model, revision, hashes))


def main(
    argv: list[str] | None = None, session=None, api=None, make_volume=None
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model", action="append", help="only this model key")
    parser.add_argument("--pin", action="store_true", help="pin the latest revision")
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)

    models = load_config()
    if args.model:
        models = [m for m in models if m["key"] in args.model]
    session = session or hub_session()
    try:
        unpinned = pin_revisions(session, models)
    except ValueError as exc:
        print(exc)
        return 1
    for m in unpinned:
        print(
            f"Not pinned: {m['key']} ({m['repo_id']}@{m['ref']}) latest {m['latest']}"
        )
    if unpinned and args.pin:
        if not args.apply and (args.dry_run or not uc.confirm_apply()):
            print(uc.NOT_WRITTEN)
            return 0
        latest = {m["key"]: m["latest"] for m in unpinned}
        everything = load_config()
        for m in everything:
            if m["key"] in latest:
                m["revision"] = latest[m["key"]]
        save_config(everything)
        print(f"Pinned in {CONFIG.name}: {', '.join(sorted(latest))}")
        models = [
            {**m, "revision": latest.get(m["key"], m.get("revision"))} for m in models
        ]
    ready = [m for m in models if m.get("revision")]
    if not ready:
        print("No pinned model to send. Use --pin to pin the latest revisions.")
        return 1
    if api is None:
        settings = file_settings()
        if settings is None:
            print(
                "Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env, then run this again."
            )
            return 1
        api = FilesApi(settings)
    plans = [plan_model(session, m, api) for m in ready]
    for plan in plans:
        size = sum(f["size"] for f in plan["todo"]) / 1e6
        print(
            f"{plan['model']['key']} @ {plan['model']['revision'][:12]}: "
            f"{len(plan['files'])} file(s), {len(plan['todo'])} to send ({size:.0f} MB)"
        )
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    (make_volume or create_volume)()
    api.make_dir(ROOT)
    text = api.read_text(f"{ROOT}/{MANIFEST_NAME}")
    manifest = json.loads(text) if text else None
    for plan in plans:
        manifest = send(session, api, plan, manifest)
    api.upload(
        f"{ROOT}/{MANIFEST_NAME}",
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    print(f"Sent: {MANIFEST_NAME}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
