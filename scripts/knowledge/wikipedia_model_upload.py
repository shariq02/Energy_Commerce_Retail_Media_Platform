"""Upload the embedding model files to the Databricks volume for notebook 05.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Copies model.onnx and tokenizer.json from the local model folder, with a
model_hashes.json of their SHA-256, to
/Volumes/energy_commerce_retail_media/knowledge/models/all-MiniLM-L6-v2/ through
the Databricks Files API. Notebook 05 embeds only with files that match those
hashes, so the Wikipedia vectors come from the same files as the local query
vectors. A file is sent only when it is missing there or its size differs;
--force sends all. The volume is created through the SQL warehouse when it is
missing. Run once, and again only when the model changes or the volume is cleared.
It prints the plan and asks before writing (y or yes); --apply writes without
asking, --dry-run never writes. Settings come from .env: DATABRICKS_HOST and
DATABRICKS_TOKEN for the files; DATABRICKS_TOKEN_SQL and DATABRICKS_HTTP_PATH for
the volume.

Usage (repository root):
    python -m scripts.knowledge.wikipedia_model_upload [--force]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from ai.embeddings import embedder as emb
from scripts.knowledge import _unit_common as uc
from scripts.knowledge import wikipedia_fetch as wf

VOLUME = f"{wf.CATALOG}.knowledge.models"
REMOTE_DIR = f"/Volumes/{wf.CATALOG}/knowledge/models/{emb.MODEL_ID}"
HASHES_NAME = "model_hashes.json"
PINNED_FILES = ("model_onnx_sha256", "tokenizer_sha256")
TIMEOUT = (30, 900)


def file_settings(env=None) -> dict | None:
    """Host and token for the Files API from the environment (.env is loaded first)."""
    if env is None:
        wf.connection_settings()  # loads .env into the environment
        env = os.environ
    host = (env.get("DATABRICKS_HOST") or "").strip()
    token = (env.get("DATABRICKS_TOKEN") or "").strip()
    if not host or not token:
        return None
    return {"host": host.removeprefix("https://").rstrip("/"), "token": token}


def hashes_text(model_dir: Path) -> str:
    hashes = emb.model_hashes(model_dir)
    pinned = {key: hashes[key] for key in PINNED_FILES}
    return json.dumps(pinned, indent=2, sort_keys=True) + "\n"


class FilesApi:
    def __init__(self, settings: dict, session=None):
        import requests

        self.base = f"https://{settings['host']}/api/2.0/fs"
        self.session = session or requests.Session()
        self.session.headers["Authorization"] = f"Bearer {settings['token']}"

    def size(self, path: str) -> int | None:
        """The size of a remote file, or None when it is not there."""
        response = self.session.head(f"{self.base}/files{path}", timeout=TIMEOUT)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return int(response.headers.get("content-length", -1))

    def read_text(self, path: str) -> str | None:
        response = self.session.get(f"{self.base}/files{path}", timeout=TIMEOUT)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.text

    def make_dir(self, path: str) -> None:
        response = self.session.put(f"{self.base}/directories{path}", timeout=TIMEOUT)
        response.raise_for_status()

    def upload(self, path: str, data) -> None:
        response = self.session.put(
            f"{self.base}/files{path}",
            params={"overwrite": "true"},
            data=data,
            timeout=TIMEOUT,
        )
        response.raise_for_status()


def create_volume() -> None:
    settings = wf.connection_settings()
    if settings is None:
        raise ValueError(f"Set {', '.join(wf.ENV_KEYS)} in .env to create the volume")
    with wf.open_connection(settings) as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")


def plan(api: FilesApi, model_dir: Path, text: str, force: bool) -> list[str]:
    """The names of the files to send."""
    todo = []
    for name in (emb.MODEL_FILE, emb.TOKENIZER_FILE):
        local = (model_dir / name).stat().st_size
        remote = api.size(f"{REMOTE_DIR}/{name}")
        if force or remote != local:
            todo.append(name)
    if force or todo or api.read_text(f"{REMOTE_DIR}/{HASHES_NAME}") != text:
        todo.append(HASHES_NAME)
    return todo


def main(
    argv: list[str] | None = None, api: FilesApi | None = None, make_volume=None
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model-dir", type=Path, default=emb.DEFAULT_MODEL_DIR)
    parser.add_argument("--force", action="store_true", help="send every file")
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)

    if api is None:
        settings = file_settings()
        if settings is None:
            print(
                "Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env, then run this again."
            )
            return 1
        api = FilesApi(settings)
    text = hashes_text(args.model_dir)
    todo = plan(api, args.model_dir, text, args.force)
    print(f"Target: {REMOTE_DIR}")
    if not todo:
        print("The model files are already there and match. Nothing to send.")
        return 0
    print(f"To send: {', '.join(todo)}")
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    (make_volume or create_volume)()
    api.make_dir(REMOTE_DIR)
    for name in todo:
        if name == HASHES_NAME:
            api.upload(f"{REMOTE_DIR}/{name}", text.encode("utf-8"))
        else:
            with (args.model_dir / name).open("rb") as handle:
                api.upload(f"{REMOTE_DIR}/{name}", handle)
        print(f"Sent: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
