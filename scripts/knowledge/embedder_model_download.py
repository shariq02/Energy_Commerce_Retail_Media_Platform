"""Download the files of an embedder model from the Hugging Face Hub.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Fetches the model file and the tokenizer file of an embedder profile at its pinned
commit into the model folder (default `~/.cache/ecrmap/models/<embedder id>/`). A
file is checked against its pinned size and SHA-256 and is kept only when both
match; a file that is already there and matches is not fetched again. The download
goes to a `.part` file first. It prints the plan with the sizes and the time limits
and asks before writing (y or yes); --apply writes without asking, --dry-run never
writes. Plain `requests`; no new package.

Usage (repository root):
    python -m scripts.knowledge.embedder_model_download [--embedder gte-modernbert-base]
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

from ai.embeddings import embedder as emb
from ai.embeddings.progress import progress_line, show
from scripts.knowledge import _unit_common as uc

HUB = "https://huggingface.co"
TIMEOUT = (30, 60)  # connect, and wait for the next block
BLOCK = 4 * 1024 * 1024


def url_of(profile: emb.Profile, remote: str) -> str:
    return f"{HUB}/{profile.source}/resolve/{profile.revision}/{remote}"


def state_of(path: Path, size: int, sha256: str) -> str:
    """`ok` (size and hash match), `wrong` or `missing`."""
    if not path.is_file():
        return "missing"
    if path.stat().st_size != size or emb.file_sha256(path) != sha256:
        return "wrong"
    return "ok"


def plan(profile: emb.Profile, folder: Path) -> list[tuple[str, str, int, str, str]]:
    """(name, remote path, size, SHA-256, state) of each file of the profile."""
    return [
        (name, remote, size, sha256, state_of(folder / name, size, sha256))
        for name, (remote, size, sha256) in profile.files.items()
    ]


def fetch(session, url: str, target: Path, size: int, sha256: str, label: str) -> None:
    """Stream a file into `<target>.part`, check its size and hash, then move it."""
    part = target.with_name(target.name + ".part")
    digest = hashlib.sha256()
    started, done = time.monotonic(), 0
    with session.get(url, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        with part.open("wb") as handle:
            for block in response.iter_content(BLOCK):
                handle.write(block)
                digest.update(block)
                done += len(block)
                show(
                    progress_line(
                        label, round(done / 1e6), round(size / 1e6), started, "MB"
                    )
                )
    if done != size or digest.hexdigest() != sha256:
        part.unlink(missing_ok=True)
        raise ValueError(
            f"{label}: size or SHA-256 differs from the pin ({done} bytes)"
        )
    part.replace(target)


def main(argv: list[str] | None = None, session=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--embedder",
        choices=sorted(slug for slug, p in emb.PROFILES.items() if p.source),
        default=emb.GTE_MODERNBERT.slug,
    )
    parser.add_argument("--model-dir", type=Path, default=None)
    uc.add_write_mode(parser)
    args = parser.parse_args(argv)
    profile = emb.PROFILES[args.embedder]
    folder = args.model_dir or emb.default_model_dir(profile)
    rows = plan(profile, folder)
    print(f"Embedder {profile.id}, commit {profile.revision}")
    print(f"Target: {folder}")
    for name, remote, size, _sha, state in rows:
        print(f"  {name}: {state} ({size / 1e6:.1f} MB, from {remote})")
    todo = [row for row in rows if row[4] != "ok"]
    if not todo:
        print("The model files are already there and match. Nothing to fetch.")
        return 0
    total = sum(row[2] for row in todo)
    print(
        f"To fetch: {total / 1e6:.0f} MB. Time limits: {TIMEOUT[0]} s to connect and "
        f"{TIMEOUT[1]} s without data; the total time depends on the connection."
    )
    if not args.apply and (args.dry_run or not uc.confirm_apply()):
        print(uc.NOT_WRITTEN)
        return 0
    if session is None:
        import requests

        session = requests.Session()
    folder.mkdir(parents=True, exist_ok=True)
    for name, remote, size, sha256, _state in todo:
        fetch(session, url_of(profile, remote), folder / name, size, sha256, name)
        print(f"Fetched: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
