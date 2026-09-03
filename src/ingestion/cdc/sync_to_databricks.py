"""CDC pipeline -- upload landed files to the Databricks Volume.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: August 2026

Purpose: push the JSON Lines files under data/cdc/landing/<table>/ into the
Unity Catalog Volume the Databricks batch step reads. Local disk is the source
-- there is no object-store hop. Uploads are retried, a per-file checkpoint
skips files already sent, and a run manifest is written next to the data.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

from src.ingestion.cdc import config

_TIMEOUT = (30, 180)
_MAX_RETRIES = 4
_CHECKPOINT = config.STATE_DIR / "upload_checkpoint.json"


def _headers() -> dict:
    return {"Authorization": f"Bearer {config.DATABRICKS_TOKEN}"}


def _load_checkpoint() -> dict:
    if _CHECKPOINT.exists():
        return json.loads(_CHECKPOINT.read_text(encoding="utf-8"))
    return {}


def _save_checkpoint(data: dict) -> None:
    _CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _local_files() -> list[Path]:
    files: list[Path] = []
    for table in config.TABLES:
        files.extend(sorted(config.landing_dir(table).glob("*.jsonl")))
    files.extend(sorted(config.LANDING_DIR.glob("_manifest__*.json")))
    files.extend(sorted(config.SNAPSHOT_DIR.glob("*.json")))
    return files


def _rel_key(local: Path) -> str:
    if local.is_relative_to(config.LANDING_DIR):
        return local.relative_to(config.LANDING_DIR).as_posix()
    return local.relative_to(config.CDC_DATA_DIR).as_posix()


def _volume_target(local: Path) -> str:
    return f"{config.volume_root()}/{_rel_key(local)}"


def _upload(local: Path, target: str) -> bool:
    url = f"{config.DATABRICKS_HOST}/api/2.0/fs/files{target}?overwrite=true"
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with local.open("rb") as handle:
                resp = requests.put(
                    url, headers=_headers(), data=handle, timeout=_TIMEOUT
                )
            if resp.status_code in (200, 204):
                return True
            print(f"    attempt {attempt}: HTTP {resp.status_code} {resp.text[:120]}")
        except requests.RequestException as exc:
            print(f"    attempt {attempt}: {type(exc).__name__}: {str(exc)[:100]}")
        if attempt < _MAX_RETRIES:
            time.sleep(5 * attempt)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload CDC landing files to Databricks"
    )
    parser.add_argument(
        "--force", action="store_true", help="re-upload files already in the checkpoint"
    )
    args = parser.parse_args()

    if not config.DATABRICKS_HOST or not config.DATABRICKS_TOKEN:
        print("FAIL  DATABRICKS_HOST / DATABRICKS_TOKEN_DBT not set in .env")
        return 1

    checkpoint = {} if args.force else _load_checkpoint()
    files = _local_files()
    if not files:
        print(
            "no landing files found under data/cdc/landing/ -- run the consumer first"
        )
        return 0

    uploaded, skipped, failed = 0, 0, 0
    for local in files:
        key = _rel_key(local)
        stat = local.stat()
        fingerprint = f"{stat.st_size}:{int(stat.st_mtime)}"
        if checkpoint.get(key) == fingerprint:
            skipped += 1
            continue
        target = _volume_target(local)
        print(f"  {key} -> {target}")
        if _upload(local, target):
            checkpoint[key] = fingerprint
            uploaded += 1
        else:
            failed += 1

    _save_checkpoint(checkpoint)
    manifest = {
        "synced_at": datetime.now(tz=UTC).isoformat(),
        "volume_root": config.volume_root(),
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
    }
    (config.STATE_DIR / "last_sync.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    print("=" * 70)
    print(f"CDC SYNC: uploaded={uploaded} skipped={skipped} failed={failed}")
    print("=" * 70)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
