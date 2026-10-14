"""Shared helpers for the knowledge corpus units.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Reads, writes and hashes the Markdown units in ai/knowledge_corpus/ and keeps the
manifest in step. Library only; the commands are in this folder.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CORPUS_DIR = ROOT / "ai" / "knowledge_corpus"
MANIFEST_NAME = "manifest.json"
KIND_DIRS = {
    "metric": "metrics",
    "contract": "contracts",
    "rule": "rules",
    "governance": "governance",
    "pipeline": "pipeline",
    "glossary": "glossary",
}
DERIVED_KINDS = ("metric", "contract", "rule")
REQUIRED_HEADER = (
    "unit_id",
    "kind",
    "ecosystem",
    "source",
    "version",
    "last_updated",
    "approval",
)
LOCAL_SOURCES_FILE = ROOT / "scripts" / "knowledge" / "local_sources.yml"
_FENCE = "---"
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_SOURCE_NAME = re.compile(r"^[a-z0-9_]+$")
_UNIT_ID = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    # Line endings are normalised so a checkout on another system hashes the same.
    return sha256_bytes(path.read_bytes().replace(b"\r\n", b"\n"))


def validate_unit_id(unit_id: object, where: str) -> None:
    """A unit id becomes a file name, so it may hold only safe characters."""
    if not _UNIT_ID.fullmatch(str(unit_id)):
        raise ValueError(f"{where}: bad unit id {unit_id!r}")


def validate_sources(sources: object, where: str) -> None:
    """Check the source entries of a unit header.

    A source is a repository path or, for an untracked file, a logical name; both
    carry the file's SHA-256. A path never points into ``docs`` or leaves the repo.
    """
    if not isinstance(sources, list) or not sources:
        raise ValueError(f"{where}: source must be a non-empty list")
    for entry in sources:
        if not isinstance(entry, dict) or not set(entry) <= {"path", "name", "sha256"}:
            raise ValueError(f"{where}: bad source entry {entry!r}")
        if len({"path", "name"} & set(entry)) != 1:
            raise ValueError(f"{where}: a source has exactly one of path and name")
        if not _SHA256.match(str(entry.get("sha256", ""))):
            raise ValueError(f"{where}: a source has no valid sha256")
        if "name" in entry and not _SOURCE_NAME.match(str(entry["name"])):
            raise ValueError(f"{where}: bad source name {entry['name']!r}")
        if "path" in entry:
            path = Path(str(entry["path"]))
            if (
                path.is_absolute()
                or "\\" in str(entry["path"])
                or {"docs", ".."} & set(path.parts)
            ):
                raise ValueError(f"{where}: a source path must be a tracked path")


def load_local_sources(
    path: Path = LOCAL_SOURCES_FILE, root: Path = ROOT
) -> dict[str, Path]:
    """Map logical source names to files on this machine (never committed)."""
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found: copy local_sources.example.yml to it and set the paths"
        )
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {str(name): root / str(relative) for name, relative in data.items()}


def named_source_entry(name: str, local_sources: dict[str, Path]) -> dict:
    if name not in local_sources:
        raise KeyError(f"no local path for the source {name!r}")
    file = local_sources[name]
    if not file.is_file():
        raise FileNotFoundError(f"source {name!r}: {file} is not a file")
    return {"name": name, "sha256": sha256_file(file)}


def pending_approval() -> dict:
    return {
        "status": "pending",
        "approver": None,
        "approved_at": None,
        "approved_version": None,
        "approved_content_hash": None,
    }


def render_unit(header: dict, body: str) -> str:
    head = yaml.safe_dump(header, sort_keys=False, allow_unicode=True, width=1000)
    return f"{_FENCE}\n{head}{_FENCE}\n\n{body.strip()}\n"


def parse_unit(text: str) -> tuple[dict, str]:
    if not text.startswith(_FENCE + "\n"):
        raise ValueError("unit has no header")
    end = text.find("\n" + _FENCE + "\n", len(_FENCE))
    if end < 0:
        raise ValueError("unit header is not closed")
    header = yaml.safe_load(text[len(_FENCE) + 1 : end]) or {}
    body = text[end + len(_FENCE) + 2 :].strip()
    return header, body


@dataclass
class Unit:
    path: Path
    header: dict
    body: str

    @property
    def unit_id(self) -> str:
        return self.header["unit_id"]

    @property
    def kind(self) -> str:
        return self.header["kind"]

    @property
    def content_hash(self) -> str:
        return sha256_text(self.body)

    @property
    def approval_status(self) -> str:
        """pending, approved, or stale: approved, but the body is no longer the
        text that was approved (or no body hash was recorded)."""
        approval = self.header["approval"]
        if approval["status"] != "approved":
            return approval["status"]
        if approval.get("approved_content_hash") != self.content_hash:
            return "stale"
        return "approved"


@dataclass
class Plan:
    unit_id: str
    kind: str
    path: Path
    action: str
    text: str | None
    was_approved: bool
    old_version: int | None = None
    new_version: int | None = None
    old_hash: str | None = None
    new_hash: str | None = None


def unit_path(corpus_dir: Path, kind: str, unit_id: str) -> Path:
    return corpus_dir / KIND_DIRS[kind] / f"{unit_id}.md"


def read_unit(path: Path) -> Unit:
    header, body = parse_unit(path.read_text(encoding="utf-8"))
    missing = [key for key in REQUIRED_HEADER if key not in header]
    if missing:
        raise ValueError(f"{path}: header lacks {missing}")
    if header["kind"] not in KIND_DIRS:
        raise ValueError(f"{path}: unknown kind {header['kind']!r}")
    validate_unit_id(header["unit_id"], str(path))
    if path.stem != header["unit_id"] or path.parent.name != KIND_DIRS[header["kind"]]:
        raise ValueError(f"{path}: file name or folder does not match the header")
    validate_sources(header["source"], str(path))
    return Unit(path=path, header=header, body=body)


def scan_units(corpus_dir: Path) -> list[Unit]:
    units = [
        read_unit(path)
        for folder in KIND_DIRS.values()
        for path in sorted((corpus_dir / folder).glob("*.md"))
    ]
    return sorted(units, key=lambda unit: unit.unit_id)


def plan_unit(
    corpus_dir: Path,
    unit_id: str,
    kind: str,
    ecosystem: str,
    sources: list[dict],
    body: str,
    today: str,
) -> Plan:
    """Decide what writing this unit would do; nothing is written.

    created: new, pending. revised: body changed, version rises, approval cleared.
    refreshed: same body, other header values -- version and approval kept.
    unchanged: no write.
    """
    validate_unit_id(unit_id, unit_id)
    validate_sources(sources, unit_id)
    body = body.strip()
    path = unit_path(corpus_dir, kind, unit_id)
    header = {
        "unit_id": unit_id,
        "kind": kind,
        "ecosystem": ecosystem,
        "source": sources,
        "version": 1,
        "last_updated": today,
        "approval": pending_approval(),
    }
    if not path.exists():
        text = render_unit(header, body)
        return Plan(
            unit_id,
            kind,
            path,
            "created",
            text,
            False,
            new_version=1,
            new_hash=sha256_text(body),
        )
    old_header, old_body = parse_unit(path.read_text(encoding="utf-8"))
    was_approved = old_header["approval"]["status"] == "approved"
    if sha256_text(old_body) != sha256_text(body):
        header["version"] = int(old_header["version"]) + 1
        text = render_unit(header, body)
        return Plan(
            unit_id,
            kind,
            path,
            "revised",
            text,
            was_approved,
            old_version=int(old_header["version"]),
            new_version=header["version"],
            old_hash=sha256_text(old_body),
            new_hash=sha256_text(body),
        )
    header["version"] = old_header["version"]
    header["last_updated"] = old_header["last_updated"]
    header["approval"] = old_header["approval"]
    if header == old_header:
        return Plan(unit_id, kind, path, "unchanged", None, was_approved)
    text = render_unit(header, body)
    return Plan(unit_id, kind, path, "refreshed", text, was_approved)


ACTIONS = ("created", "revised", "refreshed", "unchanged")
NOT_WRITTEN = "Nothing written. Use --apply, or answer y when asked, to write."


def add_write_mode(parser) -> None:
    """--apply writes without asking; --dry-run only prints; neither asks at the end."""
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--apply", action="store_true", help="write without asking")
    group.add_argument("--dry-run", action="store_true", help="print the plan only")


def is_interactive() -> bool:
    return sys.stdin.isatty()


def ask(question: str) -> str:
    return input(question).strip()


def is_yes(answer: str) -> bool:
    return answer.strip().lower() in ("y", "yes")


def confirm_apply() -> bool:
    """Ask once whether to write. Only y or yes means yes; no terminal means no."""
    if not is_interactive():
        return False
    try:
        return is_yes(ask("Apply these changes? (y/yes or n/no): "))
    except EOFError:
        return False


def plan_line(plan: Plan) -> str:
    if plan.action == "created":
        return f"{plan.unit_id}  version 1, body {plan.new_hash[:8]}"
    if plan.action == "revised":
        line = (
            f"{plan.unit_id}  version {plan.old_version} -> {plan.new_version}, "
            f"body {plan.old_hash[:8]} -> {plan.new_hash[:8]}"
        )
        return line + (", approval cleared" if plan.was_approved else "")
    return f"{plan.unit_id}  source hash updated"


def print_plans(plans: list[Plan], kinds: tuple[str, ...]) -> None:
    """Counts per kind, then the ids of every created, revised and refreshed unit."""
    counts = Counter((plan.kind, plan.action) for plan in plans)
    for kind in kinds:
        print(f"{kind}: " + ", ".join(f"{a} {counts[(kind, a)]}" for a in ACTIONS))
    for action in ("created", "revised", "refreshed"):
        chosen = [plan for plan in plans if plan.action == action]
        if chosen:
            print(f"{action.capitalize()} ({len(chosen)}):")
            for plan in chosen:
                print(f"  {plan_line(plan)}")


def write_plans(plans: list[Plan], corpus_dir: Path, apply: bool, dry_run: bool) -> int:
    """Write the planned units and the manifest. Without --apply, ask first."""
    if not apply:
        if all(plan.text is None for plan in plans):
            print("Nothing to write.")
            return 0
        if dry_run or not confirm_apply():
            print(NOT_WRITTEN)
            return 0
    for plan in plans:
        if plan.text is not None:
            write_text(plan.path, plan.text)
    manifest = write_manifest(corpus_dir)
    print(
        f"Manifest: corpus_version {manifest['corpus_version']}, "
        f"{manifest['unit_count']} unit(s), approval {manifest['approval']}"
    )
    return 0


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def fingerprint(units: list[Unit]) -> str:
    lines = sorted(
        f"{unit.unit_id}\t{unit.header['version']}\t{unit.content_hash}"
        for unit in units
    )
    return sha256_text("\n".join(lines))


def write_manifest(corpus_dir: Path) -> dict:
    """Rebuild the owned manifest fields; keys that other code added are kept."""
    units = scan_units(corpus_dir)
    path = corpus_dir / MANIFEST_NAME
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    current = fingerprint(units)
    version = previous.get("corpus_version", 0)
    if current != previous.get("fingerprint"):
        version += 1
    manifest = dict(previous)
    manifest.update(
        {
            "corpus_version": version,
            "fingerprint": current,
            "unit_count": len(units),
            "units_by_kind": {
                kind: sum(1 for unit in units if unit.kind == kind)
                for kind in KIND_DIRS
            },
            "approval": {
                status: sum(1 for unit in units if unit.approval_status == status)
                for status in ("approved", "pending", "stale")
            },
            "units": [
                {
                    "unit_id": unit.unit_id,
                    "kind": unit.kind,
                    "ecosystem": unit.header["ecosystem"],
                    "path": f"{KIND_DIRS[unit.kind]}/{unit.unit_id}.md",
                    "version": unit.header["version"],
                    "content_hash": unit.content_hash,
                    "approval_status": unit.approval_status,
                }
                for unit in units
            ],
        }
    )
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if not path.exists() or path.read_text(encoding="utf-8") != text:
        write_text(path, text)
    return manifest
