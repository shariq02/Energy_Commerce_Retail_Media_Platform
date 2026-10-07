"""Knowledge author_units -- index checks, header, versioning and dry run.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

All tests write only to a temporary folder.
"""

from __future__ import annotations

import json

import pytest
import yaml

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units
from scripts.knowledge import author_units as au

pytestmark = [pytest.mark.unit, pytest.mark.ai]

UNIT_ID = "governance.access_rules"
ENTRY = {
    "unit_id": UNIT_ID,
    "kind": "governance",
    "ecosystem": "shared",
    "sources": ["governance_design"],
}


def setup_folder(tmp_path, entries=(ENTRY,), body="# Access rules\n\ntext"):
    authored = tmp_path / "authored"
    authored.mkdir(parents=True)
    (authored / au.INDEX_NAME).write_text(
        yaml.safe_dump({"units": list(entries)}), encoding="utf-8"
    )
    (authored / f"{UNIT_ID}.md").write_text(body, encoding="utf-8")
    source = tmp_path / "source.md"
    source.write_text("source text", encoding="utf-8")
    local = tmp_path / "local_sources.yml"
    local.write_text(f"governance_design: {source}\n", encoding="utf-8")
    corpus = tmp_path / "corpus"
    argv = [
        "--authored-dir",
        str(authored),
        "--corpus-dir",
        str(corpus),
        "--local-sources",
        str(local),
    ]
    return argv, authored, corpus, source


def test_dry_run_writes_nothing(tmp_path):
    argv, _, corpus, _ = setup_folder(tmp_path)
    assert au.main(argv) == 0
    assert not corpus.exists()


def test_apply_writes_a_pending_unit_with_a_named_source(tmp_path):
    argv, _, corpus, source = setup_folder(tmp_path)
    assert au.main([*argv, "--apply"]) == 0
    unit = uc.read_unit(corpus / "governance" / f"{UNIT_ID}.md")
    assert unit.header["ecosystem"] == "shared"
    assert unit.header["version"] == 1
    assert unit.approval_status == "pending"
    assert unit.header["source"] == [
        {"name": "governance_design", "sha256": uc.sha256_file(source)}
    ]
    assert str(source) not in (corpus / "governance" / f"{UNIT_ID}.md").read_text(
        encoding="utf-8"
    )
    manifest = json.loads((corpus / uc.MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["units_by_kind"]["governance"] == 1


def test_a_second_apply_changes_nothing(tmp_path):
    argv, _, corpus, _ = setup_folder(tmp_path)
    assert au.main([*argv, "--apply"]) == 0
    before = {p: p.read_bytes() for p in corpus.rglob("*") if p.is_file()}
    assert au.main([*argv, "--apply"]) == 0
    assert before == {p: p.read_bytes() for p in corpus.rglob("*") if p.is_file()}


def test_a_changed_body_raises_the_version_and_clears_the_approval(tmp_path):
    argv, authored, corpus, _ = setup_folder(tmp_path)
    assert au.main([*argv, "--apply"]) == 0
    approve = ["--approver", "Owner", "--all-pending", "--corpus-dir", str(corpus)]
    assert approve_units.main([*approve, "--apply"]) == 0
    (authored / f"{UNIT_ID}.md").write_text("# Access rules\n\nnew", encoding="utf-8")
    assert au.main([*argv, "--apply"]) == 0
    unit = uc.read_unit(corpus / "governance" / f"{UNIT_ID}.md")
    assert unit.header["version"] == 2
    assert unit.approval_status == "pending"


def test_a_new_source_hash_keeps_the_version_and_the_approval(tmp_path):
    argv, _, corpus, source = setup_folder(tmp_path)
    assert au.main([*argv, "--apply"]) == 0
    approve = ["--approver", "Owner", "--all-pending", "--corpus-dir", str(corpus)]
    assert approve_units.main([*approve, "--apply"]) == 0
    source.write_text("changed source", encoding="utf-8")
    assert au.main([*argv, "--apply"]) == 0
    unit = uc.read_unit(corpus / "governance" / f"{UNIT_ID}.md")
    assert unit.header["version"] == 1
    assert unit.approval_status == "approved"
    assert unit.header["source"][0]["sha256"] == uc.sha256_file(source)


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "metric"},
        {"kind": "rule"},
        {"ecosystem": "mars"},
        {"unit_id": "governance.Bad Id"},
        {"unit_id": "pipeline.access_rules"},
        {"sources": []},
        {"sources": "governance_design"},
    ],
)
def test_a_bad_index_entry_is_rejected(tmp_path, change):
    argv, authored, _, _ = setup_folder(tmp_path, entries=({**ENTRY, **change},))
    with pytest.raises(ValueError):
        au.read_index(authored / au.INDEX_NAME)
    assert au.main(argv) == 2


def test_duplicate_ids_and_extra_keys_are_rejected(tmp_path):
    _, authored, _, _ = setup_folder(tmp_path, entries=(ENTRY, ENTRY))
    with pytest.raises(ValueError):
        au.read_index(authored / au.INDEX_NAME)
    _, authored, _, _ = setup_folder(tmp_path / "x", entries=({**ENTRY, "extra": 1},))
    with pytest.raises(ValueError):
        au.read_index(authored / au.INDEX_NAME)


def test_a_missing_body_an_empty_body_or_an_unknown_source_stops_the_run(tmp_path):
    argv, authored, _, _ = setup_folder(tmp_path)
    local = uc.load_local_sources(tmp_path / "local_sources.yml", root=tmp_path)
    (authored / f"{UNIT_ID}.md").write_text("  \n", encoding="utf-8")
    with pytest.raises(ValueError):
        au.authored_units(authored, local)
    (authored / f"{UNIT_ID}.md").unlink()
    with pytest.raises(FileNotFoundError):
        au.authored_units(authored, local)
    assert au.main(argv) == 2
    (authored / f"{UNIT_ID}.md").write_text("# T\n\nx", encoding="utf-8")
    with pytest.raises(KeyError):
        au.authored_units(authored, {})


def test_the_dry_run_lists_the_unit_and_never_asks(monkeypatch, tmp_path, capsys):
    argv, _, corpus, _ = setup_folder(tmp_path)
    monkeypatch.setattr(uc, "is_interactive", lambda: True)
    monkeypatch.setattr(uc, "ask", lambda question: pytest.fail("asked"))
    assert au.main([*argv, "--dry-run"]) == 0
    assert f"  {UNIT_ID}  version 1, body " in capsys.readouterr().out
    assert not corpus.exists()


@pytest.mark.parametrize(("answer", "written"), [("yes", True), ("no", False)])
def test_without_a_flag_the_command_asks_before_it_writes(
    monkeypatch, tmp_path, answer, written
):
    argv, _, corpus, _ = setup_folder(tmp_path)
    monkeypatch.setattr(uc, "is_interactive", lambda: True)
    monkeypatch.setattr(uc, "ask", lambda question: answer)
    assert au.main(argv) == 0
    assert corpus.exists() is written
