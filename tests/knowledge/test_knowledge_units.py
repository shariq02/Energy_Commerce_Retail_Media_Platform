"""Knowledge units -- conversion, approval, versioning and manifest.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

Conversion tests read the real sources and write only to a temporary folder. The
token test is skipped without the tokenizer file.
"""

from __future__ import annotations

import ast
import json
import re
import shutil
from collections import Counter

import pytest
import yaml

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import approve_units as au
from scripts.knowledge import build_units as bu
from scripts.knowledge import report_tokens as rt

pytestmark = [pytest.mark.unit, pytest.mark.ai]

ROOT = uc.ROOT
RULES_PER_SOURCE = {
    "dwd": 14,
    "honda_iot": 5,
    "mastr": 19,
    "power_plant_list": 8,
    "redispatch": 12,
    "rees46": 8,
    "smard": 10,
}
SOURCES = [{"path": "a.yml", "sha256": "0" * 64}]
OTHER_SOURCES = [{"path": "a.yml", "sha256": "1" * 64}]
TODAY = "2026-10-06"
LATER = "2026-10-07"


@pytest.fixture(scope="module")
def derived():
    return bu.derived_units(ROOT)


def build(corpus_dir):
    assert bu.main(["--corpus-dir", str(corpus_dir), "--apply"]) == 0


def create_unit(corpus_dir, unit_id="rule.x.a", body="# A\n\nbody", sources=SOURCES):
    plan = uc.plan_unit(corpus_dir, unit_id, "rule", "energy", sources, body, TODAY)
    if plan.text is not None:
        uc.write_text(plan.path, plan.text)
    return plan


def test_unit_counts(derived):
    kinds = Counter(u["kind"] for u in derived)
    assert kinds == {"metric": 8, "contract": 8, "rule": 76}
    metrics = [u for u in derived if u["kind"] == "metric"]
    assert Counter(u["ecosystem"] for u in metrics) == {"energy": 5, "commerce": 3}
    for source, count in RULES_PER_SOURCE.items():
        prefix = f"rule.{source}."
        assert sum(1 for u in derived if u["unit_id"].startswith(prefix)) == count
    assert not any(u["unit_id"].startswith("rule.ga4.") for u in derived)


def test_unit_ids_are_unique_and_safe_file_names(derived):
    ids = [u["unit_id"] for u in derived]
    assert len(ids) == len(set(ids))
    pattern = re.compile(r"^(metric|contract|rule)\.[^\s/\\]+$")
    assert [i for i in ids if not pattern.match(i)] == []


def test_excluded_fields_are_not_in_any_unit(derived):
    bodies = {u["unit_id"]: " ".join(u["body"].split()) for u in derived}
    for path in sorted((ROOT / bu.CONTRACTS_DIR).glob("*.yml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(contract, dict) or "source" not in contract:
            continue
        for rule in contract.get("quality_rules") or []:
            body = bodies[f"rule.{contract['source']}.{rule['id']}"]
            for field in ("evidence", "finding", "remediation"):
                if rule.get(field):
                    assert " ".join(str(rule[field]).split()) not in body
    for relative in bu.METRIC_FILES.values():
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        rows = next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(getattr(t, "id", "") == "METRICS" for t in node.targets)
        )
        for row in rows:
            body = bodies[f"metric.{row[0]}"]
            assert row[2] not in body and row[3] not in body
    labels = ("**Formula", "**Source mart", "**Evidence", "**Finding", "**Remediation")
    assert not any(label in body for body in bodies.values() for label in labels)


def test_build_writes_units_and_manifest(tmp_path):
    build(tmp_path)
    units = uc.scan_units(tmp_path)
    assert len(units) == 92
    for unit in units:
        assert set(uc.REQUIRED_HEADER) <= set(unit.header)
        assert unit.header["version"] == 1
        assert unit.approval_status == "pending"
        assert all(len(s["sha256"]) == 64 for s in unit.header["source"])
    manifest = json.loads((tmp_path / uc.MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["unit_count"] == 92
    assert manifest["corpus_version"] == 1
    assert manifest["units_by_kind"]["rule"] == 76
    assert manifest["approval"] == {"approved": 0, "pending": 92, "stale": 0}


def test_second_build_changes_nothing(tmp_path):
    build(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    build(tmp_path)
    after = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert before == after


def test_dry_run_writes_nothing(tmp_path):
    assert bu.main(["--corpus-dir", str(tmp_path)]) == 0
    assert not any(tmp_path.rglob("*"))


def test_the_dry_run_lists_every_new_unit(tmp_path, capsys):
    assert bu.main(["--corpus-dir", str(tmp_path), "--dry-run"]) == 0
    output = capsys.readouterr().out
    assert "Created (92):" in output
    assert "  rule.dwd.station_set  version 1, body " in output
    assert uc.NOT_WRITTEN in output


@pytest.mark.parametrize(
    ("answer", "written"),
    [("y", True), ("yes", True), ("n", False), ("no", False), ("", False)],
)
def test_without_a_flag_the_command_asks_before_it_writes(
    monkeypatch, tmp_path, answer, written
):
    asked = []
    monkeypatch.setattr(uc, "is_interactive", lambda: True)
    monkeypatch.setattr(uc, "ask", lambda question: asked.append(question) or answer)
    assert bu.main(["--corpus-dir", str(tmp_path)]) == 0
    assert len(asked) == 1
    assert any(tmp_path.rglob("*")) is written


def test_the_dry_run_flag_never_asks(monkeypatch, tmp_path):
    monkeypatch.setattr(uc, "is_interactive", lambda: True)
    monkeypatch.setattr(uc, "ask", lambda question: pytest.fail("asked"))
    assert bu.main(["--corpus-dir", str(tmp_path), "--dry-run"]) == 0
    assert not any(tmp_path.rglob("*"))


def test_nothing_to_write_means_no_question(monkeypatch, tmp_path, capsys):
    build(tmp_path)
    capsys.readouterr()
    monkeypatch.setattr(uc, "is_interactive", lambda: True)
    monkeypatch.setattr(uc, "ask", lambda question: pytest.fail("asked"))
    assert bu.main(["--corpus-dir", str(tmp_path)]) == 0
    assert "Nothing to write." in capsys.readouterr().out


def test_a_revised_unit_shows_versions_hashes_and_the_cleared_approval(tmp_path):
    plan = create_unit(tmp_path)
    unit = uc.read_unit(plan.path)
    uc.write_text(unit.path, au.approved_text(unit, "Owner", TODAY))
    again = uc.plan_unit(
        tmp_path, "rule.x.a", "rule", "energy", SOURCES, "# A\n\nnew", LATER
    )
    line = uc.plan_line(again)
    assert "version 1 -> 2" in line
    assert f"{uc.sha256_text('# A' + chr(10) + chr(10) + 'body')[:8]} ->" in line
    assert line.endswith("approval cleared")


def test_changed_body_raises_version_and_clears_approval(tmp_path):
    plan = create_unit(tmp_path)
    assert plan.action == "created"
    unit = uc.read_unit(plan.path)
    uc.write_text(unit.path, au.approved_text(unit, "Owner", TODAY))
    again = uc.plan_unit(
        tmp_path, "rule.x.a", "rule", "energy", SOURCES, "# A\n\nnew", LATER
    )
    assert again.action == "revised"
    assert again.was_approved
    header, _ = uc.parse_unit(again.text)
    assert header["version"] == 2
    assert header["last_updated"] == LATER
    assert header["approval"]["status"] == "pending"


def test_new_source_hash_keeps_version_and_approval(tmp_path):
    plan = create_unit(tmp_path)
    unit = uc.read_unit(plan.path)
    uc.write_text(unit.path, au.approved_text(unit, "Owner", TODAY))
    again = uc.plan_unit(
        tmp_path, "rule.x.a", "rule", "energy", OTHER_SOURCES, "# A\n\nbody", LATER
    )
    assert again.action == "refreshed"
    header, _ = uc.parse_unit(again.text)
    assert header["version"] == 1
    assert header["last_updated"] == TODAY
    assert header["approval"]["status"] == "approved"
    assert header["source"] == OTHER_SOURCES


def test_unchanged_unit_is_not_rewritten(tmp_path):
    create_unit(tmp_path)
    again = uc.plan_unit(
        tmp_path, "rule.x.a", "rule", "energy", SOURCES, "# A\n\nbody", TODAY
    )
    assert again.action == "unchanged"
    assert again.text is None


def test_approval_covers_only_the_chosen_kind(tmp_path):
    build(tmp_path)
    argv = ["--approver", "Owner", "--kind", "metric", "--corpus-dir", str(tmp_path)]
    assert au.main([*argv, "--apply"]) == 0
    units = uc.scan_units(tmp_path)
    approved = [u for u in units if u.approval_status == "approved"]
    assert len(approved) == 8
    assert {u.kind for u in approved} == {"metric"}
    assert all(u.header["approval"]["approved_version"] == 1 for u in approved)
    manifest = json.loads((tmp_path / uc.MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["approval"] == {"approved": 8, "pending": 84, "stale": 0}
    assert manifest["corpus_version"] == 1
    assert au.select_units(units, ["metric"], [], False) == []


def test_approval_dry_run_writes_nothing(tmp_path):
    build(tmp_path)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    argv = ["--approver", "Owner", "--all-pending", "--corpus-dir", str(tmp_path)]
    assert au.main(argv) == 0
    assert before == {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}


def test_approval_needs_a_selection_and_a_known_unit(tmp_path):
    build(tmp_path)
    with pytest.raises(SystemExit):
        au.main(["--approver", "Owner", "--corpus-dir", str(tmp_path)])
    with pytest.raises(SystemExit):
        au.main(["--approver", " ", "--all-pending", "--corpus-dir", str(tmp_path)])
    with pytest.raises(ValueError):
        au.select_units(uc.scan_units(tmp_path), [], ["rule.nope.nothing"], False)


def test_corpus_version_rises_only_when_content_changes(tmp_path):
    build(tmp_path)
    unit = uc.scan_units(tmp_path)[0]
    header = {**unit.header, "version": 2}
    uc.write_text(unit.path, uc.render_unit(header, unit.body + "\n\nextra"))
    assert uc.write_manifest(tmp_path)["corpus_version"] == 2
    assert uc.write_manifest(tmp_path)["corpus_version"] == 2


def test_manifest_keeps_keys_added_by_other_code(tmp_path):
    build(tmp_path)
    path = tmp_path / uc.MANIFEST_NAME
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["embedding"] = {"model": "example"}
    path.write_text(json.dumps(manifest), encoding="utf-8")
    assert uc.write_manifest(tmp_path)["embedding"] == {"model": "example"}


def test_render_and_parse_round_trip():
    header = {
        "unit_id": "rule.x.a",
        "kind": "rule",
        "last_updated": TODAY,
        "approval": uc.pending_approval(),
    }
    body = "# T\n\ntext"
    parsed_header, parsed_body = uc.parse_unit(uc.render_unit(header, body))
    assert parsed_header == header
    assert parsed_body == body


@pytest.mark.parametrize("text", ["no header", "---\nunit_id: a\n"])
def test_parse_rejects_bad_headers(text):
    with pytest.raises(ValueError):
        uc.parse_unit(text)


def test_scan_rejects_a_unit_in_the_wrong_folder(tmp_path):
    build(tmp_path)
    source = next((tmp_path / "rules").glob("*.md"))
    (tmp_path / "metrics").mkdir(exist_ok=True)
    shutil.move(str(source), str(tmp_path / "metrics" / source.name))
    with pytest.raises(ValueError):
        uc.scan_units(tmp_path)


def test_token_report_stops_when_the_tokenizer_is_missing(tmp_path):
    argv = ["--tokenizer", str(tmp_path / "none.json"), "--corpus-dir", str(tmp_path)]
    assert rt.main(argv) == 2


@pytest.mark.skipif(
    not rt.DEFAULT_TOKENIZER.is_file(), reason="embedding tokenizer file not present"
)
def test_token_classes():
    pytest.importorskip("tokenizers")
    tokenizer = rt.load_tokenizer(rt.DEFAULT_TOKENIZER)
    assert rt.classify(tokenizer, "# Rule\n\nshort text")[0] == "ok"
    paragraph = " ".join(["energy"] * 100)
    assert rt.classify(tokenizer, "\n\n".join([paragraph] * 3))[0] == "split"
    assert rt.classify(tokenizer, " ".join(["energy"] * 400))[0] == "fail"


GOOD_HASH = "a" * 64


def test_sources_accept_a_tracked_path_and_a_logical_name():
    uc.validate_sources([{"path": "src/a.yml", "sha256": GOOD_HASH}], "u")
    uc.validate_sources([{"name": "governance_design", "sha256": GOOD_HASH}], "u")


@pytest.mark.parametrize(
    "sources",
    [
        [],
        "not a list",
        [{"path": "docs/design/a.md", "sha256": GOOD_HASH}],
        [{"path": "/home/user/a.md", "sha256": GOOD_HASH}],
        [{"path": "a.md", "name": "b", "sha256": GOOD_HASH}],
        [{"sha256": GOOD_HASH}],
        [{"path": "a.md", "sha256": "short"}],
        [{"name": "Bad Name", "sha256": GOOD_HASH}],
        [{"path": "a.md", "sha256": GOOD_HASH, "extra": 1}],
    ],
)
def test_sources_reject_bad_entries(sources):
    with pytest.raises(ValueError):
        uc.validate_sources(sources, "u")


def test_a_unit_with_a_docs_path_is_rejected_on_read(tmp_path):
    header = {
        "unit_id": "rule.x.a",
        "kind": "rule",
        "ecosystem": "energy",
        "source": [{"path": "docs/design/a.md", "sha256": GOOD_HASH}],
        "version": 1,
        "last_updated": TODAY,
        "approval": uc.pending_approval(),
    }
    path = uc.unit_path(tmp_path, "rule", "rule.x.a")
    uc.write_text(path, uc.render_unit(header, "# A\n\nbody"))
    with pytest.raises(ValueError):
        uc.read_unit(path)


def test_named_source_entry_hashes_the_local_file(tmp_path):
    document = tmp_path / "doc.md"
    document.write_bytes(b"line one\r\nline two\r\n")
    mapping = tmp_path / "local_sources.yml"
    mapping.write_text("governance_design: doc.md\n", encoding="utf-8")
    local = uc.load_local_sources(mapping, root=tmp_path)
    entry = uc.named_source_entry("governance_design", local)
    assert entry == {
        "name": "governance_design",
        "sha256": uc.sha256_bytes(b"line one\nline two\n"),
    }
    uc.validate_sources([entry], "u")


def test_named_source_errors_are_clear(tmp_path):
    with pytest.raises(FileNotFoundError):
        uc.load_local_sources(tmp_path / "missing.yml", root=tmp_path)
    with pytest.raises(KeyError):
        uc.named_source_entry("unknown", {})
    with pytest.raises(FileNotFoundError):
        uc.named_source_entry("x", {"x": tmp_path / "nothing.md"})


def test_the_example_local_sources_file_is_valid():
    example = uc.LOCAL_SOURCES_FILE.with_name("local_sources.example.yml")
    data = yaml.safe_load(example.read_text(encoding="utf-8"))
    assert data
    assert all(re.match(r"^[a-z0-9_]+$", name) for name in data)
    assert all(isinstance(path, str) for path in data.values())
    assert not any("docs" in path.split("/") for path in data.values())


def test_committed_units_hold_no_reference_to_planning_documents():
    units = uc.scan_units(uc.CORPUS_DIR)
    if not units:
        pytest.skip("no units on this machine")
    pattern = re.compile(
        r"ADR-\d|UC-\d|Entry \d{3}|Phase \d|docs/|CHANGELOG|PROJECT_"
        + "PLAN|MASTER_BUILD"
    )
    offenders = [u.unit_id for u in units if pattern.search(u.body)]
    assert offenders == []


def test_a_hand_edited_body_makes_the_approval_stale(tmp_path):
    build(tmp_path)
    argv = ["--approver", "Owner", "--kind", "metric", "--corpus-dir", str(tmp_path)]
    assert au.main([*argv, "--apply"]) == 0
    unit = next(u for u in uc.scan_units(tmp_path) if u.kind == "metric")
    uc.write_text(unit.path, uc.render_unit(unit.header, unit.body + "\n\nextra"))
    edited = next(u for u in uc.scan_units(tmp_path) if u.unit_id == unit.unit_id)
    assert edited.approval_status == "stale"
    assert au.needs_approval(edited)
    manifest = uc.write_manifest(tmp_path)
    assert manifest["approval"] == {"approved": 7, "pending": 84, "stale": 1}


def test_an_approval_without_a_body_hash_is_stale(tmp_path):
    plan = create_unit(tmp_path)
    unit = uc.read_unit(plan.path)
    approval = {**uc.pending_approval(), "status": "approved", "approved_version": 1}
    del approval["approved_content_hash"]
    header = {**unit.header, "approval": approval}
    uc.write_text(unit.path, uc.render_unit(header, unit.body))
    assert uc.read_unit(unit.path).approval_status == "stale"


@pytest.mark.parametrize("unit_id", ["../x", "rule/x", "Rule.A", "rule.x.a\n", ""])
def test_a_unit_id_must_be_safe_as_a_file_name(tmp_path, unit_id):
    with pytest.raises(ValueError):
        uc.plan_unit(tmp_path, unit_id, "rule", "energy", SOURCES, "# A", TODAY)


@pytest.mark.parametrize("path", ["../x.yml", "a/../../x.yml", "docs/x.md", "/etc/x"])
def test_a_source_path_stays_inside_the_repository(path):
    with pytest.raises(ValueError):
        uc.validate_sources([{"path": path, "sha256": "0" * 64}], "unit")
