"""Knowledge run_all -- order, stop on failure and the approval boundary.

Energy Commerce and Retail Media Analytics Platform
Author: Sharique Mohammad
Date: October 2026
"""

from __future__ import annotations

from collections import Counter

import pytest

from scripts.knowledge import _unit_common as uc
from scripts.knowledge import run_all as ra

pytestmark = [pytest.mark.unit, pytest.mark.ai]


def test_steps_run_in_order():
    assert [name for name, _ in ra.STEPS] == ["units", "tokens"]


def test_dry_run_writes_nothing(tmp_path, capsys):
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
    assert not any(tmp_path.rglob("*"))
    output = capsys.readouterr().out
    assert "token check is skipped" in output
    assert "Dry run: nothing was written" in output


def test_a_failing_step_stops_the_run(monkeypatch, tmp_path, capsys):
    calls = []

    def ok(args):
        calls.append("first")
        return 0

    def bad(args):
        calls.append("second")
        return 3

    def never(args):
        calls.append("third")
        return 0

    monkeypatch.setattr(ra, "STEPS", [("a", ok), ("b", bad), ("c", never)])
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 3
    assert calls == ["first", "second"]
    assert "Stopped at step b (exit code 3)" in capsys.readouterr().out


def test_apply_builds_units_and_stops_when_the_tokenizer_is_missing(tmp_path, capsys):
    argv = [
        "--corpus-dir",
        str(tmp_path),
        "--tokenizer",
        str(tmp_path / "none.json"),
        "--apply",
    ]
    assert ra.main(argv) == 2
    assert len(uc.scan_units(tmp_path)) == 92
    assert "Stopped at step tokens (exit code 2)" in capsys.readouterr().out


def run_units_only(monkeypatch, tmp_path, answers, extra=(), interactive=True):
    """Run the build step only, answering the prompts from `answers`."""
    asked = []

    def fake_ask(question):
        asked.append(question)
        return answers.pop(0)

    monkeypatch.setattr(ra, "STEPS", [("units", ra.step_units)])
    monkeypatch.setattr(ra, "ask", fake_ask)
    monkeypatch.setattr(ra, "is_interactive", lambda: interactive)
    argv = ["--corpus-dir", str(tmp_path), "--apply", *extra]
    return ra.main(argv), asked


def approved_kinds(corpus_dir):
    units = uc.scan_units(corpus_dir)
    return Counter(u.kind for u in units if u.approval_status == "approved")


def test_without_a_terminal_the_hint_is_printed_and_nothing_is_approved(
    monkeypatch, tmp_path, capsys
):
    code, asked = run_units_only(monkeypatch, tmp_path, [], interactive=False)
    assert code == 0
    assert asked == []
    output = capsys.readouterr().out
    assert "waiting for approval: 92" in output
    assert "scripts.knowledge.approve_units" in output
    assert approved_kinds(tmp_path) == {}


@pytest.mark.parametrize(
    ("choice", "expected"),
    [
        ("1", {"metric": 8, "contract": 8, "rule": 76}),
        ("2", {"metric": 8}),
        ("3", {"contract": 8}),
        ("4", {"rule": 76}),
        ("5", {}),
        ("all", {}),
        ("x", {}),
    ],
)
def test_menu_choice_decides_what_is_approved(monkeypatch, tmp_path, choice, expected):
    answers = [choice, "Owner"]
    code, _ = run_units_only(monkeypatch, tmp_path, answers)
    assert code == 0
    assert approved_kinds(tmp_path) == expected


def test_approval_records_the_name_and_the_version(monkeypatch, tmp_path):
    run_units_only(monkeypatch, tmp_path, ["2", "Owner"])
    unit = next(u for u in uc.scan_units(tmp_path) if u.kind == "metric")
    approval = unit.header["approval"]
    assert approval["approver"] == "Owner"
    assert approval["approved_version"] == unit.header["version"]
    manifest = uc.write_manifest(tmp_path)
    assert manifest["approval"] == {"approved": 8, "pending": 84, "stale": 0}


def test_an_empty_name_means_no_approval(monkeypatch, tmp_path):
    run_units_only(monkeypatch, tmp_path, ["1", ""])
    assert approved_kinds(tmp_path) == {}


def test_the_approver_argument_skips_the_name_question(monkeypatch, tmp_path):
    extra = ["--approver", "Owner"]
    _, asked = run_units_only(monkeypatch, tmp_path, ["2"], extra=extra)
    assert len(asked) == 1
    assert approved_kinds(tmp_path) == {"metric": 8}


def test_a_dry_run_never_asks(monkeypatch, tmp_path):
    monkeypatch.setattr(ra, "STEPS", [("units", ra.step_units)])
    monkeypatch.setattr(ra, "is_interactive", lambda: True)
    monkeypatch.setattr(ra, "ask", lambda question: pytest.fail("asked in a dry run"))
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
