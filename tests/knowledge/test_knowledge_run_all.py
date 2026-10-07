"""Knowledge run_all -- order, stop on failure and the approval boundary.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
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
    assert [name for name, _ in ra.STEPS] == ["units", "authored", "tokens"]


def only_step(monkeypatch, name, step, answers=(), interactive=False):
    """Run one step only; the prompts are answered from `answers`."""
    asked = []
    answers = list(answers)

    def fake_ask(question):
        asked.append(question)
        return answers.pop(0)

    monkeypatch.setattr(ra, "STEPS", [(name, step)])
    monkeypatch.setattr(ra, "ask", fake_ask)
    monkeypatch.setattr(ra, "is_interactive", lambda: interactive)
    return asked


def test_dry_run_lists_the_units_and_writes_nothing(monkeypatch, tmp_path, capsys):
    only_step(monkeypatch, "units", ra.step_units)
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
    assert not any(tmp_path.rglob("*"))
    output = capsys.readouterr().out
    assert "Created (92):" in output
    assert "  metric.ga4_sessions  version 1" in output
    assert "Dry run: nothing was written" in output


def test_without_an_index_the_authored_step_is_skipped(monkeypatch, tmp_path, capsys):
    only_step(monkeypatch, "authored", ra.step_authored)
    argv = ["--corpus-dir", str(tmp_path), "--authored-dir", str(tmp_path / "none")]
    assert ra.main(argv) == 0
    assert "No authored units yet" in capsys.readouterr().out


@pytest.mark.parametrize("answer", ["n", "no", "", "x", "maybe"])
def test_any_answer_but_yes_writes_nothing(monkeypatch, tmp_path, answer):
    asked = only_step(monkeypatch, "units", ra.step_units, [answer], interactive=True)
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
    assert len(asked) == 1
    assert not any(tmp_path.rglob("*"))


@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES"])
def test_yes_runs_the_steps_again_to_write_then_offers_approval(
    monkeypatch, tmp_path, answer
):
    asked = only_step(
        monkeypatch, "units", ra.step_units, [answer, "5"], interactive=True
    )
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
    assert len(asked) == 2
    assert len(uc.scan_units(tmp_path)) == 92
    assert approved_kinds(tmp_path) == {}


@pytest.mark.skipif(
    not ra.rt.DEFAULT_TOKENIZER.is_file(), reason="embedding tokenizer file not present"
)
def test_the_dry_run_checks_the_size_of_the_planned_bodies(
    monkeypatch, tmp_path, capsys
):
    only_step(monkeypatch, "tokens", ra.step_tokens)
    assert ra.main(["--corpus-dir", str(tmp_path)]) == 0
    assert not any(tmp_path.rglob("*"))
    assert "Units: 92" in capsys.readouterr().out


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
