"""The write gate: which tool calls count as a write, and which words count as approval.

ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform
Author: Sharique Mohammad
Date: October 2026

The hook file lives in `.claude/`, which is not in git. The tests skip when the file is
missing, so CI passes without it. They call the functions of the hook and write
nothing except two small files in a temporary folder that the test removes.
"""

from __future__ import annotations

import importlib.util
import shutil
import tempfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

ROOT = Path(__file__).resolve().parents[2]
HOOK = ROOT / ".claude" / "hooks" / "approval_gate.py"

if not HOOK.exists():
    pytest.skip("the hook file is not in this checkout", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("approval_gate", HOOK)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)


@pytest.fixture(scope="module")
def scratch():
    folder = Path(tempfile.mkdtemp(prefix="claude-", dir="/tmp"))
    (folder / "reads.py").write_text("import sys\nprint(sys.argv)\n", encoding="utf-8")
    (folder / "writes.py").write_text(
        "import json\njson.dump({}, open('t.json', 'w'))\n", encoding="utf-8"
    )
    yield str(folder)
    shutil.rmtree(folder, ignore_errors=True)


def reason(command: str) -> str | None:
    return gate.bash_reason(command, str(ROOT))


READS = [
    "ls -la ai",
    "cat file | head -5",
    'grep -rn "x > y" ai | cut -c1-100',
    "sed -n '1,20p' file",
    "python3 - <<'EOF'\nimport ast\nprint(1>0)\nEOF",
    "git status --short | head",
    "git diff --stat | tail -3",
    "git log --oneline -3",
    "python3 -m ai.mcp_servers.contracts",
    "awk 'length($0)>88{print FILENAME}' a.py",
    "wc -l file; echo done",
    "for f in a b; do echo $f; done",
    "python3 -c \"import ast;ast.parse(open('x').read());print('ok')\"",
    "python3 -c 'import sys; print(1>0)'",
    "pip install --dry-run --break-system-packages x",
    "python3 -m pip show mcp",
    "ls docs 2>&1 | head",
    "python3 q.py > /dev/null 2>&1",
    "python3 - <<'E'\nprint(open('f').read())\nE",
    "python3 - <<'E'\nopen('f', 'r')\nE",
    "python3 - <<'E'\nwith open('f', encoding='utf-8') as h: pass\nE",
]
WRITES = [
    "echo hi > out.txt",
    "echo hi>>out.txt",
    "cat > x.py <<'EOF'\nprint(1)\nEOF",
    "python3 - <<'EOF'\nopen('a','w').write('x')\nEOF",
    "python3 -c \"open('a','w').write('x')\"",
    "python3 - <<'E'\nopen('p','wb').write(b'x')\nE",
    "python3 - <<'E'\nopen('f', mode='w')\nE",
    "python3 - <<'E'\nfrom pathlib import Path\nPath('a').open('w')\nE",
    "python3 - <<'EOF'\nfrom pathlib import Path\nPath('a').write_text('x')\nEOF",
    "python3 - <<'EOF'\nimport shutil\nshutil.copy('a','b')\nEOF",
    "sed -i 's/a/b/' file",
    "perl -pi -e 's/a/b/' f",
    "rm -rf build",
    "mv a b",
    "cp a b",
    "mkdir x",
    "touch x",
    "git add -A",
    "git commit -m x",
    "git checkout main",
    "git fetch -q",
    "git -C . push",
    "pip install foo",
    "curl -o f http://x",
    "touch /tmp/ecrmap_write_ok_x",
    "python3 -m ai.mcp_servers.contracts_write",
    "python3 scripts/utilities/make_tool_tokens.py",
    "ls | xargs rm",
    'bash -c "rm x"',
    "echo x | tee out.txt",
    "echo x > /tmp/claude-1/../../home/x",
    "for f in a; do rm $f; done",
    "terraform apply",
    "gh pr create",
]


@pytest.mark.parametrize("command", READS)
def test_a_command_that_only_reads_is_allowed(command):
    assert reason(command) is None


@pytest.mark.parametrize("command", WRITES)
def test_a_command_that_writes_is_denied(command):
    assert reason(command)


def test_the_scratchpad_may_be_written(scratch):
    assert reason(f"cat > {scratch}/a.py <<'EOF'\nprint(1)\nEOF") is None
    assert reason(f"cp docs/x {scratch}/backup.md") is None
    assert reason(f"echo x | tee {scratch}/o.txt") is None
    assert reason(f"touch {scratch}/f") is None


def test_a_script_in_the_scratchpad_is_read_before_it_runs(scratch):
    assert reason(f"cd {scratch} && python3 reads.py x") is None
    assert reason(f"cd {scratch} && python3 writes.py") is None
    assert reason(f"python3 {scratch}/writes.py")
    open_outside = f"cd {scratch} && python3 - <<'E'\nopen('/mnt/c/x','w')\nE"
    assert reason(open_outside)


@pytest.mark.parametrize(
    "tool, tool_input, writes",
    [
        ("Write", {"file_path": "/mnt/c/proj/x.py"}, True),
        ("Edit", {"file_path": "a.py"}, True),
        ("NotebookEdit", {"notebook_path": "a.ipynb"}, True),
        ("Write", {"file_path": "/tmp/claude-1/../etc/x"}, True),
        ("Artifact", {"action": "publish"}, True),
        ("Artifact", {}, True),
        ("Artifact", {"action": "delete"}, True),
        ("Artifact", {"action": "read"}, False),
        ("Artifact", {"action": "list"}, False),
        ("ArtifactData", {"action": "set"}, True),
        ("ArtifactData", {"action": "get"}, False),
        ("mcp__claude_ai_Claude_Docs__create", {}, True),
        ("mcp__claude_ai_Claude_Docs__read", {}, False),
        ("CronCreate", {}, True),
        ("Read", {"file_path": "a"}, False),
    ],
)
def test_the_tools_that_write(tool, tool_input, writes):
    assert bool(gate.write_reason(tool, tool_input, str(ROOT))) is writes


def test_a_write_to_the_scratchpad_is_allowed(scratch):
    assert gate.write_reason("Write", {"file_path": f"{scratch}/x.py"}) is None


@pytest.mark.parametrize(
    "prompt, approved",
    [
        ("update the rule and the hook", True),
        ("implement", True),
        ("lint ok, now implement the first part", True),
        ("sign off", True),
        (
            '<pasted_content id="1">please update the stage</pasted_content id="1"> ok',
            False,
        ),
        ("```\nupdate\n```", False),
        ("I updated it", False),
        ("unimplemented", False),
        ("what is next?", False),
        ("it is done, thanks", False),
    ],
)
def test_only_a_typed_keyword_counts_as_approval(prompt, approved):
    assert gate.keyword_in(prompt) is approved
