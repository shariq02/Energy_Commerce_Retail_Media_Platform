# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REGISTRY LIBRARY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** the registry rules, the version planner, the reproducibility
# MAGIC outcome and the registry findings text. Pulled in with
# MAGIC `%run ../lib/_registry_rules` after `_model_common` and `_approval_rules`. Plain
# MAGIC Python; only the two table readers use Spark. The findings text is committed, so it passes through
# MAGIC `scrub_private`.

# COMMAND ----------

# DBTITLE 1,Imports
import json as _json_registry
import math as _math_registry

# COMMAND ----------

# DBTITLE 1,Registry constants
LIFECYCLE = ("registered", "deprecated", "retired")
REGISTRY_ROLES = ("selected", "baseline_fallback")
CARDS_SUBDIR = "src/model_cards"
# a change in one of these fields creates a new version
LINK_FIELDS = (
    "decision",
    "restriction",
    "conditions",
    "mlflow_run_id",
    "frozen_delta_version",
    "approval_run_id",
    "card_path",
)
REPRODUCED = ("matched", "not_comparable")
CHECK_PASSED = "passed"
CHECK_FAILED = "failed"
# a check recorded per ecosystem carries the ecosystem after a colon
_OTHER_ECOSYSTEM = {"energy": ":commerce", "commerce": ":energy"}

# COMMAND ----------

# DBTITLE 1,Table readers


def registry_rows(ecosystem: str) -> list:
    rows = read_model("model_registry", ecosystem=ecosystem).collect()
    return [r.asDict() for r in rows]


def check_rows(ecosystem: str) -> list:
    rows = read_model("registry_check", ecosystem=ecosystem).collect()
    return [r.asDict() for r in rows]


# COMMAND ----------

# DBTITLE 1,Entries the registry may hold


def card_path(ecosystem: str, task_id: str) -> str:
    return f"{CARDS_SUBDIR}/{ecosystem}/{task_id}.md"


def artifact_uri(run_id):
    return f"runs:/{run_id}/candidate/candidate.pkl" if run_id else None


def _entry_fields(row, fit_versions) -> dict:
    tid = row["task_id"]
    return {
        "task_id": tid,
        "dataset_id": row["dataset_id"],
        "model_name": row["model_name"],
        "role": row["role"],
        "decision": row["decision"],
        "restriction": row["restriction"],
        "conditions": row["conditions"],
        "primary_metric": row["primary_metric"],
        "validation_value": row["validation_value"],
        "frozen_delta_version": row["frozen_delta_version"],
        "mlflow_run_id": row["mlflow_run_id"],
        "artifact_uri": artifact_uri(row["mlflow_run_id"]),
        "approval_run_id": row["run_id"],
        "approval_decided_at": row["decided_at"],
        "card_path": card_path(TASK_BY_ID[tid]["ecosystem"], tid),
        "fit_library_versions": fit_versions.get(tid),
    }


def registry_entries(approvals, fit_versions) -> list:
    """The approved selected model of each task and the baseline fallback of that
    task, as registry fields. Nothing else may be registered."""
    out = []
    for sel in approvals:
        if sel["role"] != "selected" or sel["decision"] not in APPROVED:
            continue
        if sel["decided_by"] != DECIDER_OWNER:
            continue
        fallback = [
            a
            for a in approvals
            if a["task_id"] == sel["task_id"] and a["role"] == "baseline_fallback"
        ]
        out += [_entry_fields(r, fit_versions) for r in (sel, *fallback)]
    return out


# COMMAND ----------

# DBTITLE 1,Version planning


def _entry_key(row) -> tuple:
    return (row["task_id"], row["model_name"], row["role"])


def latest_versions(rows) -> dict:
    """The highest version row of each (task, model, role)."""
    best: dict = {}
    for r in rows:
        key = _entry_key(r)
        if key not in best or r["version"] > best[key]["version"]:
            best[key] = r
    return best


def plan_versions(existing, wanted, changes, rid, now) -> list:
    """New registry rows only. A first entry is version 1. A changed link creates the
    next version with the changed fields as reason. A lifecycle change from `changes`
    ({(task, model, role): {"status", "reason"}}) creates the next version too. A
    version row is never edited."""
    latest = latest_versions(existing)
    new = []
    for w in wanted:
        key = _entry_key(w)
        cur = latest.get(key)
        if cur is None:
            new.append(
                {
                    **w,
                    "version": 1,
                    "lifecycle_status": "registered",
                    "reason": "first registration",
                }
            )
            continue
        diff = [f for f in LINK_FIELDS if cur[f] != w[f]]
        if diff:
            new.append(
                {
                    **w,
                    "version": cur["version"] + 1,
                    "lifecycle_status": cur["lifecycle_status"],
                    "reason": f"link changed: {', '.join(diff)}",
                }
            )
    pending = {**latest, **{_entry_key(r): r for r in new}}
    for key, change in sorted(changes.items()):
        cur = pending.get(key)
        if cur is None:
            raise ValueError(f"lifecycle change for an entry not registered: {key}")
        if change["status"] not in LIFECYCLE:
            raise ValueError(f"lifecycle status not valid: {key}, {change['status']}")
        reason = str(change.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"lifecycle change without a reason: {key}")
        if change["status"] == cur["lifecycle_status"]:
            continue
        row = {
            **cur,
            "version": cur["version"] + 1,
            "lifecycle_status": change["status"],
            "reason": reason,
        }
        new.append(row)
        pending[key] = row
    return [{**r, "run_id": rid, "registered_at": now} for r in new]


# COMMAND ----------

# DBTITLE 1,Environment record and reproducibility outcome


def environment_record() -> dict:
    """Processor type, Python version and library versions of this compute."""
    return {
        "processor_type": _platform.machine(),
        "python_version": _platform.python_version(),
        "library_versions": _json_registry.dumps(
            {name: library_version(name) for name in sorted(LIBRARIES)}, sort_keys=True
        ),
    }


def reproduction_outcome(recorded, rescored, tolerance, comparable=True):
    """(rescore status, relative gap) of one validation re-score against the
    recorded validation value."""
    if not comparable:
        return "not_comparable", None
    if recorded is None or not _math_registry.isfinite(recorded):
        return "no_recorded_value", None
    if rescored is None or not _math_registry.isfinite(rescored):
        return "score_failed", None
    gap = abs(rescored - recorded) / max(abs(recorded), 1e-12)
    return ("matched" if gap <= tolerance else "not_matched"), gap


def check_status(reload_status, rescore_status) -> str:
    ok = reload_status == "ok" and rescore_status in REPRODUCED
    return CHECK_PASSED if ok else CHECK_FAILED


def latest_checks(rows) -> dict:
    """The latest check row of each (task, model, role, processor type)."""
    best: dict = {}
    for r in rows:
        key = (*_entry_key(r), r["processor_type"])
        if key not in best or str(r["checked_at"]) > str(best[key]["checked_at"]):
            best[key] = r
    return best


# COMMAND ----------

# DBTITLE 1,Registry findings text


def _count_lines(rows) -> list:
    counts: dict = {}
    for r in rows:
        key = (r["role"], r["lifecycle_status"])
        counts[key] = counts.get(key, 0) + 1
    return [f"- {role}, {state}: {n}" for (role, state), n in sorted(counts.items())]


def _library_table(checks) -> str:
    """One row per library, one column per processor type, from the latest check."""
    by_type: dict = {}
    for r in sorted(checks, key=lambda r: str(r["checked_at"])):
        by_type[r["processor_type"]] = r
    types = sorted(by_type)
    versions = {
        t: _json_registry.loads(by_type[t]["library_versions"] or "{}") for t in types
    }
    names = sorted({n for v in versions.values() for n in v})
    header = ["item", *types]
    rows = [
        ("python", *[by_type[t]["python_version"] for t in types]),
        *[(n, *[versions[t].get(n) or "not installed" for t in types]) for n in names],
    ]
    return markdown_table(header, rows)


def _check_order(c) -> tuple:
    return (c["task_id"], c["role"], c["model_name"], c["processor_type"])


def render_registry_findings(eco, registry, checks, results, stamp) -> str:
    """The registry and the reproducibility record of one ecosystem as markdown."""
    latest = sorted(
        latest_versions(registry).values(),
        key=lambda r: (r["task_id"], r["role"], r["model_name"]),
    )
    current = latest_checks(checks)
    lines = [
        f"# {eco.upper()} REGISTRY FINDINGS",
        "",
        (
            "_Auto-generated by `databricks/models/register/gate/03_export_findings`. "
            "A registered version is never edited; a change creates a new version. "
            f"Generated: {stamp}_"
        ),
        "",
        "## summary",
        "",
        f"- entries (latest version of each): {len(latest)}",
        f"- version rows in total: {len(registry)}",
        *_count_lines(latest),
        f"- processor types checked: {sorted({c['processor_type'] for c in checks})}",
        "",
        "## registry (latest version of each entry)",
        "",
        markdown_table(
            [
                "task",
                "model",
                "role",
                "version",
                "lifecycle",
                "decision",
                "restriction",
                "frozen version",
                "run id",
                "card",
            ],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["role"],
                    r["version"],
                    r["lifecycle_status"],
                    r["decision"],
                    r["restriction"] or "-",
                    r["frozen_delta_version"],
                    r["mlflow_run_id"],
                    r["card_path"],
                )
                for r in latest
            ],
        ),
        "",
        "## version history (a version above 1)",
        "",
        markdown_table(
            ["task", "model", "role", "version", "lifecycle", "reason", "registered"],
            [
                (
                    r["task_id"],
                    r["model_name"],
                    r["role"],
                    r["version"],
                    r["lifecycle_status"],
                    r["reason"],
                    r["registered_at"],
                )
                for r in sorted(registry, key=lambda r: (*_entry_key(r), r["version"]))
                if r["version"] > 1
            ],
        ),
        "",
        "## reproducibility checks (latest of each entry and processor type)",
        "",
        markdown_table(
            [
                "task",
                "model",
                "role",
                "version",
                "processor",
                "reload",
                "re-score",
                "recorded",
                "re-scored",
                "gap",
                "tolerance",
                "status",
                "detail",
            ],
            [
                (
                    c["task_id"],
                    c["model_name"],
                    c["role"],
                    c["registry_version"],
                    c["processor_type"],
                    c["reload_status"],
                    c["rescore_status"],
                    _number(c["recorded_value"]),
                    _number(c["rescored_value"]),
                    _number(c["gap_relative"]),
                    _number(c["tolerance"]),
                    c["status"],
                    c["detail"] or c["reload_detail"],
                )
                for c in sorted(current.values(), key=_check_order)
            ],
        ),
        "",
        "## environment of the checks",
        "",
        _library_table(checks) if checks else "- no check recorded",
        "",
        "## check results",
        "",
        markdown_table(
            ["component", "check", "status", "detail", "recorded"],
            [
                (
                    c["component"],
                    c["metric_name"],
                    c["status"],
                    c["error_detail"],
                    c["recorded_at"],
                )
                for c in sorted(results, key=lambda c: str(c["recorded_at"]))
                if not c["metric_name"].endswith(_OTHER_ECOSYSTEM[eco])
            ],
        ),
    ]
    return scrub_private("\n".join(lines))
