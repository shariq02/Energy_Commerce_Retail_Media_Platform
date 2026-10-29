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
import hashlib as _hashlib_registry
import json as _json_registry
import math as _math_registry

# COMMAND ----------

# DBTITLE 1,Registry constants
LIFECYCLE = ("registered", "deprecated", "retired")
# Recorded for each new version: the code of the training run (a commit), the data it
# read, the model file and the task definition. Versions made before these columns
# existed hold NULL in all of them and are reported as not recorded.
VERSION_FIELDS = (
    "code_version",
    "data_fingerprint",
    "artifact_sha256",
    "task_spec_hash",
)
REQUIRED_VERSION_FIELDS = ("data_fingerprint", "artifact_sha256", "task_spec_hash")
COMMIT_TAGS = ("mlflow.source.git.commit", "mlflow.databricks.gitRepoCommit")
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


def _entry_fields(row, fit_versions, versions=None) -> dict:
    tid = row["task_id"]
    recorded = (versions or {}).get((tid, row["model_name"], row["role"]), {})
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
        **{field: recorded.get(field) for field in VERSION_FIELDS},
        "replaces": None,
    }


def registry_entries(approvals, fit_versions, versions=None) -> list:
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
        out += [_entry_fields(r, fit_versions, versions) for r in (sel, *fallback)]
    return out


# COMMAND ----------

# DBTITLE 1,A foundation model that becomes the selected model of a task


def annotate_replacements(wanted, existing, is_foundation) -> list:
    """Give a first registration of a foundation model as the selected model the
    reason that it replaces the model registered before; `plan_versions` retires
    the earlier entry."""
    latest = latest_versions(existing)
    out = []
    for w in wanted:
        if (
            w["role"] == "selected"
            and is_foundation(w["model_name"])
            and _entry_key(w) not in latest
        ):
            earlier = sorted(
                key[1]
                for key, row in latest.items()
                if key[0] == w["task_id"]
                and key[2] == "selected"
                and key[1] != w["model_name"]
                and row["lifecycle_status"] != "retired"
            )
            if earlier:
                reason = (
                    f"replaces {', '.join(earlier)} as the selected model: first on "
                    "the validation rank and approved (rule R01)"
                )
                w = {**w, "reason": reason}
        out.append(w)
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


def _replaced_entries(latest, wanted) -> list:
    """A retired version of each entry whose (task, role) the approval gives to
    another model. Only the registry roles are replaced."""
    taken = {
        (w["task_id"], w["role"]): w for w in wanted if w["role"] in REGISTRY_ROLES
    }
    out = []
    for key, cur in sorted(latest.items()):
        task_id, model_name, role = key
        new_owner = taken.get((task_id, role))
        if (
            new_owner is None
            or new_owner["model_name"] == model_name
            or cur["lifecycle_status"] == "retired"
        ):
            continue
        out.append(
            {
                **cur,
                "version": cur["version"] + 1,
                "lifecycle_status": "retired",
                "reason": (
                    f"replaced by {new_owner['model_name']} "
                    f"(approval run {new_owner['approval_run_id']})"
                ),
            }
        )
    return out


def _replaced_links(replaced_rows) -> dict:
    """{(task, role): ["model@vN", ...]}: what the retired rows of a plan replace."""
    links: dict = {}
    for row in replaced_rows:
        key = (row["task_id"], row["role"])
        links.setdefault(key, []).append(f"{row['model_name']}@v{row['version'] - 1}")
    return {key: sorted(found) for key, found in links.items()}


def plan_versions(existing, wanted, changes, rid, now) -> list:
    """New registry rows only. A first entry is version 1. A changed link creates the
    next version with the changed fields as reason. A lifecycle change from `changes`
    ({(task, model, role): {"status", "reason"}}) creates the next version too. A
    version row is never edited."""
    latest = latest_versions(existing)
    replaced = _replaced_entries(latest, wanted)
    links = _replaced_links(replaced)
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
                    "reason": w.get("reason") or "first registration",
                    "replaces": ", ".join(links.get((w["task_id"], w["role"]), []))
                    or None,
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
                    "replaces": cur.get("replaces"),
                }
            )
    new += replaced
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

# DBTITLE 1,The version fields of a registered model


def task_spec_hash(task_id: str) -> str:
    """SHA-256 of the task definition (dataset, target, kind, baseline, metric)."""
    text = _json_registry.dumps(TASK_BY_ID[task_id], sort_keys=True, default=str)
    return _hashlib_registry.sha256(text.encode("utf-8")).hexdigest()


def data_fingerprint(task_id: str, version: int) -> str:
    """A fingerprint of the frozen dataset at its Delta version: the row count, the
    schema, and the sum of a hash of the partition and the target of every row. It
    reads two columns. A changed or truncated dataset gives another value."""
    task = TASK_BY_ID[task_id]
    full = ml_fqn(f"dataset_{task['dataset_id']}", task["ecosystem"])
    df = spark.sql(f"SELECT * FROM {full} VERSION AS OF {int(version)}")
    columns = [F.col("partition")]
    if task["target"] in df.columns:
        columns.append(F.col(task["target"]))
    hashed = F.pmod(F.xxhash64(*columns), F.lit(2147483647))
    row = df.agg(F.count(F.lit(1)).alias("n"), F.sum(hashed).alias("h")).first()
    schema = _hashlib_registry.sha256(df.schema.json().encode("utf-8")).hexdigest()
    text = f"{row['n']}|{row['h']}|{schema}"
    return _hashlib_registry.sha256(text.encode("utf-8")).hexdigest()[:32]


def artifact_digest(run_id: str) -> str:
    """SHA-256 of the stored model file of a run, read in blocks."""
    import tempfile

    import mlflow

    with tempfile.TemporaryDirectory() as folder:
        path = mlflow.artifacts.download_artifacts(
            artifact_uri=artifact_uri(run_id), dst_path=folder
        )
        digest = _hashlib_registry.sha256()
        with open(path, "rb") as handle:
            while block := handle.read(8 * 1024 * 1024):
                digest.update(block)
        return digest.hexdigest()


def training_commit(run_id: str):
    """The git commit that MLflow recorded for the training run, or None."""
    import mlflow

    tags = mlflow.tracking.MlflowClient().get_run(run_id).data.tags
    return next((tags[t] for t in COMMIT_TAGS if tags.get(t)), None)


def version_fields_of(entry: dict) -> dict:
    """The four version fields of a new entry; each step prints its time."""
    import time as _time_registry

    out = {}
    for name, step in (
        ("task_spec_hash", lambda: task_spec_hash(entry["task_id"])),
        (
            "data_fingerprint",
            lambda: data_fingerprint(entry["task_id"], entry["frozen_delta_version"]),
        ),
        ("artifact_sha256", lambda: artifact_digest(entry["mlflow_run_id"])),
        ("code_version", lambda: training_commit(entry["mlflow_run_id"])),
    ):
        started = _time_registry.monotonic()
        out[name] = step()
        print(
            f"  {entry['task_id']} {entry['model_name']} {name}: "
            f"{_time_registry.monotonic() - started:.1f} s"
        )
    return out


def version_field_problems(rows) -> list:
    """(task, model, role, version, what) for a row that holds some of the required
    version fields but not all of them. A row with none is older than the fields."""
    problems = []
    for r in rows:
        held = [f for f in REQUIRED_VERSION_FIELDS if r.get(f)]
        if held and len(held) != len(REQUIRED_VERSION_FIELDS):
            lacking = [f for f in REQUIRED_VERSION_FIELDS if not r.get(f)]
            problems.append(
                (r["task_id"], r["model_name"], r["role"], r["version"], lacking)
            )
    return problems


def versions_not_recorded(rows) -> int:
    """How many version rows hold none of the required version fields."""
    return sum(1 for r in rows if not any(r.get(f) for f in REQUIRED_VERSION_FIELDS))


def version_field_mismatches(rows) -> list:
    """Recompute the fields of each row that holds them and list the differences.
    The artifact is read again, so only rows with a recorded hash cost time."""
    out = []
    for r in rows:
        if not r.get("data_fingerprint"):
            continue
        if r["task_spec_hash"] != task_spec_hash(r["task_id"]):
            out.append((r["task_id"], r["model_name"], r["version"], "task_spec_hash"))
        if r["data_fingerprint"] != data_fingerprint(
            r["task_id"], r["frozen_delta_version"]
        ):
            out.append(
                (r["task_id"], r["model_name"], r["version"], "data_fingerprint")
            )
        if r["artifact_sha256"] != artifact_digest(r["mlflow_run_id"]):
            out.append((r["task_id"], r["model_name"], r["version"], "artifact_sha256"))
    return out


# COMMAND ----------

# DBTITLE 1,Check of the planned registry


def planned_registry_problems(existing, planned, approvals) -> list:
    """What the registry guards would find after the planned rows are written:
    (task, what, detail) for every approved task without exactly one entry in each
    registry role, every registered entry that differs from its approval row and
    every registered entry of a task that is not approved."""
    latest = latest_versions([*existing, *planned])
    approved = {
        a["task_id"]
        for a in approvals
        if a["role"] == "selected" and a["decision"] in APPROVED
    }
    wanted = {_entry_key(w): w for w in registry_entries(approvals, {})}
    counts: dict = {}
    for r in latest.values():
        if r["lifecycle_status"] != "retired":
            key = (r["task_id"], r["role"])
            counts[key] = counts.get(key, 0) + 1
    out = []
    for task_id in sorted(approved):
        for role in REGISTRY_ROLES:
            n = counts.get((task_id, role), 0)
            if n != 1:
                out.append((task_id, f"{n} entries in role {role}", ""))
    for key, r in sorted(latest.items()):
        if r["lifecycle_status"] != "registered":
            continue
        if r["task_id"] not in approved:
            out.append((r["task_id"], "registered but not approved", r["model_name"]))
            continue
        w = wanted.get(key)
        diff = [f for f in LINK_FIELDS if w is None or r[f] != w[f]]
        if diff:
            out.append((r["task_id"], f"differs from the approval: {diff}", key[1]))
    return out


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
        (
            "- version rows without recorded code, data and file versions (made "
            f"before the columns existed): {versions_not_recorded(registry)}"
        ),
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
