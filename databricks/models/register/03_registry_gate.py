# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # REGISTRY GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** check the registry record and the reproducibility checks, then write
# MAGIC both of each ecosystem to the repository as markdown. Hard failures: an approved
# MAGIC task without its registered model and fallback, an entry for a task that is not
# MAGIC approved, an entry that differs from its approval row, an entry without a run, at
# MAGIC another frozen version or without a card, version numbers with a gap, a row that
# MAGIC holds only some of the code, data, file and task versions, a recorded version
# MAGIC that no longer matches, a registered entry without a check of its current
# MAGIC version, a failed check, and a check row without its environment.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/03_registry_gate"
FINDINGS_DIR = _findings_dir()
_os.makedirs(FINDINGS_DIR, exist_ok=True)
# the findings folder is src/findings/model_findings: three levels below the root
REPO_ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(FINDINGS_DIR)))

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry, the decisions and the checks
record = {}
for eco in ECOSYSTEMS:
    registry = registry_rows(eco)
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    checks = check_rows(eco)
    record[eco] = (registry, approvals, checks)
    print(f"{eco}: {len(registry)} registry row(s), {len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Every approved task is registered once with its fallback, and only those
_wrong, _extra = [], []
for registry, approvals, _checks in record.values():
    counts = {}
    for r in latest_versions(registry).values():
        if r["lifecycle_status"] != "retired":
            key = (r["task_id"], r["role"])
            counts[key] = counts.get(key, 0) + 1
    approved = {
        a["task_id"]
        for a in approvals
        if a["role"] == "selected" and a["decision"] in APPROVED
    }
    for task_id in approved:
        for role in REGISTRY_ROLES:
            n = counts.get((task_id, role))
            if n != 1:
                _wrong.append((task_id, role, n))
    for r in latest_versions(registry).values():
        if r["lifecycle_status"] == "registered" and r["task_id"] not in approved:
            _extra.append((r["task_id"], r["model_name"], r["role"]))
guard(
    COMPONENT,
    rid,
    "every_approved_task_registered_once_with_fallback",
    _wrong,
    "task, role, entries",
)
guard(
    COMPONENT,
    rid,
    "no_registered_entry_for_an_unapproved_task",
    _extra,
    "registered although not approved",
)

# COMMAND ----------

# DBTITLE 1,Each registered entry equals its approval row
_differs = []
for registry, approvals, _checks in record.values():
    wanted = {_entry_key(w): w for w in registry_entries(approvals, {})}
    for key, r in latest_versions(registry).items():
        w = wanted.get(key)
        if r["lifecycle_status"] != "registered":
            continue
        diff = [f for f in LINK_FIELDS if w is None or r[f] != w[f]]
        if diff:
            _differs.append((*key, diff))
guard(
    COMPONENT,
    rid,
    "registered_entry_equals_its_approval_row",
    _differs,
    "registry behind the approval record",
)

# COMMAND ----------

# DBTITLE 1,Each entry has a run, an artifact address, the frozen version and a card
_latest = [
    r
    for registry, _a, _c in record.values()
    for r in latest_versions(registry).values()
]
guard(
    COMPONENT,
    rid,
    "every_entry_has_a_run_and_an_artifact",
    [
        (r["task_id"], r["model_name"])
        for r in _latest
        if not r["mlflow_run_id"] or not r["artifact_uri"]
    ],
    "entries without a run or an artifact",
)
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
guard(
    COMPONENT,
    rid,
    "entry_at_the_frozen_dataset_version",
    [
        (
            r["task_id"],
            r["model_name"],
            r["frozen_delta_version"],
            _frozen[r["task_id"]],
        )
        for r in _latest
        if r["lifecycle_status"] == "registered"
        and r["frozen_delta_version"] != _frozen[r["task_id"]]
    ],
    "task, model, entry version, frozen version",
)
guard(
    COMPONENT,
    rid,
    "every_entry_has_a_card",
    [
        (r["task_id"], r["model_name"])
        for r in _latest
        if not _os.path.isfile(_os.path.join(REPO_ROOT, r["card_path"]))
    ],
    "entries without a card file",
)

# COMMAND ----------

# DBTITLE 1,Version numbers and version fields
_gaps = []
for registry, _a, _c in record.values():
    seen = {}
    for r in registry:
        seen.setdefault(_entry_key(r), []).append(r["version"])
    for key, versions in seen.items():
        if sorted(versions) != list(range(1, len(versions) + 1)):
            _gaps.append((*key, sorted(versions)))
guard(COMPONENT, rid, "version_numbers_have_no_gap_or_repeat", _gaps, "entry, versions")
print(
    "version rows without recorded versions: "
    f"{sum(versions_not_recorded(registry) for registry, _a, _c in record.values())}"
)
guard(
    COMPONENT,
    rid,
    "version_fields_are_complete_or_absent",
    [
        problem
        for registry, _a, _c in record.values()
        for problem in version_field_problems(registry)
    ],
    "task, model, role, version, lacking",
)
_changed = []
for registry, _a, _c in record.values():
    live = [
        r
        for r in latest_versions(registry).values()
        if r["lifecycle_status"] != "retired"
    ]
    _changed += version_field_mismatches(live)
guard(
    COMPONENT,
    rid,
    "recorded_version_fields_match",
    _changed,
    "task, model, version, field",
)

# COMMAND ----------

# DBTITLE 1,Every registered entry has a passed check of its current version
_unchecked, _failed, _bare = [], [], []
for registry, _a, checks in record.values():
    done = {(*_entry_key(c), c["registry_version"]) for c in checks}
    current = {k: r["version"] for k, r in latest_versions(registry).items()}
    for key, r in latest_versions(registry).items():
        if r["lifecycle_status"] != "retired" and (*key, r["version"]) not in done:
            _unchecked.append((*key, r["version"]))
    for c in latest_checks(checks).values():
        is_current = c["registry_version"] == current.get(_entry_key(c))
        if is_current and c["status"] != CHECK_PASSED:
            _failed.append(
                (*_entry_key(c), c["processor_type"], c["rescore_status"], c["detail"])
            )
    _bare += [
        (*_entry_key(c), c["processor_type"])
        for c in checks
        if not c["processor_type"]
        or not c["python_version"]
        or not c["library_versions"]
    ]
guard(COMPONENT, rid, "every_registered_entry_checked", _unchecked, "entry, version")
guard(
    COMPONENT,
    rid,
    "every_reproducibility_check_passed",
    _failed,
    "entry, processor, re-score, detail",
)
guard(
    COMPONENT,
    rid,
    "every_check_records_its_environment",
    _bare,
    "check rows without processor type, Python version or libraries",
)
for eco, (_r, _a, checks) in record.items():
    print(
        eco, "processor types checked:", sorted({c["processor_type"] for c in checks})
    )

# COMMAND ----------

# DBTITLE 1,Write the registry findings
results = [
    r.asDict()
    for r in spark.table(AUDIT_TABLE)
    .filter((F.col("stage") == ML_STAGE) & F.col("component").like("models/register/%"))
    .select("component", "metric_name", "status", "error_detail", "recorded_at")
    .collect()
]
stamp = now_utc().strftime("%Y-%m-%dT%H:%MZ")
for eco in ECOSYSTEMS:
    registry, _approvals, checks = record[eco]
    text = render_registry_findings(eco, registry, checks, results, stamp)
    with open(
        _os.path.join(FINDINGS_DIR, f"{eco}_registry.md"), "w", encoding="utf-8"
    ) as fh:
        fh.write(text + "\n")
    print(f"OK  {eco}_registry.md")

# COMMAND ----------

# DBTITLE 1,Merge the findings of the notebooks
merge_notebook_findings()
