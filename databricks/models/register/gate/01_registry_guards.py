# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REGISTRY GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** check the registry record. Hard failures: an approved task without its
# MAGIC registered model and fallback, an entry for a task that is not approved,
# MAGIC an entry that differs from its approval row, an entry without a run, at
# MAGIC another frozen version or without a card, and version numbers with a gap.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/gate/01_registry_guards"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Repo-root discovery


def _repo_root():
    p = _os.path.abspath(_os.getcwd())
    for _ in range(12):
        if _os.path.isdir(_os.path.join(p, "src", "schemas")) and _os.path.isdir(
            _os.path.join(p, "databricks")
        ):
            return p
        if _os.path.dirname(p) == p:
            break
        p = _os.path.dirname(p)
    try:
        wp = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    except Exception as exc:
        raise RuntimeError(
            "repo root not found -- run from inside the repo's Databricks Git folder"
        ) from exc
    i = wp.rfind("/databricks/")
    if i > 0:
        for cand in (wp[:i], "/Workspace" + wp[:i]):
            if _os.path.isdir(_os.path.join(cand, "src", "schemas")):
                return cand
    raise RuntimeError(
        "repo root not found -- run from inside the repo's Databricks Git folder"
    )


REPO_ROOT = _repo_root()

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry and the decisions
record = {}
for eco in ("energy", "commerce"):
    registry = registry_rows(eco)
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    record[eco] = (registry, approvals)
    print(f"{eco}: {len(registry)} registry row(s), {len(approvals)} decision row(s)")

# COMMAND ----------

# DBTITLE 1,Every approved task is registered once with its fallback
_wrong = []
for eco, (registry, approvals) in record.items():
    counts = {}
    for r in latest_versions(registry).values():
        if r["lifecycle_status"] != "retired":
            key = (r["task_id"], r["role"])
            counts[key] = counts.get(key, 0) + 1
    for a in approvals:
        if a["role"] == "selected" and a["decision"] in APPROVED:
            for role in REGISTRY_ROLES:
                n = counts.get((a["task_id"], role))
                if n != 1:
                    _wrong.append((a["task_id"], role, n))
check(
    COMPONENT,
    SOURCE,
    "every_approved_task_registered_once_with_fallback",
    not _wrong,
    detail=f"task, role, entries: {_wrong}",
    metric_value=float(len(_wrong)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,No registered entry for a task that is not approved
_extra = []
for registry, approvals in record.values():
    approved = {
        a["task_id"]
        for a in approvals
        if a["role"] == "selected" and a["decision"] in APPROVED
    }
    for r in latest_versions(registry).values():
        if r["lifecycle_status"] == "registered" and r["task_id"] not in approved:
            _extra.append((r["task_id"], r["model_name"], r["role"]))
check(
    COMPONENT,
    SOURCE,
    "no_registered_entry_for_an_unapproved_task",
    not _extra,
    detail=f"registered although not approved: {_extra}",
    metric_value=float(len(_extra)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Each registered entry equals its approval row
_differs = []
for registry, approvals in record.values():
    wanted = {_entry_key(w): w for w in registry_entries(approvals, {})}
    for key, r in latest_versions(registry).items():
        w = wanted.get(key)
        if r["lifecycle_status"] != "registered":
            continue
        diff = [f for f in LINK_FIELDS if w is None or r[f] != w[f]]
        if diff:
            _differs.append((*key, diff))
check(
    COMPONENT,
    SOURCE,
    "registered_entry_equals_its_approval_row",
    not _differs,
    detail=f"registry behind the approval record: {_differs}",
    metric_value=float(len(_differs)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Each entry has a run and an artifact address
_no_run = [
    (r["task_id"], r["model_name"])
    for registry, _a in record.values()
    for r in latest_versions(registry).values()
    if not r["mlflow_run_id"] or not r["artifact_uri"]
]
check(
    COMPONENT,
    SOURCE,
    "every_entry_has_a_run_and_an_artifact",
    not _no_run,
    detail=f"entries without a run or an artifact: {_no_run}",
    metric_value=float(len(_no_run)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Each entry is at the frozen dataset version
_frozen = {t["task_id"]: frozen_version(t["dataset_id"], t["ecosystem"]) for t in TASKS}
_moved = [
    (r["task_id"], r["model_name"], r["frozen_delta_version"], _frozen[r["task_id"]])
    for registry, _a in record.values()
    for r in latest_versions(registry).values()
    if r["lifecycle_status"] == "registered"
    and r["frozen_delta_version"] != _frozen[r["task_id"]]
]
check(
    COMPONENT,
    SOURCE,
    "entry_at_the_frozen_dataset_version",
    not _moved,
    detail=f"task, model, entry version, frozen version: {_moved}",
    metric_value=float(len(_moved)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Each entry has a card
_no_card = [
    (r["task_id"], r["model_name"])
    for registry, _a in record.values()
    for r in latest_versions(registry).values()
    if not _os.path.isfile(_os.path.join(REPO_ROOT, r["card_path"]))
]
check(
    COMPONENT,
    SOURCE,
    "every_entry_has_a_card",
    not _no_card,
    detail=f"entries without a card file: {_no_card}",
    metric_value=float(len(_no_card)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Version numbers run from 1 without a gap
_gaps = []
for registry, _a in record.values():
    seen = {}
    for r in registry:
        seen.setdefault(_entry_key(r), []).append(r["version"])
    for key, versions in seen.items():
        if sorted(versions) != list(range(1, len(versions) + 1)):
            _gaps.append((*key, sorted(versions)))
check(
    COMPONENT,
    SOURCE,
    "version_numbers_have_no_gap_or_repeat",
    not _gaps,
    detail=f"entry, versions: {_gaps}",
    metric_value=float(len(_gaps)),
    rid=rid,
)
