# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REGISTER MODELS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** register the approved model of each task and the baseline fallback
# MAGIC of that task, linked to the approval row, the run, the frozen dataset version
# MAGIC and the model card. A change creates a new version row. Reads recorded results
# MAGIC only; no model is loaded, refitted or scored.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Imports
import os as _os

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/01_register_models"
SOURCE = "models"
# {(task_id, model_name, role): {"status": "deprecated" | "retired", "reason": "..."}}
LIFECYCLE_CHANGES = {}

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
print(f"OK  repository root: {REPO_ROOT}")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the decisions, the fit environment and the registry so far
evidence = {}
for eco in ("energy", "commerce"):
    approvals = [
        r.asDict() for r in read_model("model_approval", ecosystem=eco).collect()
    ]
    fit_rows = (
        read_model("task_run_context", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "finished"))
        .select("task_id", "library_versions", "recorded_at")
        .collect()
    )
    fit_versions = {}
    for r in sorted(fit_rows, key=lambda r: str(r["recorded_at"])):
        fit_versions[r["task_id"]] = r["library_versions"]
    existing = registry_rows(eco)
    evidence[eco] = (approvals, fit_versions, existing)
    print(f"{eco}: {len(approvals)} decision row(s), {len(existing)} registry row(s)")

# COMMAND ----------

# DBTITLE 1,Plan the new version rows
now = now_utc()
planned = {}
for eco, (approvals, fit_versions, existing) in evidence.items():
    wanted = registry_entries(approvals, fit_versions)
    changes = {
        k: v
        for k, v in LIFECYCLE_CHANGES.items()
        if TASK_BY_ID[k[0]]["ecosystem"] == eco
    }
    planned[eco] = plan_versions(existing, wanted, changes, rid, now)
    print(f"{eco}: {len(wanted)} entry(ies) allowed, {len(planned[eco])} new row(s)")

# COMMAND ----------

# DBTITLE 1,New version rows
for eco, rows in planned.items():
    for r in rows:
        print(
            eco,
            r["task_id"],
            r["model_name"],
            r["role"],
            f"v{r['version']}",
            r["lifecycle_status"],
            r["reason"],
        )

# COMMAND ----------

# DBTITLE 1,Write the new version rows
for eco, rows in planned.items():
    if not rows:
        print(f"OK  {eco}: registry unchanged")
        continue
    cols = read_model("model_registry", ecosystem=eco).columns
    tuples = [tuple(r[c] for c in cols) for r in rows]
    (
        spark.createDataFrame(tuples, MODEL_DDL["model_registry"])
        .write.format("delta")
        .mode("append")
        .saveAsTable(model_fqn("model_registry", eco))
    )
    print(f"OK  {eco}: {len(rows)} row(s) appended")

# COMMAND ----------

# DBTITLE 1,Every entry has an artifact and a card
_unlinked = []
for eco in evidence:
    for key, r in latest_versions(registry_rows(eco)).items():
        card = _os.path.join(REPO_ROOT, r["card_path"])
        if not r["mlflow_run_id"] or not _os.path.isfile(card):
            _unlinked.append(key)
check(
    COMPONENT,
    SOURCE,
    "every_entry_has_a_run_and_a_card",
    not _unlinked,
    detail=f"entries without a run or a card: {_unlinked}",
    metric_value=float(len(_unlinked)),
    rid=rid,
)
