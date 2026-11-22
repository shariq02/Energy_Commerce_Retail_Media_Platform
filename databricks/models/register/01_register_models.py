# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # REGISTER MODELS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** register the approved model of each task and the baseline fallback
# MAGIC of that task, linked to the approval row, the run, the frozen dataset version
# MAGIC and the model card. A change creates a new version row. Reads recorded results
# MAGIC only; no model is loaded, refitted or scored.

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
COMPONENT = "models/register/01_register_models"
# {(task_id, model_name, role): {"status": "deprecated" | "retired", "reason": "..."}}
LIFECYCLE_CHANGES = {}
# the findings folder is src/findings/model_findings: three levels below the root
REPO_ROOT = _os.path.dirname(_os.path.dirname(_os.path.dirname(_findings_dir())))
print(f"OK  repository root: {REPO_ROOT}")

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the decisions, the fit environment and the registry so far
evidence = {}
for eco in ECOSYSTEMS:
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

# DBTITLE 1,The version fields of the new entries
# A new registered version records its code, data, file and task definition. Old
# versions keep NULL (not recorded); a retired or deprecated row keeps what it had.
for eco, rows in planned.items():
    for r in rows:
        if r["lifecycle_status"] == "registered" and not r.get("data_fingerprint"):
            r.update(version_fields_of(r))

# COMMAND ----------

# DBTITLE 1,The planned registry passes the guards
_problems = [
    (eco, *problem)
    for eco, (approvals, _fit, existing) in evidence.items()
    for problem in planned_registry_problems(existing, planned[eco], approvals)
]
guard(
    COMPONENT,
    rid,
    "planned_registry_is_consistent",
    _problems,
    "ecosystem, task, problem, model",
)

# COMMAND ----------

# DBTITLE 1,Write the new version rows
for eco, rows in planned.items():
    if not rows:
        print(f"OK  {eco}: registry unchanged")
        continue
    cols = row_columns("model_registry", ecosystem=eco)
    tuples = [tuple(r.get(c) for c in cols) for r in rows]
    append_model_rows(
        spark.createDataFrame(tuples, MODEL_DDL["model_registry"]),
        "model_registry",
        ecosystem=eco,
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
guard(
    COMPONENT,
    rid,
    "every_entry_has_a_run_and_a_card",
    _unlinked,
    "entries without a run or a card",
)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "register__01_register_models",
    "Register Models",
    ["model_registry"],
    rid,
)
