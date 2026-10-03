# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # APPROVAL RECOMMENDATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** read the recorded evaluation results and write one recommendation
# MAGIC per forwarded model of every task, with the rules behind it. Reads recorded
# MAGIC results only; no model is loaded, refitted or scored.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/approve/01_recommend"
SOURCE = "models"
RESULT_FIELDS = (
    "task_id",
    "model_name",
    "stage",
    "status",
    "detail",
    "metric",
    "value",
    "validation_value",
    "n_rows",
    "frozen_delta_version",
    "mlflow_run_id",
)
SELECTION_FIELDS = (
    "task_id",
    "model_name",
    "rank",
    "is_baseline",
    "mlflow_run_id",
    "frozen_delta_version",
)

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the evidence of each ecosystem
evidence = {}
for eco in ("energy", "commerce"):
    spec = spec_from_rows(read_model("approval_spec", ecosystem=eco).collect())
    results = [
        r.asDict()
        for r in read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke"))
        .select(*RESULT_FIELDS)
        .collect()
    ]
    selection = [
        r.asDict()
        for r in read_model("candidate_selection", ecosystem=eco)
        .select(*SELECTION_FIELDS)
        .collect()
    ]
    context = (
        read_model(APPROVAL_CONTEXT_TABLE, ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("status") == "data_ready"))
        .select("task_id", "run_id")
        .distinct()
        .collect()
    )
    reads = {}
    for c in context:
        reads[c["task_id"]] = reads.get(c["task_id"], 0) + 1
    evidence[eco] = (spec, results, selection, reads)
    print(f"{eco}: {len(results)} result row(s), {len(selection)} selection row(s)")

# COMMAND ----------

# DBTITLE 1,Recommend for every task
recommended = {eco: [] for eco in evidence}
for task in TASKS:
    eco = task["ecosystem"]
    spec, results, selection, reads = evidence[eco]
    tid = task["task_id"]
    recommended[eco] += recommend_task(
        task,
        [s for s in selection if s["task_id"] == tid],
        [r for r in results if r["task_id"] == tid],
        spec,
        reads.get(tid, 0),
    )
print({eco: len(rows) for eco, rows in recommended.items()})

# COMMAND ----------

# DBTITLE 1,Selected models by decision
for eco, rows in recommended.items():
    counts = {}
    for r in rows:
        if r["role"] == "selected":
            key = (r["decision"], r["restriction"] or "-")
            counts[key] = counts.get(key, 0) + 1
    print(eco, dict(sorted(counts.items())))

# COMMAND ----------

# DBTITLE 1,Write the recommendations
now = now_utc()
for eco, rows in recommended.items():
    tuples = approval_tuples(rows, rid, now)
    replace_model_rows(
        spark.createDataFrame(tuples, MODEL_DDL["model_approval"]),
        "model_approval",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )

# COMMAND ----------

# DBTITLE 1,Every task has a selected model and a baseline fallback
_missing = []
for eco, rows in recommended.items():
    for t in (t for t in TASKS if t["ecosystem"] == eco):
        roles = {r["role"] for r in rows if r["task_id"] == t["task_id"]}
        if not {"selected", "baseline_fallback"} <= roles:
            _missing.append(t["task_id"])
check(
    COMPONENT,
    SOURCE,
    "every_task_recommended",
    not _missing,
    detail=f"no recommendation: {_missing}",
    metric_value=float(len(_missing)),
    rid=rid,
)
