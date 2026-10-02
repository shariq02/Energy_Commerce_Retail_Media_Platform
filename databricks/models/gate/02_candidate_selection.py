# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # CANDIDATE SELECTION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** choose, per task, the models forwarded for evaluation on the test
# MAGIC partition: the best candidates by the task's primary validation metric (only
# MAGIC those whose fitted artifact was stored) plus the baseline. A forwarded candidate
# MAGIC without a stored artifact is a failure.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/gate/02_candidate_selection"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Primary metric per candidate from the full (non-smoke) record
_records = {}
for t in TASKS:
    df = (
        read_model("candidate_results", ecosystem=t["ecosystem"])
        .filter(
            (F.col("task_id") == t["task_id"])
            & ~F.col("smoke")
            & (F.col("status") == "ok")
            & (F.col("metric") == t["primary_metric"])
        )
        .select(
            "model_name",
            "family",
            "stage",
            "value",
            "mlflow_run_id",
            "artifact_status",
            "frozen_delta_version",
        )
    )
    _records[t["task_id"]] = [r.asDict() for r in df.collect()]
print({k: len(v) for k, v in _records.items()})

# COMMAND ----------

# DBTITLE 1,Rank the candidates and keep the top ones plus the baseline
_selected = []
for t in TASKS:
    recs = [r for r in _records[t["task_id"]] if r["value"] is not None]
    cands = [
        r for r in recs if r["stage"] == "candidate" and r["artifact_status"] == "ok"
    ]
    cands.sort(key=lambda r: r["value"], reverse=bool(t["higher_is_better"]))
    base = [r for r in recs if r["stage"] == "baseline"]
    chosen = [(i + 1, r) for i, r in enumerate(cands[:FORWARD_TOP_N])]
    chosen += [(0, r) for r in base]
    for rank, r in chosen:
        _selected.append(
            (
                t["task_id"],
                t["dataset_id"],
                r["model_name"],
                r["family"],
                rank,
                t["primary_metric"],
                float(r["value"]),
                r["stage"] == "baseline",
                r["mlflow_run_id"],
                int(r["frozen_delta_version"]),
                rid,
                now_utc(),
            )
        )
print(f"{len(_selected)} selection row(s) for {len(TASKS)} task(s)")

# COMMAND ----------

# DBTITLE 1,Every task forwards its baseline and at least one candidate
_missing_base = [
    t["task_id"]
    for t in TASKS
    if not any(s[0] == t["task_id"] and s[7] for s in _selected)
]
_missing_candidate = [
    t["task_id"]
    for t in TASKS
    if not any(s[0] == t["task_id"] and not s[7] for s in _selected)
]
check(
    COMPONENT,
    SOURCE,
    "every_task_forwards_its_baseline",
    not _missing_base,
    detail=f"no baseline selected: {_missing_base}",
    metric_value=float(len(_missing_base)),
    rid=rid,
)
check(
    COMPONENT,
    SOURCE,
    "every_task_forwards_a_candidate",
    not _missing_candidate,
    detail=f"no candidate with a stored artifact: {_missing_candidate}",
    metric_value=float(len(_missing_candidate)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Write the selection
for eco in ("energy", "commerce"):
    rows = [s for s in _selected if TASK_BY_ID[s[0]]["ecosystem"] == eco]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["candidate_selection"]),
        "candidate_selection",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )
    print(f"OK  {len(rows)} row(s) for {eco}")