# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FOUNDATION SELECTION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** per foundation task, forward the best foundation candidate by the
# MAGIC task's primary validation metric (only one whose reference bundle was stored)
# MAGIC for evaluation on the test partition, next to the models already forwarded by the
# MAGIC accepted selection. The accepted selection table is not touched.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Foundation time-series library
# MAGIC %run ../lib/_fit_foundation_ts

# COMMAND ----------

# DBTITLE 1,Foundation tabular library
# MAGIC %run ../lib/_fit_foundation_tab

# COMMAND ----------

# DBTITLE 1,Foundation anomaly and reconstruction library
# MAGIC %run ../lib/_fit_foundation_anomaly

# COMMAND ----------

# DBTITLE 1,Foundation survival library
# MAGIC %run ../lib/_fit_foundation_survival

# COMMAND ----------

# DBTITLE 1,Foundation ranking library
# MAGIC %run ../lib/_fit_foundation_rank

# COMMAND ----------

# DBTITLE 1,Foundation offline policy library
# MAGIC %run ../lib/_fit_foundation_rl

# COMMAND ----------

# DBTITLE 1,Foundation stacking library
# MAGIC %run ../lib/_fit_foundation_stack

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/gate/05_foundation_selection"
SOURCE = "models"
ECOSYSTEMS = ("energy", "commerce")
TASK_IDS = foundation_task_ids()

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Primary metric per foundation candidate from the full record
_records = {}
for t in TASK_IDS:
    task = TASK_BY_ID[t]
    df = (
        read_model(FOUNDATION_RESULTS, ecosystem=task["ecosystem"])
        .filter(
            (F.col("task_id") == t)
            & ~F.col("smoke")
            & (F.col("status") == "ok")
            & (F.col("metric") == task["primary_metric"])
        )
        .select(
            "model_name",
            "family",
            "value",
            "mlflow_run_id",
            "artifact_status",
            "frozen_delta_version",
        )
    )
    _records[t] = [r.asDict() for r in df.collect()]
print({k: len(v) for k, v in _records.items()})

# COMMAND ----------

# DBTITLE 1,Keep the best candidate with a stored artifact
_selected = []
for t in TASK_IDS:
    task = TASK_BY_ID[t]
    cands = [
        r
        for r in _records[t]
        if r["value"] is not None and r["artifact_status"] == "ok"
    ]
    cands.sort(key=lambda r: r["value"], reverse=bool(task["higher_is_better"]))
    for r in cands[:1]:
        _selected.append(
            (
                t,
                task["dataset_id"],
                r["model_name"],
                r["family"],
                FOUNDATION_RANK,
                task["primary_metric"],
                float(r["value"]),
                False,
                r["mlflow_run_id"],
                int(r["frozen_delta_version"]),
                rid,
                now_utc(),
            )
        )
print(f"{len(_selected)} foundation candidate(s) forwarded for {len(TASK_IDS)} task(s)")

# COMMAND ----------

# DBTITLE 1,A forwarded candidate always has a stored artifact
_unstored = [s[:3] for s in _selected if not s[8]]
check(
    COMPONENT,
    SOURCE,
    "forwarded_foundation_candidates_have_an_artifact",
    not _unstored,
    detail=f"no stored run: {_unstored}",
    metric_value=float(len(_unstored)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Tasks without a forwardable foundation candidate
_none = [t for t in TASK_IDS if not any(s[0] == t for s in _selected)]
print("no foundation candidate forwarded:", _none)

# COMMAND ----------

# DBTITLE 1,Write the foundation selection
for eco in ECOSYSTEMS:
    rows = [s for s in _selected if TASK_BY_ID[s[0]]["ecosystem"] == eco]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["candidate_selection"]),
        FOUNDATION_SELECTION,
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )
    print(f"OK  {len(rows)} row(s) for {eco}")
