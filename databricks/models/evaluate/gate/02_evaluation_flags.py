# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION FLAGS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** raise the diagnostic flags from the recorded evaluation (change
# MAGIC from validation above the threshold, skill interval that includes zero, failed
# MAGIC reproduction, prediction sanity) and store them per model. A flag never rejects
# MAGIC a model; the next step decides.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../../_model_metrics

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../../_eval_common

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the evaluation results of the full run
FIELDS = ("task_id", "model_name", "stage", "status", "detail", "metric", "value")
results = {
    eco: [
        r.asDict()
        for r in read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke"))
        .select(*FIELDS)
        .collect()
    ]
    for eco in ("energy", "commerce")
}
print({eco: len(rows) for eco, rows in results.items()})

# COMMAND ----------

# DBTITLE 1,Raise the flags
flags = {
    eco: evaluation_flag_rows(rows, read_thresholds(eco))
    for eco, rows in results.items()
}
for eco, rows in flags.items():
    kinds = {}
    for row in rows:
        kinds[row[3]] = kinds.get(row[3], 0) + 1
    print(eco, kinds or "no flags")

# COMMAND ----------

# DBTITLE 1,Store the flags
for eco, rows in flags.items():
    stamped = [(*row, rid, now_utc()) for row in rows]
    replace_model_rows(
        spark.createDataFrame(stamped, MODEL_DDL["evaluation_flags"]),
        "evaluation_flags",
        ecosystem=eco,
        predicate="task_id IS NOT NULL",
    )
