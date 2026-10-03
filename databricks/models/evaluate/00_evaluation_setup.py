# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATION SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the evaluation result tables and record the evaluation
# MAGIC settings (resampling, flag thresholds, seeds) before any held-out partition is
# MAGIC read. Once the held-out partition has been read the settings cannot change.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../_model_common

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../_eval_common

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the evaluation tables
EVALUATION_TABLES = ("evaluation_results", "evaluation_flags", EVAL_CONTEXT_TABLE)
for eco in ("energy", "commerce"):
    for name in EVALUATION_TABLES:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_fqn(name, eco)} "
            f"({MODEL_DDL[name]}) USING delta"
        )
        print(f"OK  evaluation table ready: {model_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Settings recorded so far
recorded = {}
for eco in ("energy", "commerce"):
    rows = (
        read_model("evaluation_spec", ecosystem=eco)
        .filter(F.col("dataset_id") == EVAL_SPEC_DATASET)
        .collect()
    )
    recorded[eco] = {r["spec_key"]: r["spec_value"] for r in rows}
    print(f"{eco}: {len(rows)} setting(s) recorded")

# COMMAND ----------

# DBTITLE 1,Settings cannot change once the held-out partition was read
for eco, have in recorded.items():
    changed = [k for k, v in EVAL_SPEC_DEFAULTS.items() if have.get(k, v) != v]
    read_already = (
        read_model("evaluation_results", ecosystem=eco)
        .filter(~F.col("smoke") & (F.col("partition") == TEST_PARTITION))
        .limit(1)
        .count()
    )
    check(
        "models/evaluate/00_evaluation_setup",
        "models",
        f"settings_unchanged_after_read:{eco}",
        not (changed and read_already),
        detail=f"settings differ from the defaults after a read: {changed}",
        metric_value=float(len(changed) if read_already else 0),
        rid=rid,
    )

# COMMAND ----------

# DBTITLE 1,Record the settings
for eco, have in recorded.items():
    if have == EVAL_SPEC_DEFAULTS:
        print(f"OK  {eco}: settings already recorded, kept")
        continue
    rows = [
        (EVAL_SPEC_DATASET, key, value, rid, now_utc())
        for key, value in EVAL_SPEC_DEFAULTS.items()
    ]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["evaluation_spec"]),
        "evaluation_spec",
        ecosystem=eco,
        predicate=f"dataset_id = '{EVAL_SPEC_DATASET}'",
    )
    print(f"OK  {len(rows)} setting(s) recorded for {eco}")

# COMMAND ----------

# DBTITLE 1,The recorded settings load
for eco in ("energy", "commerce"):
    print(eco, read_thresholds(eco))
