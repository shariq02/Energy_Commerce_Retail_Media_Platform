# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MODEL SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the two model schemas and their registry tables and register
# MAGIC every modelling task. Idempotent; the frozen dataset schemas are not touched.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ./_model_common

# COMMAND ----------

# DBTITLE 1,Configuration
MODEL_ECOSYSTEMS = ("energy", "commerce")

# COMMAND ----------

# DBTITLE 1,Create the model schemas
for eco in MODEL_ECOSYSTEMS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{model_schema_for(eco)}")
    print(f"OK  schema ready: {CATALOG}.{model_schema_for(eco)}")

# COMMAND ----------

# DBTITLE 1,Create the registry tables
for eco in MODEL_ECOSYSTEMS:
    for name, ddl in MODEL_DDL.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_fqn(name, eco)} ({ddl}) USING delta"
        )
        print(f"OK  registry table ready: {model_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Register the modelling tasks
for eco in MODEL_ECOSYSTEMS:
    rows = [
        (
            t["task_id"],
            t["dataset_id"],
            t["ecosystem"],
            t["paradigm"],
            t["task_type"],
            t["target"],
            t["baseline"],
            t["primary_metric"],
            bool(t["higher_is_better"]),
            t["notebook"],
            now_utc(),
        )
        for t in TASKS
        if t["ecosystem"] == eco
    ]
    replace_model_rows(
        spark.createDataFrame(rows, MODEL_DDL["task_registry"]),
        "task_registry",
        ecosystem=eco,
        predicate=f"ecosystem = '{eco}'",
    )
    print(f"OK  {len(rows)} task(s) registered for {eco}")

# COMMAND ----------

# DBTITLE 1,Every task is registered once
_ids = [t["task_id"] for t in TASKS]
assert len(_ids) == len(set(_ids)) == 26, f"expected 26 unique tasks, have {len(_ids)}"
print(
    "OK  26 modelling tasks across", len({t["dataset_id"] for t in TASKS}), "datasets"
)