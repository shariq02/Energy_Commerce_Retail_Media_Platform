# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REGISTRY SETUP
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** create the registry tables. A registered version is never edited,
# MAGIC so the tables only receive new rows.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Create the registry tables
for eco in ("energy", "commerce"):
    for name in ("model_registry", "registry_check"):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {model_fqn(name, eco)} "
            f"({MODEL_DDL[name]}) USING delta"
        )
        print(f"OK  registry table ready: {model_fqn(name, eco)}")

# COMMAND ----------

# DBTITLE 1,Rows recorded so far
for eco in ("energy", "commerce"):
    for name in ("model_registry", "registry_check"):
        n = read_model(name, ecosystem=eco).count()
        print(f"{eco}.{name}: {n} row(s)")
