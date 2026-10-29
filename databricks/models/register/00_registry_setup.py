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
        full = create_model_table(name, MODEL_DDL[name], eco)
        print(f"OK  registry table ready: {full}")

# COMMAND ----------

# DBTITLE 1,Add the columns that an older registry table lacks
for eco in ("energy", "commerce"):
    for name in ("model_registry", "registry_check"):
        added = add_missing_columns(name, ecosystem=eco, ddl=MODEL_DDL[name])
        print(f"{eco}.{name}: {len(added)} column(s) added")

# COMMAND ----------

# DBTITLE 1,Rows recorded so far
for eco in ("energy", "commerce"):
    for name in ("model_registry", "registry_check"):
        n = read_model(name, ecosystem=eco).count()
        print(f"{eco}.{name}: {n} row(s)")

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ("energy", "commerce"),
    "register__00_registry_setup",
    "Registry Setup",
    [],
    rid,
    notes={
        eco: ["registry tables created if missing"] for eco in ("energy", "commerce")
    },
)
