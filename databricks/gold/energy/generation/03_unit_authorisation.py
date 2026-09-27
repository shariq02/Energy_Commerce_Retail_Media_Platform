# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- UNIT_AUTHORISATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.unit_authorisation` -- a view over Silver
# MAGIC `unit_authorisation`, already the right shape (it already carries its own
# MAGIC `linked_unit_ids`). Grain: authorisation record.

# COMMAND ----------

# DBTITLE 1,Shared Silver library
# MAGIC %run ../../../silver/_silver_common

# COMMAND ----------

# DBTITLE 1,Gold shared library
# MAGIC %run ../../_gold_common

# COMMAND ----------

# DBTITLE 1,Gold inspection library
# MAGIC %run ../../_gold_inspect

# COMMAND ----------

# DBTITLE 1,Configuration
SOURCE = "mastr"
COMPONENT = "gold/energy/generation/unit_authorisation"
TABLE = "unit_authorisation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Read Silver as a temp view
read_silver(TABLE).createOrReplaceTempView("_src")

# COMMAND ----------

# DBTITLE 1,Create the view
write_gold_view("SELECT * FROM _src", TABLE, source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold(TABLE, source=SOURCE),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["authorisation_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
