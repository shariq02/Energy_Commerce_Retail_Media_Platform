# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- UNIT_REPOWERING
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.unit_repowering` -- a view over Silver
# MAGIC `unit_repowering`. No unit id exists on this table or a matching link
# MAGIC elsewhere yet -- `entity_link` (build step 3) is where that resolution
# MAGIC belongs, not invented here. Grain: repowering record.

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
COMPONENT = "gold/energy/generation/unit_repowering"
TABLE = "unit_repowering"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Create the view
write_gold_view(f"SELECT * FROM {silver_fqn(TABLE)}", TABLE, source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold(TABLE, source=SOURCE),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["repowering_id"],
)
write_gold_findings(SOURCE, f"generation__{TABLE}", TABLE, _blocks)
