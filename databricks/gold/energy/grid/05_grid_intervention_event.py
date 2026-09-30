# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- GRID_INTERVENTION_EVENT
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.grid_intervention_event` -- a view over Silver
# MAGIC `grid_intervention_event` (redispatch), already the right shape. Grain:
# MAGIC event.

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
SOURCE = "redispatch"
COMPONENT = "gold/energy/grid/grid_intervention_event"
TABLE = "grid_intervention_event"

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
    key_cols=["event_key"],
)
write_gold_findings(SOURCE, f"grid__{TABLE}", TABLE, _blocks)
