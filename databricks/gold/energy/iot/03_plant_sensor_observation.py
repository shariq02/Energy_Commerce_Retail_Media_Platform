# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- PLANT_SENSOR_OBSERVATION
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.plant_sensor_observation` -- a view over Silver
# MAGIC `plant_operating_sample` (CCPP), already the right shape. Grain: sample
# MAGIC (with repeat index).

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
SOURCE = "power_plant_ccpp"
COMPONENT = "gold/energy/iot/plant_sensor_observation"
TABLE = "plant_sensor_observation"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,Create the view
write_gold_view(
    f"SELECT * FROM {silver_fqn('plant_operating_sample')}", TABLE, source=SOURCE
)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold(TABLE, source=SOURCE),
    TABLE,
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["sample_key", "repeat_index"],
)
write_gold_findings(SOURCE, f"iot__{TABLE}", TABLE, _blocks)
