# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- WEATHER_DAILY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.weather_daily` -- a view over Silver
# MAGIC `weather_daily` (Seattle native + DWD derived), already the right shape.
# MAGIC Grain: place x local date x variable x statistic (x level).

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
SOURCE = "dwd"
COMPONENT = "gold/energy/weather/weather_daily"
TABLE = "weather_daily"

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
    key_cols=["daily_key"],
)
write_gold_findings(SOURCE, f"weather__{TABLE}", TABLE, _blocks)
