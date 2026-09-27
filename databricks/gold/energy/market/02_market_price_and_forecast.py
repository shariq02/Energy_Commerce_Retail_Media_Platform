# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # GOLD -- MARKET_PRICE + GENERATION_FORECAST
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** `energy_gold.market_price` (from `electricity_price`) and
# MAGIC `generation_forecast` (from `electricity_generation_forecast`) -- both
# MAGIC views, already the right shape, including the sign-disputed PV series
# MAGIC with its issue reference.

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
SOURCE = "smard"
COMPONENT = "gold/energy/market/market_price_and_forecast"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = gold_run_id()

# COMMAND ----------

# DBTITLE 1,market_price -- view
read_silver("electricity_price").createOrReplaceTempView("_price")
write_gold_view("SELECT * FROM _price", "market_price", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,generation_forecast -- view
read_silver("electricity_generation_forecast").createOrReplaceTempView("_forecast")
write_gold_view("SELECT * FROM _forecast", "generation_forecast", source=SOURCE)

# COMMAND ----------

# DBTITLE 1,Inspect + export findings
_blocks = inspect_gold_table(
    read_gold("market_price", source=SOURCE),
    "market_price",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["observation_key"],
)
_blocks += inspect_gold_table(
    read_gold("generation_forecast", source=SOURCE),
    "generation_forecast",
    source=SOURCE,
    component=COMPONENT,
    rid=rid,
    key_cols=["observation_key"],
)
write_gold_findings(
    SOURCE,
    "market__market_price_and_forecast",
    "market_price / generation_forecast",
    _blocks,
)
