# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE TABULAR MODELS ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored regression, quantile and classification models of the energy tasks
# MAGIC on the held-out partition, exactly as stored.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model metrics library
# MAGIC %run ../lib/_model_metrics

# COMMAND ----------

# DBTITLE 1,Tabular model library
# MAGIC %run ../lib/_fit_tabular

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Tabular evaluation library
# MAGIC %run ../lib/_eval_tabular

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../lib/_eval_specs

# COMMAND ----------

# DBTITLE 1,Smoke parameter
dbutils.widgets.text("smoke", "false")
SMOKE = dbutils.widgets.get("smoke").lower() == "true"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Evaluate price_daily.price
evaluate_by_id("price_daily.price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate price_quarter_hour.price
evaluate_by_id("price_quarter_hour.price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate price_quarter_hour.negative_price
evaluate_by_id("price_quarter_hour.negative_price", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate load.load
evaluate_by_id("load.load", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate bias.bias
evaluate_by_id("bias.bias", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate zone_generation.generation
evaluate_by_id("zone_generation.generation", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate honda_forecast.increment
evaluate_by_id("honda_forecast.increment", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate ccpp.output
evaluate_by_id("ccpp.output", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate capacity_additions.additions
evaluate_by_id("capacity_additions.additions", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate redispatch.any_event
evaluate_by_id("redispatch.any_event", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate redispatch.event_energy
evaluate_by_id("redispatch.event_energy", rid, SMOKE)
