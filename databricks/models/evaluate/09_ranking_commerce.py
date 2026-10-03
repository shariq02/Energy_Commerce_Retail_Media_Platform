# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE RANKING MODELS COMMERCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored next-item rankers on the held-out partition (the REES46 task uses the
# MAGIC same user sample as the candidates).

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

# DBTITLE 1,Ranking model library
# MAGIC %run ../lib/_fit_ranking

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Ranking evaluation library
# MAGIC %run ../lib/_eval_ranking

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

# DBTITLE 1,Evaluate next_item_ga4.next_item
evaluate_by_id("next_item_ga4.next_item", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate next_item_rees46.next_item
evaluate_by_id("next_item_rees46.next_item", rid, SMOKE)
