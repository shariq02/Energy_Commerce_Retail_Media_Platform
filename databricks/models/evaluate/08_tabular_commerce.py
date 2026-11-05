# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE TABULAR MODELS COMMERCE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored session purchase and lapse classifiers on the held-out partition,
# MAGIC exactly as stored.

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

# DBTITLE 1,Evaluate session_purchase_ga4.purchase
evaluate_by_id("session_purchase_ga4.purchase", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate session_purchase_rees46.purchase
evaluate_by_id("session_purchase_rees46.purchase", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate lapse_ga4.return
evaluate_by_id("lapse_ga4.return", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Evaluate lapse_rees46.return
evaluate_by_id("lapse_rees46.return", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ("commerce",),
    "evaluate__08_tabular_commerce",
    "Evaluate Tabular Models Commerce",
    ["evaluation_results", "evaluation_flags", "evaluation_run_context"],
    rid,
    task_ids=[
        "session_purchase_ga4.purchase",
        "session_purchase_rees46.purchase",
        "lapse_ga4.return",
        "lapse_rees46.return",
    ],
    smoke=SMOKE,
)
