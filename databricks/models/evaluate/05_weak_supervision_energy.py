# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # EVALUATE WEAK SUPERVISION ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** score the stored label model and the majority vote on the held-out partition of the
# MAGIC weak supervision split.

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

# DBTITLE 1,Weak supervision model library
# MAGIC %run ../lib/_fit_weak

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Weak supervision evaluation library
# MAGIC %run ../lib/_eval_weak

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

# DBTITLE 1,Evaluate weak_supervision.labels
evaluate_by_id("weak_supervision.labels", rid, SMOKE)

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ("energy",),
    "evaluate__05_weak_supervision_energy",
    "Evaluate Weak Supervision Energy",
    ["evaluation_results", "evaluation_flags", "evaluation_run_context"],
    rid,
    task_ids=["weak_supervision.labels"],
    smoke=SMOKE,
)
