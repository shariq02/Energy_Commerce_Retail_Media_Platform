# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REPRODUCIBILITY CHECK ENERGY
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** reload each registered energy model from its stored run, score it on the
# MAGIC validation rows again and compare with the recorded validation value. The
# MAGIC processor type, the Python version and the library versions of this compute
# MAGIC are recorded. Run it once on each processor type in use.

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

# DBTITLE 1,Survival model library
# MAGIC %run ../lib/_fit_survival

# COMMAND ----------

# DBTITLE 1,Anomaly model library
# MAGIC %run ../lib/_fit_anomaly

# COMMAND ----------

# DBTITLE 1,Reconstruction model library
# MAGIC %run ../lib/_fit_reconstruction

# COMMAND ----------

# DBTITLE 1,Weak supervision model library
# MAGIC %run ../lib/_fit_weak

# COMMAND ----------

# DBTITLE 1,Reinforcement learning model library
# MAGIC %run ../lib/_fit_rl

# COMMAND ----------

# DBTITLE 1,Evaluation shared library
# MAGIC %run ../lib/_eval_common

# COMMAND ----------

# DBTITLE 1,Tabular evaluation library
# MAGIC %run ../lib/_eval_tabular

# COMMAND ----------

# DBTITLE 1,Survival evaluation library
# MAGIC %run ../lib/_eval_survival

# COMMAND ----------

# DBTITLE 1,Anomaly evaluation library
# MAGIC %run ../lib/_eval_anomaly

# COMMAND ----------

# DBTITLE 1,Reconstruction evaluation library
# MAGIC %run ../lib/_eval_reconstruction

# COMMAND ----------

# DBTITLE 1,Weak supervision evaluation library
# MAGIC %run ../lib/_eval_weak

# COMMAND ----------

# DBTITLE 1,Reinforcement learning evaluation library
# MAGIC %run ../lib/_eval_rl

# COMMAND ----------

# DBTITLE 1,Evaluation task specifications
# MAGIC %run ../lib/_eval_specs

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Registry check library
# MAGIC %run ../lib/_registry_check

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/02_reproducibility_energy"
SOURCE = "models"
ECOSYSTEM = "energy"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Check every registered energy model
rows = reproduce_ecosystem(ECOSYSTEM, rid)

# COMMAND ----------

# DBTITLE 1,Record the check rows
record_checks(ECOSYSTEM, rows)

# COMMAND ----------

# DBTITLE 1,Every registered model was checked
_registered = {
    _entry_key(r)
    for r in latest_versions(registry_rows(ECOSYSTEM)).values()
    if r["lifecycle_status"] != "retired"
}
_unchecked = sorted(_registered - {_entry_key(r) for r in rows})
check(
    COMPONENT,
    SOURCE,
    f"every_registered_model_checked:{ECOSYSTEM}",
    not _unchecked,
    detail=f"not checked: {_unchecked}",
    metric_value=float(len(_unchecked)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every check passed
_failed = [
    (r["task_id"], r["model_name"], r["rescore_status"], r["detail"])
    for r in rows
    if r["status"] != CHECK_PASSED
]
check(
    COMPONENT,
    SOURCE,
    f"reproducibility_checks_passed:{ECOSYSTEM}",
    not _failed,
    detail=f"failed: {_failed}",
    metric_value=float(len(_failed)),
    rid=rid,
)
