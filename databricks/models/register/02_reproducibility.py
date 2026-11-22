# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # REPRODUCIBILITY CHECK
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** November 2026
# MAGIC
# MAGIC **Purpose:** reload each registered model of every ecosystem from its stored run,
# MAGIC score it on the validation rows again and compare with the recorded validation
# MAGIC value. The processor type, the Python version and the library versions of this
# MAGIC compute are recorded. Run it once on each processor type in use.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Model fit library
# MAGIC %run ../lib/_model_fit

# COMMAND ----------

# DBTITLE 1,Model evaluation library
# MAGIC %run ../lib/_model_evaluate

# COMMAND ----------

# DBTITLE 1,Model governance library
# MAGIC %run ../lib/_model_govern

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/02_reproducibility"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Environment of this compute
print(environment_record())

# COMMAND ----------

# DBTITLE 1,Check every registered model
checked = {eco: reproduce_ecosystem(eco, rid) for eco in ECOSYSTEMS}

# COMMAND ----------

# DBTITLE 1,Record the check rows
for eco, rows in checked.items():
    record_checks(eco, rows)

# COMMAND ----------

# DBTITLE 1,Every registered model was checked and every check passed
for eco, rows in checked.items():
    registered = {
        _entry_key(r)
        for r in latest_versions(registry_rows(eco)).values()
        if r["lifecycle_status"] != "retired"
    }
    guard(
        COMPONENT,
        rid,
        f"every_registered_model_checked:{eco}",
        sorted(registered - {_entry_key(r) for r in rows}),
        "not checked",
    )
    guard(
        COMPONENT,
        rid,
        f"reproducibility_checks_passed:{eco}",
        [
            (r["task_id"], r["model_name"], r["rescore_status"], r["detail"])
            for r in rows
            if r["status"] != CHECK_PASSED
        ],
        "failed",
    )

# COMMAND ----------

# DBTITLE 1,Write the findings of this notebook
write_model_findings(
    ECOSYSTEMS,
    "register__02_reproducibility",
    "Reproducibility Check",
    ["registry_check"],
    rid,
)
