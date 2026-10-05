# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # REPRODUCIBILITY GUARDS
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** October 2026
# MAGIC
# MAGIC **Purpose:** check the reproducibility record. Hard failures: a registered entry
# MAGIC without a check of its current version, a check that did not pass, and a
# MAGIC check row without a processor type or a Python version. The processor types
# MAGIC checked are reported.

# COMMAND ----------

# DBTITLE 1,Model shared library
# MAGIC %run ../../lib/_model_common

# COMMAND ----------

# DBTITLE 1,Approval rules library
# MAGIC %run ../../lib/_approval_rules

# COMMAND ----------

# DBTITLE 1,Registry library
# MAGIC %run ../../lib/_registry_rules

# COMMAND ----------

# DBTITLE 1,Configuration
COMPONENT = "models/register/gate/02_reproducibility_guards"
SOURCE = "models"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = model_run_id()

# COMMAND ----------

# DBTITLE 1,Read the registry and the checks
record = {}
for eco in ("energy", "commerce"):
    registry, checks = registry_rows(eco), check_rows(eco)
    record[eco] = (registry, checks)
    print(f"{eco}: {len(registry)} registry row(s), {len(checks)} check row(s)")

# COMMAND ----------

# DBTITLE 1,Every registered entry has a check of its current version
_unchecked = []
for registry, checks in record.values():
    done = {(*_entry_key(c), c["registry_version"]) for c in checks}
    for key, r in latest_versions(registry).items():
        if r["lifecycle_status"] != "retired" and (*key, r["version"]) not in done:
            _unchecked.append((*key, r["version"]))
check(
    COMPONENT,
    SOURCE,
    "every_registered_entry_checked",
    not _unchecked,
    detail=f"entry, version: {_unchecked}",
    metric_value=float(len(_unchecked)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every check of a current version passed
_failed = []
for registry, checks in record.values():
    current = {k: r["version"] for k, r in latest_versions(registry).items()}
    for c in latest_checks(checks).values():
        is_current = c["registry_version"] == current.get(_entry_key(c))
        if is_current and c["status"] != CHECK_PASSED:
            _failed.append(
                (*_entry_key(c), c["processor_type"], c["rescore_status"], c["detail"])
            )
check(
    COMPONENT,
    SOURCE,
    "every_reproducibility_check_passed",
    not _failed,
    detail=f"entry, processor, re-score, detail: {_failed}",
    metric_value=float(len(_failed)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Every check row records its environment
_bare = [
    (*_entry_key(c), c["processor_type"])
    for _r, checks in record.values()
    for c in checks
    if not c["processor_type"] or not c["python_version"] or not c["library_versions"]
]
check(
    COMPONENT,
    SOURCE,
    "every_check_records_its_environment",
    not _bare,
    detail=f"check rows without processor type, Python version or libraries: {_bare}",
    metric_value=float(len(_bare)),
    rid=rid,
)

# COMMAND ----------

# DBTITLE 1,Processor types checked
for eco, (_r, checks) in record.items():
    print(eco, sorted({c["processor_type"] for c in checks}))
