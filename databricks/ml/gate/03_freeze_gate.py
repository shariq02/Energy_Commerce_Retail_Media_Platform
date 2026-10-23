# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FREEZE GATE
# MAGIC
# MAGIC **ECRMAP -- Ecosystem-Centric Real-World Multi-Domain Analytics Platform**
# MAGIC
# MAGIC **Author:** Sharique Mohammad
# MAGIC
# MAGIC **Date:** September 2026
# MAGIC
# MAGIC **Purpose:** freeze every fitted dataset whose latest leakage audit and null-handling
# MAGIC audit have no failed check; refuse otherwise. Writes freeze status and the
# MAGIC Delta version.

# COMMAND ----------

# DBTITLE 1,ML shared library
# MAGIC %run ../_ml_common

# COMMAND ----------

# DBTITLE 1,Configuration
ECOSYSTEMS = ("energy", "commerce")
SOURCE = "ml"
COMPONENT = "ml/gate/03_freeze_gate"
GATES = ("01_leakage_audit", "02_null_handling_audit")
RETENTION = "interval 3650 days"

# COMMAND ----------

# DBTITLE 1,Run identity
rid = ml_run_id()

# COMMAND ----------

# DBTITLE 1,Protect a version and read all of it


def protect_and_read(full: str, version: int) -> str | None:
    """Set long retention, then read every file of the version; the error or None."""
    spark.sql(
        f"ALTER TABLE {full} SET TBLPROPERTIES ("
        f"'delta.deletedFileRetentionDuration' = '{RETENTION}', "
        f"'delta.logRetentionDuration' = '{RETENTION}')"
    )
    try:
        (
            spark.sql(f"SELECT * FROM {full} VERSION AS OF {version}")
            .write.format("noop")
            .mode("overwrite")
            .save()
        )
    except Exception as exc:
        return str(exc).splitlines()[0][:200]
    return None


# COMMAND ----------

# DBTITLE 1,Freeze the passing datasets
_frozen, _refused = [], []
for eco in ECOSYSTEMS:
    fitted = (
        read_ml("dataset_manifest", ecosystem=eco)
        .filter(F.col("status") == "fitted")
        .collect()
    )
    results = read_ml("gate_results", ecosystem=eco)
    for d in fitted:
        ds = d["dataset_id"]
        mine = results.filter(F.col("dataset_id") == ds)
        seen = {r["gate"] for r in mine.select("gate").distinct().collect()}
        latest_fail = 0
        for g in GATES:
            run = mine.filter(F.col("gate") == g).agg(F.max("run_at")).first()[0]
            if run is None:
                continue
            latest_fail += mine.filter(
                (F.col("gate") == g)
                & (F.col("run_at") == run)
                & (F.col("status") == "FAIL")
            ).count()
        if seen >= set(GATES) and latest_fail == 0:
            full = ml_fqn(f"dataset_{ds}", eco)
            version = spark.sql(f"DESCRIBE HISTORY {full} LIMIT 1").first()["version"]
            read_error = protect_and_read(full, int(version))
            if read_error:
                _refused.append(
                    (eco, ds, f"version {int(version)} unreadable: {read_error}")
                )
                continue
            spark.sql(
                f"UPDATE {ml_fqn('dataset_manifest', eco)} SET status = 'frozen', "
                f"freeze_status = 'frozen', frozen_delta_version = {int(version)}, "
                f"updated_at = current_timestamp() WHERE dataset_id = '{ds}'"
            )
            _frozen.append((eco, ds, int(version)))
        else:
            _refused.append(
                (eco, ds, f"gates_seen={sorted(seen)} failed_checks={latest_fail}")
            )

# COMMAND ----------

# DBTITLE 1,Report
for eco, ds, v in _frozen:
    print(f"FROZEN  {eco}.{ds} at Delta version {v}")
for eco, ds, why in _refused:
    print(f"REFUSED {eco}.{ds}: {why}")
print(f"frozen={len(_frozen)} refused={len(_refused)}")
